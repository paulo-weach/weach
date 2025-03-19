import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime
import os
import json
from streamlit_autorefresh import st_autorefresh
import urllib.parse
from google.cloud import bigquery

# --- Mecanismo de alerta global (vários alertas) ---

def conectar_bigquery():
    """Conecta ao BigQuery usando credenciais armazenadas nos secrets do Streamlit"""
    try:
        credentials_json = st.secrets["bigquery_credentials"]
        credentials_dict = json.loads(credentials_json)
        
        print("Project ID:", credentials_dict["project_id"])
        
        client = bigquery.Client.from_service_account_info(
            credentials_dict,
            project=credentials_dict["project_id"]
        )
        return client
    except Exception as e:
        st.error(f"Erro ao conectar ao BigQuery: {str(e)}")
        st.error(f"Detalhes do erro: {type(e).__name__}")
        print(f"Erro completo: {e}")
        return None


def get_campaign_data(client):
    """Busca dados das campanhas do BigQuery"""
    try:
        query = """
        SELECT *
        FROM `banco-de-dados-weach-451217.banco_inicial.campanhas`
        """
        df_campanhas = client.query(query).to_dataframe()
        return df_campanhas
    except Exception as e:
        st.error(f"Erro ao buscar campanhas: {e}")
        return None

def get_daily_data(client):
    """Busca dados diários do BigQuery com join nas condições especificadas"""
    try:
        query = """
        SELECT 
            t.`Insertion Order`,  -- Usando crases para escapar o nome da coluna
            t.Date,
            t.Impressions,
            t.Clicks,
            t.Complete_Views,
            t.Revenue
        FROM 
            `banco-de-dados-weach-451217.banco_inicial.teste` t
        JOIN 
            `banco-de-dados-weach-451217.banco_inicial.campanhas` c
        ON 
            t.`Insertion Order` = c.insertion_order
        AND 
            t.Date BETWEEN c.inicio_campanha AND c.fim_campanha
        """
        df_daily = client.query(query).to_dataframe()
        return df_daily
    except Exception as e:
        st.error(f"Erro ao buscar dados diários: {e}")
        return None

def calcular_metricas(df_campanhas, df_daily):
    resultados = []
    
    for _, campanha in df_campanhas.iterrows():
        try:
            inicio = pd.to_datetime(campanha['inicio_campanha']).date()
            fim = pd.to_datetime(campanha['fim_campanha']).date()
            data_atual = datetime.now().date()  # Data atual
            
            # Verifica se a campanha já terminou
            if fim < data_atual:
                continue  # Ignora campanhas que já terminaram
            
            # Calcula os dias totais e dias decorridos
            dias_totais = (fim - inicio).days + 1
            dias_decorridos = (data_atual - inicio).days + 1
            
            # Calcula a meta diária
            meta_diaria = float(campanha['volume_contratado']) / dias_totais
            
            # Filtra os dados da campanha
            dados_campanha = df_daily[df_daily['Insertion Order'] == campanha['insertion_order']]
            dados_campanha = dados_campanha.sort_values('Date', ascending=True)  # Ordena por data
            
            # Calcula o volume acumulado até o momento
            volume_acumulado = dados_campanha['Impressions'].sum() if campanha['modelo'] == 'CPM' else dados_campanha['Complete_Views'].sum()
            
            # Calcula o volume esperado até o momento
            volume_esperado = meta_diaria * dias_decorridos
            
            # Calcula o pace acumulado
            pace_acumulado = (volume_acumulado / volume_esperado * 100) if volume_esperado > 0 else 0
            
            # Define o status com base no pace acumulado
            if pace_acumulado < 90:
                status = "Under"
            elif pace_acumulado > 110:
                status = "Over"
            else:
                status = "On Track"
            
            # Calcula a métrica principal (Impressions ou Complete_Views)
            metrica = 'Impressions' if campanha['modelo'] == 'CPM' else 'Complete_Views'
            ultima_metrica = dados_campanha.iloc[-1][metrica] if not dados_campanha.empty else 0
            
            # Verifica o modelo da campanha para calcular o Underperforming
            if campanha['modelo'] == 'CPM':
                # Calcula o CTR para CPM
                soma_clicks = dados_campanha['Clicks'].sum()
                ctr = (soma_clicks / volume_acumulado * 100) if volume_acumulado > 0 else 0
                underperforming = "Sim" if ctr < 0.20 else "Não"
                valor_underperforming = f"CTR: {ctr:.2f}%"  # Valor que determinou o Underperforming
            elif campanha['modelo'] == 'CPV':
                # Calcula a taxa de Complete Views para CPV
                taxa_complete_views = (volume_acumulado / dados_campanha['Impressions'].sum() * 100) if dados_campanha['Impressions'].sum() > 0 else 0
                underperforming = "Sim" if taxa_complete_views < 50 else "Não"
                valor_underperforming = f"Complete Views: {taxa_complete_views:.2f}%"  # Valor que determinou o Underperforming
            else:
                underperforming = "Não"  # Caso o modelo não seja CPM ou CPV
                valor_underperforming = "N/A"
            
            # Calcula a meta necessária por dia para bater a meta total
            dias_restantes = (fim - data_atual).days
            if dias_restantes > 0:
                meta_necessaria_diaria = (float(campanha['volume_contratado']) - volume_acumulado) / dias_restantes
            else:
                meta_necessaria_diaria = 0  # Campanha já terminou
            
            # Adiciona os resultados
            resultados.append({
                "Campanha": campanha['insertion_order'],
                "Modelo": campanha['modelo'],
                "Volume Contratado": campanha['volume_contratado'],
                "Meta Diária": meta_diaria,
                "Volume Acumulado": volume_acumulado,  # Adicionado o volume acumulado
                "Volume Esperado": volume_esperado,  # Adicionado o volume esperado
                "Última Entrega": ultima_metrica,
                "Pace": pace_acumulado,
                "Status": status,
                "Dias Restantes": dias_restantes,
                "Início Campanha": inicio,
                "Fim Campanha": fim,
                "Underperforming": underperforming,
                "Valor Underperforming": valor_underperforming,
                "Meta Necessária Diária": meta_necessaria_diaria  # Nova coluna
            })
        except Exception as e:
            st.error(f"Erro ao processar campanha: {campanha['insertion_order']}, Erro: {str(e)}")
            continue
    
    return pd.DataFrame(resultados)

def calcular_metricas_programatica(df_campanhas, df_daily):
    resultados = []
    
    for _, campanha in df_campanhas.iterrows():
        try:
            # Pega o budget (investimento) da campanha
            investimento = float(campanha['budget'].replace('R$', '').replace('.', '').replace(',', '.'))
            
            # Soma o revenue total para esta campanha
            dados_campanha = df_daily[df_daily['Insertion Order'] == campanha['insertion_order']]
            investimento_entregue = dados_campanha['Revenue'].sum()
            
            # Calcula a margem
            margem = ((investimento - investimento_entregue) / investimento * 100) if investimento > 0 else 0
            
            resultados.append({
                "Campanha": campanha['insertion_order'],
                "Investimento Total": investimento,
                "Investimento Entregue": investimento_entregue,
                "Margem (%)": margem,
                "Status": "Boa" if margem >= 30 else "Média" if margem >= 20 else "Ruim"
            })
        except Exception as e:
            st.error(f"Erro ao processar campanha programática: {campanha['insertion_order']}, Erro: {str(e)}")
            continue
    
    return pd.DataFrame(resultados)

def programatica_page():
    st.title("Campanhas Programática")
    
    client = conectar_bigquery()
    if client is None:
        return
        
    try:
        df_campanhas = get_campaign_data(client)
        df_daily = get_daily_data(client)
        
        if df_campanhas is None or df_daily is None:
            st.error("Não foi possível carregar os dados das campanhas.")
            return
            
        df_resultados = calcular_metricas_programatica(df_campanhas, df_daily)
        
        # Mostra as métricas gerais
        col1, col2, col3 = st.columns(3)
        
        with col1:
            media_margem = df_resultados['Margem (%)'].mean()
            st.metric("Margem Média", f"{media_margem:.1f}%")
        
        with col2:
            total_investido = df_resultados['Investimento Total'].sum()
            st.metric("Total Investido", f"R$ {total_investido:,.2f}")
            
        with col3:
            total_entregue = df_resultados['Investimento Entregue'].sum()
            st.metric("Total Entregue", f"R$ {total_entregue:,.2f}")
        
        # Gráfico de barras com as margens
        fig = px.bar(
            df_resultados,
            x='Campanha',
            y='Margem (%)',
            color='Status',
            color_discrete_map={
                'Boa': 'green',
                'Média': 'yellow',
                'Ruim': 'red'
            }
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Tabela detalhada
        st.write("### Detalhamento por Campanha")
        
        # Formatação da tabela
        df_display = df_resultados.copy()
        df_display['Investimento Total'] = df_display['Investimento Total'].apply(lambda x: f"R$ {x:,.2f}")
        df_display['Investimento Entregue'] = df_display['Investimento Entregue'].apply(lambda x: f"R$ {x:,.2f}")
        df_display['Margem (%)'] = df_display['Margem (%)'].apply(lambda x: f"{x:.1f}%")
        
        st.dataframe(df_display)
        
    except Exception as e:
        st.error(f"Erro ao processar dados programática: {e}")
        return

def dashboard_page():
    st.title("Dashboard de Campanhas")
    
    client = conectar_bigquery()
    if client is None:
        return
        
    try:
        df_campanhas = get_campaign_data(client)
        df_daily = get_daily_data(client)
        
        if df_campanhas is None or df_daily is None:
            st.error("Não foi possível carregar os dados das campanhas.")
            return
            
        df_resultados = calcular_metricas(df_campanhas, df_daily)
    except Exception as e:
        st.error(f"Erro ao processar dados: {e}")
        return
    
    # Filtros acima dos KPIs
    col1, col2 = st.columns(2)
    with col1:
        status_filtro = st.selectbox(
            "Filtrar por Status",
            ["Todos", "Under", "Over", "On Track"],
            index=0
        )
    with col2:
        underperforming_filtro = st.selectbox(
            "Filtrar por Underperforming",
            ["Todos", "Sim", "Não"],
            index=0
        )
    
    # Aplicar filtros
    if status_filtro == "Todos":
        df_filtrado = df_resultados
    else:
        df_filtrado = df_resultados[df_resultados['Status'] == status_filtro]
    
    if underperforming_filtro != "Todos":
        df_filtrado = df_filtrado[df_filtrado['Underperforming'] == underperforming_filtro]
    
    # KPIs
    under = len(df_resultados[df_resultados['Status'] == 'Under'])
    over = len(df_resultados[df_resultados['Status'] == 'Over'])
    on_track = len(df_resultados[df_resultados['Status'] == 'On Track'])
    pace_medio = f"{df_resultados['Pace'].mean():.1f}%"
    
    card_css = """
    <style>
    .card {
        background-color: #fff;
        border: 1px solid #e1e1e1;
        border-radius: 10px;
        padding: 20px;
        margin: 10px;
        box-shadow: 2px 2px 8px rgba(0,0,0,0.1);
        text-align: center;
    }
    </style>
    """
    st.markdown(card_css, unsafe_allow_html=True)
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.markdown(f"""
        <div class="card">
          <h3>Under Delivery</h3>
          <p style="font-size: 24px;">{under}</p>
        </div>
        """, unsafe_allow_html=True)
    with col2:
        st.markdown(f"""
        <div class="card">
          <h3>Over Delivery</h3>
          <p style="font-size: 24px;">{over}</p>
        </div>
        """, unsafe_allow_html=True)
    with col3:
        st.markdown(f"""
        <div class="card">
          <h3>On Track</h3>
          <p style="font-size: 24px;">{on_track}</p>
        </div>
        """, unsafe_allow_html=True)
    with col4:
        st.markdown(f"""
        <div class="card">
          <h3>Pace Médio</h3>
          <p style="font-size: 24px;">{pace_medio}</p>
        </div>
        """, unsafe_allow_html=True)
    
    st.write("### Status das Campanhas")
    
    # Ajuste das colunas para incluir Volume Acumulado, Volume Esperado e Meta Necessária Diária
    header_cols = st.columns([3, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1])
    header_cols[0].markdown("**Campanha**")
    header_cols[1].markdown("**Modelo**")
    header_cols[2].markdown("**Volume Contratado**")
    header_cols[3].markdown("**Meta Diária**")
    header_cols[4].markdown("**Volume Acumulado**")
    header_cols[5].markdown("**Volume Esperado**")
    header_cols[6].markdown("**Última Entrega**")
    header_cols[7].markdown("**Pace**")
    header_cols[8].markdown("**Status**")
    header_cols[9].markdown("**Dias Restantes**")
    header_cols[10].markdown("**Início Campanha**")
    header_cols[11].markdown("**Fim Campanha**")
    header_cols[12].markdown("**Underperforming**")
    header_cols[13].markdown("**Meta Necessária Diária**")
    st.markdown("<hr>", unsafe_allow_html=True)
    
    for idx, row in df_filtrado.iterrows():
        row_cols = st.columns([3, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1])
        row_cols[0].write(row['Campanha'])
        row_cols[1].write(row['Modelo'])
        row_cols[2].write(f"{row['Volume Contratado']:,.0f}")
        row_cols[3].write(f"{row['Meta Diária']:,.0f}")
        row_cols[4].write(f"{row['Volume Acumulado']:,.0f}")  # Volume Acumulado
        row_cols[5].write(f"{row['Volume Esperado']:,.0f}")  # Volume Esperado
        row_cols[6].write(f"{row['Última Entrega']:,.0f}")
        row_cols[7].write(f"{row['Pace']:.1f}%")
        if row['Status'] == "Under":
                        status_color = "red"
        elif row['Status'] == "Over":
            status_color = "lightcoral"
        else:
            status_color = "green"
        row_cols[8].markdown(
            f"<span style='color: {status_color}; font-weight: bold;'>{row['Status']}</span>",
            unsafe_allow_html=True
        )
        row_cols[9].write(row['Dias Restantes'])
        row_cols[10].write(row['Início Campanha'])
        row_cols[11].write(row['Fim Campanha'])
        row_cols[12].write(row['Underperforming'])
        row_cols[13].write(f"{row['Meta Necessária Diária']:,.0f}")  # Meta Necessária Diária
        st.markdown("<hr>", unsafe_allow_html=True)

def main():
    st.set_page_config(page_title="Dashboard de Campanhas", page_icon="📊", layout="wide")
    
    menu = st.sidebar.selectbox(
        "Selecione a Página",
        ["Dashboard", "Campanhas Programática"]
    )
    
    if menu == "Dashboard":
        dashboard_page()
    else:
        programatica_page()

if __name__ == "__main__":
    main()
