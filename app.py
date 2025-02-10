import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime
import os
import json
from streamlit_autorefresh import st_autorefresh
import urllib.parse

# --- Mecanismo de alerta global (vários alertas) ---
ALERT_FILE = "global_alerts.json"

def get_global_alerts():
    """Lê os alertas globais do arquivo (se existirem) e retorna uma lista."""
    if os.path.exists(ALERT_FILE):
        try:
            with open(ALERT_FILE, 'r', encoding='utf-8') as f:
                alerts = json.load(f)
                if isinstance(alerts, list):
                    return alerts
        except Exception as e:
            st.error(f"Erro ao ler os alertas globais: {e}")
    return []

def add_global_alert(message):
    """Adiciona um alerta global à lista, preservando os alertas existentes."""
    alerts = get_global_alerts()
    alerts.append(message)
    with open(ALERT_FILE, 'w', encoding='utf-8') as f:
        json.dump(alerts, f)

def clear_global_alerts():
    """Limpa todos os alertas globais removendo o arquivo."""
    if os.path.exists(ALERT_FILE):
        os.remove(ALERT_FILE)

# --- Função para cálculo das métricas (igual ao seu código original) ---
def calcular_metricas(df_campanhas, df_daily):
    resultados = []
    for _, campanha in df_campanhas.iterrows():
        inicio = pd.to_datetime(campanha['inicio_campanha']).date()
        fim = pd.to_datetime(campanha['fim_campanha']).date()
        dias_totais = (fim - inicio).days + 1
        
        meta_diaria = campanha['volume_contratado'] / dias_totais
        
        dados_campanha = df_daily[df_daily['insertion_order'] == campanha['insertion_order']]
        dados_campanha = dados_campanha.sort_values('data', ascending=False)
        
        metrica = 'impressions' if campanha['modelo'] == 'CPM' else 'views'
        ultima_metrica = dados_campanha.iloc[1][metrica] if len(dados_campanha) > 1 else 0
        
        pace = (ultima_metrica / meta_diaria * 100) if meta_diaria > 0 else 0
        
        status = "Under" if pace < 90 else "Over" if pace > 110 else "On Track"
        
        dias_restantes = dias_totais - len(dados_campanha)
        
        resultados.append({
            "Campanha": campanha['insertion_order'],
            "Modelo": campanha['modelo'],
            "Meta Diária": meta_diaria,
            "Última Entrega": ultima_metrica,
            "Pace": pace,
            "Status": status,
            "Dias Restantes": dias_restantes
        })
    
    return pd.DataFrame(resultados)

# --- Função principal ---
def main():
    st.set_page_config(page_title="Dashboard de Campanhas", page_icon="📊", layout="wide")
    st.title("Dashboard de Campanhas")
    
    # Auto-refresh a cada 10 segundos para que todas as sessões verifiquem os alertas globais.
    st_autorefresh(interval=10 * 1000, key="global_alert_refresh")
    
    # Exibe os alertas globais para todos os usuários (se existirem).
    global_alerts = get_global_alerts()
    if global_alerts:
        for alert in global_alerts:
            st.warning(alert)
        # Botão para limpar os alertas globais (pode ser restrito a administradores)
        if st.button("Limpar alertas globais"):
            clear_global_alerts()
            st.experimental_rerun()
    
    # --- Leitura dos dados do Excel ---
    try:
        df_campanhas = pd.read_excel('campanhas.xlsx', sheet_name='campanha')
        df_daily = pd.read_excel('campanhas.xlsx', sheet_name='data')
    except Exception as e:
        st.error(f"Erro ao carregar os dados: {e}")
        return
    
    df_resultados = calcular_metricas(df_campanhas, df_daily)
    
    # --- Seção de KPIs em Cartões ---
    under = len(df_resultados[df_resultados['Status'] == 'Under'])
    over = len(df_resultados[df_resultados['Status'] == 'Over'])
    on_track = len(df_resultados[df_resultados['Status'] == 'On Track'])
    pace_medio = f"{df_resultados['Pace'].mean():.1f}%"
    
    # CSS para os cartões (cards)
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
    
    # --- Gráfico das campanhas ---
    fig = px.bar(
        df_resultados,
        x='Campanha',
        y='Pace',
        color='Status',
        color_discrete_map={
            'Under': 'red',
            'Over': 'lightcoral',
            'On Track': 'green'
        }
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # --- Exibição da Tabela com Layout "melhorado" ---
    st.write("### Status das Campanhas")
    
    # Cabeçalho da tabela com 7 colunas (incluindo "Alerta")
    header_cols = st.columns([3, 1, 1, 1, 1, 1, 1])
    header_cols[0].markdown("**Campanha**")
    header_cols[1].markdown("**Meta Diária**")
    header_cols[2].markdown("**Última Entrega**")
    header_cols[3].markdown("**Pace**")
    header_cols[4].markdown("**Status**")
    header_cols[5].markdown("**Dias Restantes**")
    header_cols[6].markdown("**Alerta**")
    st.markdown("<hr>", unsafe_allow_html=True)
    
    # Linhas da tabela com botão de alerta em cada linha
    for idx, row in df_resultados.iterrows():
        row_cols = st.columns([3, 1, 1, 1, 1, 1, 1])
        row_cols[0].write(row['Campanha'])
        row_cols[1].write(f"{row['Meta Diária']:,.0f}")
        row_cols[2].write(f"{row['Última Entrega']:,.0f}")
        row_cols[3].write(f"{row['Pace']:.1f}%")
        if row['Status'] == "Under":
            status_color = "red"
        elif row['Status'] == "Over":
            status_color = "lightcoral"
        else:
            status_color = "green"
        row_cols[4].markdown(
            f"<span style='color: {status_color}; font-weight: bold;'>{row['Status']}</span>",
            unsafe_allow_html=True
        )
        row_cols[5].write(row['Dias Restantes'])
        if row_cols[6].button("🚨", key=f"alert_{idx}"):
            alerta_msg = f"Alerta: Campanha {row['Campanha']} está com status {row['Status']}!"
            add_global_alert(alerta_msg)
            st.success(f"Alerta global enviado para todos! ({row['Campanha']})")
        st.markdown("<hr>", unsafe_allow_html=True)

if __name__ == "__main__":
    main()
