import streamlit as st
import requests
import pandas as pd
from io import BytesIO
from urllib.parse import quote

# ========================
# CONFIGURAÇÕES
# ========================
BASE_URL = "http://apiadvisor.climatempo.com.br/api/v1"

TOKEN_PREVISAO = "531a8163c4464184b1e8ff89742d531f"
TOKEN_HISTORICO = "8445618686be6cffc02c0954cbaada35"

st.set_page_config(page_title="Climatologia - Previsão e Histórico", layout="wide")

# ========================
# FUNÇÕES AUXILIARES
# ========================
def buscar_cidade(nome, uf):
    url = f"{BASE_URL}/locale/city?name={quote(nome)}&state={uf}&token={TOKEN_PREVISAO}"
    r = requests.get(url)
    r.raise_for_status()
    return r.json()[0]

def get_previsao(locale_id, dias):
    url = f"{BASE_URL}/forecast/locale/{locale_id}/days/{dias}?token={TOKEN_PREVISAO}"
    r = requests.get(url)
    r.raise_for_status()
    return r.json()

def get_historico(locale_id, dias):
    url = f"{BASE_URL}/history/locale/{locale_id}/days/{dias}?token={TOKEN_HISTORICO}"
    r = requests.get(url)
    r.raise_for_status()
    return r.json()

def json_para_df(data):
    linhas = []
    for d in data.get("data", []):
        linhas.append({
            "Data": d.get("date_br"),
            "Temp Min (°C)": d.get("temperature", {}).get("min"),
            "Temp Max (°C)": d.get("temperature", {}).get("max"),
            "Umidade Min (%)": d.get("humidity", {}).get("min"),
            "Umidade Max (%)": d.get("humidity", {}).get("max"),
            "Chuva (mm)": d.get("rain", {}).get("precipitation"),
            "Prob. Chuva (%)": d.get("rain", {}).get("probability"),
            "Vento Médio (km/h)": d.get("wind", {}).get("velocity_avg"),
            "Rajada Máx (km/h)": d.get("wind", {}).get("gust_max"),
            "Pressão (hPa)": d.get("pressure", {}).get("pressure"),
            "UV Máx": d.get("uv", {}).get("max")
        })
    return pd.DataFrame(linhas)

def df_para_excel(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Dados")
    return output.getvalue()

# ========================
# INTERFACE
# ========================
st.title("🌦️ Climatologia – Previsão e Histórico")

cidade = st.text_input("Cidade", "Juiz de Fora")
uf = st.text_input("UF", "MG")

if cidade and uf:
    try:
        locale = buscar_cidade(cidade, uf)
        locale_id = locale["id"]
        st.success(f"Cidade selecionada: {locale['name']} - {locale['state']} (ID {locale_id})")
    except Exception as e:
        st.error("Erro ao localizar a cidade")
        st.stop()

tab1, tab2 = st.tabs(["🔮 Previsão", "🕒 Histórico"])

# ========================
# ABA PREVISÃO
# ========================
with tab1:
    dias_prev = st.slider("Dias de previsão", 1, 60, 15)

    if st.button("Gerar previsão"):
        with st.spinner("Consultando previsão..."):
            dados_prev = get_previsao(locale_id, dias_prev)
            df_prev = json_para_df(dados_prev)
            st.dataframe(df_prev)

            excel_prev = df_para_excel(df_prev)
            st.download_button(
                "⬇️ Download Previsão (XLSX)",
                excel_prev,
                file_name=f"previsao_{cidade}_{uf}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

# ========================
# ABA HISTÓRICO
# ========================
with tab2:
    dias_hist = st.slider("Dias de histórico", 1, 60, 15)

    if st.button("Gerar histórico"):
        with st.spinner("Consultando histórico..."):
            dados_hist = get_historico(locale_id, dias_hist)
            df_hist = json_para_df(dados_hist)
            st.dataframe(df_hist)

            excel_hist = df_para_excel(df_hist)
            st.download_button(
                "⬇️ Download Histórico (XLSX)",
                excel_hist,
                file_name=f"historico_{cidade}_{uf}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
