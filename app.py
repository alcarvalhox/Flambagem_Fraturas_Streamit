import streamlit as st
import requests
import pandas as pd
from io import BytesIO
from urllib.parse import quote

BASE_URL = "http://apiadvisor.climatempo.com.br/api/v1"
API_MANAGER = "http://apiadvisor.climatempo.com.br/api-manager"

TOKEN_PREVISAO = "531a8163c4464184b1e8ff89742d531f"
TOKEN_HISTORICO = "8445618686be6cffc02c0954cbaada35"

st.set_page_config(page_title="Climatempo • Previsão & Histórico", layout="wide")
st.title("🌦️ Climatempo • Previsão & Histórico (Excel)")

# =========================
# FUNÇÕES DE API
# =========================
def buscar_cidade(nome, uf):
    url = f"{BASE_URL}/locale/city?name={quote(nome)}&state={uf}&token={TOKEN_PREVISAO}"
    r = requests.get(url)
    r.raise_for_status()
    return r.json()[0]

def registrar_locale(locale_id):
    url = f"{API_MANAGER}/user-token/{TOKEN_PREVISAO}/locales"
    payload = {"localeId[]": str(locale_id)}
    r = requests.put(url, data=payload, timeout=30)
    return r.status_code < 400, r.text

def buscar_previsao(locale_id, dias):
    # Climatempo aceita apenas 15 ou 270
    endpoint_dias = 15 if dias <= 15 else 270
    url = f"{BASE_URL}/forecast/locale/{locale_id}/days/{endpoint_dias}?token={TOKEN_PREVISAO}"
    r = requests.get(url)
    r.raise_for_status()
    return r.json()["data"][:dias]

def normalizar(dados):
    linhas = []
    for d in dados:
        linhas.append({
            "Data": d.get("date_br"),
            "Temp Min (°C)": d["temperature"]["min"],
            "Temp Max (°C)": d["temperature"]["max"],
            "Umidade Min (%)": d["humidity"]["min"],
            "Umidade Max (%)": d["humidity"]["max"],
            "Chuva (mm)": d["rain"]["precipitation"],
            "Prob. Chuva (%)": d["rain"]["probability"],
            "Vento Médio (km/h)": d["wind"]["velocity_avg"],
            "Rajada Máx (km/h)": d["wind"]["gust_max"],
            "UV Máx": d["uv"]["max"]
        })
    return pd.DataFrame(linhas)

def gerar_excel(df):
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False)
    return buffer.getvalue()

# =========================
# INTERFACE
# =========================
cidade = st.text_input("Cidade", "Juiz de Fora")
uf = st.text_input("UF", "MG")
dias = st.slider("Período de previsão (dias)", 1, 60, 15)

if st.button("🔮 Gerar Previsão"):
    try:
        locale = buscar_cidade(cidade, uf)
        locale_id = locale["id"]

        st.info(f"Registrando cidade {cidade}-{uf} no token...")
        ok, msg = registrar_locale(locale_id)

        if not ok:
            st.error("Falha ao registrar cidade no token")
            st.code(msg)
            st.stop()

        st.success("Cidade registrada com sucesso. Consultando previsão...")

        dados = buscar_previsao(locale_id, dias)
        df = normalizar(dados)

        st.dataframe(df, use_container_width=True)

        excel = gerar_excel(df)
        st.download_button(
            "⬇️ Download Previsão (XLSX)",
            excel,
            file_name=f"previsao_{cidade}_{uf}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    except Exception as e:
        st.error("Erro ao gerar previsão")
        st.exception(e)
``
