# =========================
# APP STREAMLIT - CLIMATEMPO
# OPÇÃO A + HISTÓRICO (OPÇÃO 2)
# =========================

import streamlit as st
import requests
import pandas as pd
from io import BytesIO
from urllib.parse import quote
from datetime import datetime, timedelta, date

# =========================
# CONFIGURAÇÕES GERAIS
# =========================
BASE_V1 = "http://apiadvisor.climatempo.com.br/api/v1"
API_MANAGER = "http://apiadvisor.climatempo.com.br/api-manager"
GEOCODE_URL = "https://nominatim.openstreetmap.org/search"

TOKEN_PREVISAO = "531a8163c4464184b1e8ff89742d531f"
TOKEN_HISTORICO = "8445618686be6cffc02c0954cbaada35"

MAX_DIAS = 60

st.set_page_config(page_title="Climatempo • Previsão & Histórico", layout="wide")
st.title("🌦️ Climatempo • Previsão (até 60 dias) & Histórico GEO")

# =========================
# FUNÇÕES HTTP
# =========================
def http_get(url, timeout=30):
    r = requests.get(url, timeout=timeout)
    if r.status_code >= 400:
        return False, None, r.status_code, r.text
    return True, r.json() if r.text else {}, r.status_code, ""

def http_put_form(url, data):
    r = requests.put(url, data=data)
    if r.status_code >= 400:
        return False, None, r.status_code, r.text
    return True, r.json() if r.text else {}, r.status_code, ""

# =========================
# GEO CODING
# =========================
def geocode_city(city, uf):
    params = {
        "q": f"{city}, {uf}, Brazil",
        "format": "json",
        "limit": 1
    }
    headers = {"User-Agent": "climatempo-streamlit"}
    r = requests.get(GEOCODE_URL, params=params, headers=headers)
    r.raise_for_status()
    data = r.json()
    if not data:
        raise ValueError("Geocoding não retornou coordenadas.")
    return float(data[0]["lat"]), float(data[0]["lon"])

# =========================
# PREVISÃO (LOCALE)
# =========================
def buscar_cidade(city, uf):
    url = f"{BASE_V1}/locale/city?name={quote(city)}&state={uf}&token={TOKEN_PREVISAO}"
    ok, data, status, err = http_get(url)
    if not ok or not data:
        raise RuntimeError("Cidade não encontrada.")
    return data[0]

def registrar_locale(locale_id):
    url = f"{API_MANAGER}/user-token/{TOKEN_PREVISAO}/locales"
    return http_put_form(url, {"localeId[]": str(locale_id)})

def previsao_60_dias(locale_id):
    url = f"{BASE_V1}/forecast/locale/{locale_id}/days/270?token={TOKEN_PREVISAO}"
    ok, payload, status, err = http_get(url)
    if not ok:
        raise RuntimeError(err)
    return payload["data"][:MAX_DIAS]

def normalizar_previsao(data):
    rows = []
    for d in data:
        rows.append({
            "Data": d.get("date_br"),
            "Temp Min (°C)": d["temperature"]["min"],
            "Temp Max (°C)": d["temperature"]["max"],
            "Chuva (mm)": d["rain"]["precipitation"],
            "Prob Chuva (%)": d["rain"]["probability"],
            "Umidade Min (%)": d["humidity"]["min"],
            "Umidade Max (%)": d["humidity"]["max"],
            "Vento Médio (km/h)": d["wind"]["velocity_avg"],
            "UV Máx": d["uv"]["max"]
        })
    return pd.DataFrame(rows)

# =========================
# HISTÓRICO GEO / HOURLY
# =========================
def history_geo_hourly(lat, lon, from_date):
    url = (
        f"{BASE_V1}/history/geo/hourly"
        f"?token={TOKEN_HISTORICO}"
        f"&from={from_date}"
        f"&latitude={lat}"
        f"&longitude={lon}"
    )
    return http_get(url)

def normalize_history_hourly(payload):
    if "data" in payload:
        return pd.json_normalize(payload["data"])
    return pd.json_normalize(payload)

def resumo_diario(df_hourly):
    if df_hourly.empty:
        return df_hourly

    df_hourly["date"] = pd.to_datetime(df_hourly["date"]).dt.date

    resumo = df_hourly.groupby("date").agg(
        chuva_total_mm=("rain.precipitation", "sum"),
        temp_min_c=("temperature", "min"),
        temp_max_c=("temperature", "max"),
        umidade_media=("humidity", "mean")
    ).reset_index()

    return resumo

# =========================
# XLSX
# =========================
def to_xlsx(dfs: dict):
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        for sheet, df in dfs.items():
            df.to_excel(writer, index=False, sheet_name=sheet)
    return buffer.getvalue()

# =========================
# ESTADO
# =========================
st.session_state.setdefault("points", [])

# =========================
# UI - PONTOS
# =========================
with st.expander("📍 Pontos (Cidade → Coordenadas)", expanded=True):
    c1, c2, c3 = st.columns([3, 1, 1])
    city = c1.text_input("Cidade", "Juiz de Fora")
    uf = c2.text_input("UF", "MG")
    add = c3.button("➕ Adicionar")

    if add:
        try:
            loc = buscar_cidade(city, uf)
            lat, lon = geocode_city(city, uf)

            st.session_state.points.append({
                "city": city,
                "uf": uf,
                "locale_id": loc["id"],
                "lat": lat,
                "lon": lon
            })
            st.success(f"{city}-{uf} adicionado com sucesso.")
        except Exception as e:
            st.error(str(e))

if st.session_state.points:
    st.dataframe(pd.DataFrame(st.session_state.points), use_container_width=True)

# =========================
# TABS
# =========================
tab_prev, tab_hist = st.tabs(["🔮 Previsão (60 dias)", "🕒 Histórico GEO (Hourly + Diário)"])

# =========================
# TAB PREVISÃO
# =========================
with tab_prev:
    if st.button("⚙️ Gerar Previsão"):
        dfs = []
        for p in st.session_state.points:
            try:
                data = previsao_60_dias(p["locale_id"])
                df = normalizar_previsao(data)
                df.insert(0, "Cidade", f'{p["city"]}-{p["uf"]}')
                dfs.append(df)
            except Exception as e:
                st.error(f"Erro em {p['city']}: {e}")

        if dfs:
            final = pd.concat(dfs, ignore_index=True)
            st.dataframe(final, use_container_width=True)
            st.download_button(
                "⬇️ Download Previsão (XLSX)",
                to_xlsx({"Previsao_60_dias": final}),
                "previsao_60_dias.xlsx"
            )

# =========================
# TAB HISTÓRICO
# =========================
with tab_hist:
    dias = st.slider("Dias de histórico", 1, MAX_DIAS, 7)
    data_inicio = st.date_input("Data inicial", value=date.today() - timedelta(days=dias))

    if st.button("⚙️ Gerar Histórico"):
        hourly_all = []
        resumo_all = []

        for p in st.session_state.points:
            dfs_point = []
            for i in range(dias):
                d = (data_inicio + timedelta(days=i)).strftime("%Y-%m-%d")
                ok, payload, status, err = history_geo_hourly(p["lat"], p["lon"], d)
                if not ok:
                    st.warning(f"{p['city']} em {d}: {err}")
                    break
                dfs_point.append(normalize_history_hourly(payload))

            if dfs_point:
                hourly = pd.concat(dfs_point, ignore_index=True)
                hourly.insert(0, "Cidade", f'{p["city"]}-{p["uf"]}')
                hourly_all.append(hourly)

                resumo = resumo_diario(hourly)
                resumo.insert(0, "Cidade", f'{p["city"]}-{p["uf"]}')
                resumo_all.append(resumo)

        if hourly_all:
            df_hourly = pd.concat(hourly_all, ignore_index=True)
            df_resumo = pd.concat(resumo_all, ignore_index=True)

            st.dataframe(df_resumo, use_container_width=True)

            st.download_button(
                "⬇️ Download Histórico (XLSX)",
                to_xlsx({
                    "Historico_Horario": df_hourly,
                    "Resumo_Diario": df_resumo
                }),
                "historico_geo.xlsx"
            )
