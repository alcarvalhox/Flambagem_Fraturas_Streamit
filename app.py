import streamlit as st
import requests
import pandas as pd
from io import BytesIO
from urllib.parse import quote
from datetime import datetime, timedelta, date

# =========================
# CONFIG
# =========================
BASE_V1 = "http://apiadvisor.climatempo.com.br/api/v1"
API_MANAGER = "http://apiadvisor.climatempo.com.br/api-manager"
GEOCODE_URL = "https://nominatim.openstreetmap.org/search"

TOKEN_PREVISAO_DEFAULT = "531a8163c4464184b1e8ff89742d531f"
TOKEN_HIST_DEFAULT = "8445618686be6cffc02c0954cbaada35"

MAX_DIAS = 60

# =========================
# LISTA FIXA DO SMAC (89 cidades + UF)
# =========================
SMAC_CITY_STATE = {
    "Alumínio": "SP",
    "Andrelândia": "MG",
    "Arantina": "MG",
    "Barbacena": "MG",
    "Barra do Piraí": "RJ",
    "Barra Mansa": "RJ",
    "Belo Horizonte": "MG",
    "Belo Vale": "MG",
    "Bom Jardim de Minas": "MG",
    "Brotas": "SP",
    "Brumadinho": "MG",
    "Caçapava": "SP",
    "Cachoeira Paulista": "SP",
    "Campinas": "SP",
    "Carandaí": "MG",
    "Comendador Levy Gasparian": "RJ",
    "Congonhas": "MG",
    "Conselheiro Lafaiete": "MG",
    "Coronel Xavier Chaves": "MG",
    "Cruzeiro": "SP",
    "Cubatão": "SP",
    "Dois Córregos": "SP",
    "Embu das Artes": "SP",
    "Engenheiro Paulo de Frontin": "RJ",
    "Entre Rios de Minas": "MG",
    "Francisco Morato": "SP",
    "Franco da Rocha": "SP",
    "Guararema": "SP",
    "Guaratinguetá": "SP",
    "Ibirité": "MG",
    "Iracemápolis": "SP",
    "Itabirito": "MG",
    "Itaguaí": "RJ",
    "Itaquaquecetuba": "SP",
    "Itatiaia": "RJ",
    "Itirapina": "SP",
    "Itu": "SP",
    "Jacareí": "SP",
    "Japeri": "RJ",
    "Jaú": "SP",
    "Jeceaba": "MG",
    "Juiz de Fora": "MG",
    "Jundiaí": "SP",
    "Lavrinhas": "SP",
    "Limeira": "SP",
    "Lorena": "SP",
    "Madre de Deus de Minas": "MG",
    "Mairinque": "SP",
    "Mangaratiba": "RJ",
    "Matias Barbosa": "MG",
    "Mauá": "SP",
    "Mendes": "RJ",
    "Mesquita": "RJ",
    "Moeda": "MG",
    "Mogi das Cruzes": "SP",
    "Nova Lima": "MG",
    "Ouro Preto": "MG",
    "Paracambi": "RJ",
    "Paraíba do Sul": "RJ",
    "Passa Vinte": "MG",
    "Pederneiras": "SP",
    "Pindamonhangaba": "SP",
    "Pinheiral": "RJ",
    "Porto Real": "RJ",
    "Praia Grande": "SP",
    "Quatis": "RJ",
    "Queimados": "RJ",
    "Queluz": "SP",
    "Resende": "RJ",
    "Resende Costa": "MG",
    "Ribeirão Pires": "SP",
    "Rio Claro": "SP",
    "Rio de Janeiro": "RJ",
    "Santo André": "SP",
    "Santos": "SP",
    "Santos Dumont": "MG",
    "São Brás do Suaçuí": "MG",
    "São Caetano do Sul": "SP",
    "São João del Rei": "MG",
    "São Joaquim de Bicas": "MG",
    "São José dos Campos": "SP",
    "São Paulo": "SP",
    "Sarzedo": "MG",
    "Seropédica": "RJ",
    "Taubaté": "SP",
    "Três Rios": "RJ",
    "Várzea Paulista": "SP",
    "Vassouras": "RJ",
    "Volta Redonda": "RJ",
}
SMAC_CITIES = sorted(SMAC_CITY_STATE.keys())

# =========================
# UI
# =========================
st.set_page_config(page_title="SMAC • Previsão & Histórico", layout="wide")
st.title("🌦️ SMAC • Previsão (até 60 dias) & Histórico (Hourly + Diário)")

with st.sidebar:
    st.header("🔐 Tokens")
    TOKEN_PREVISAO = st.text_input("Token Previsão", value=st.secrets.get("TOKEN_PREVISAO", TOKEN_PREVISAO_DEFAULT), type="password")
    TOKEN_HIST = st.text_input("Token Histórico", value=st.secrets.get("TOKEN_HISTORICO", TOKEN_HIST_DEFAULT), type="password")
    st.divider()
    DEBUG = st.checkbox("Modo debug", value=False)

    st.subheader("🧭 Mapa de coordenadas SMAC (recomendado)")
    st.caption("Envie um CSV/XLSX com colunas: city, uf, lat, lon. Isso evita bloqueio por whitelist.")
    coord_file = st.file_uploader("Upload coordenadas_smac.csv ou .xlsx", type=["csv", "xlsx"])

# =========================
# HTTP helpers
# =========================
def http_get(url: str, timeout: int = 30):
    try:
        r = requests.get(url, timeout=timeout)
        if r.status_code >= 400:
            return False, None, r.status_code, r.text
        return True, r.json() if r.text else {}, r.status_code, ""
    except Exception as e:
        return False, None, -1, str(e)

def http_put_form(url: str, data: dict, timeout: int = 30):
    try:
        r = requests.put(url, data=data, timeout=timeout)
        if r.status_code >= 400:
            return False, None, r.status_code, r.text
        return True, r.json() if r.text else {}, r.status_code, ""
    except Exception as e:
        return False, None, -1, str(e)

# =========================
# Carregar mapa de coordenadas (city+uf -> lat/lon)
# =========================
@st.cache_data(ttl=3600, show_spinner=False)
def load_coord_map(file) -> dict:
    if file is None:
        return {}

    if file.name.lower().endswith(".csv"):
        df = pd.read_csv(file)
    else:
        df = pd.read_excel(file, engine="openpyxl")

    # normaliza nomes de colunas
    df.columns = [c.strip().lower() for c in df.columns]
    required = {"city", "uf", "lat", "lon"}
    if not required.issubset(set(df.columns)):
        raise ValueError("Arquivo deve conter colunas: city, uf, lat, lon")

    coord_map = {}
    for _, row in df.iterrows():
        key = (str(row["city"]).strip(), str(row["uf"]).strip().upper())
        coord_map[key] = (float(row["lat"]), float(row["lon"]))
    return coord_map

coord_map = {}
try:
    coord_map = load_coord_map(coord_file)
except Exception as e:
    st.sidebar.error(str(e))

# =========================
# Geocoding fallback (somente se não houver mapa SMAC)
# =========================
@st.cache_data(ttl=86400, show_spinner=False)
def geocode_city(city: str, uf: str):
    params = {"q": f"{city}, {uf}, Brazil", "format": "json", "limit": 1}
    headers = {"User-Agent": "smac-streamlit"}
    r = requests.get(GEOCODE_URL, params=params, headers=headers, timeout=20)
    r.raise_for_status()
    data = r.json()
    if not data:
        raise ValueError(f"Geocoding não retornou coordenadas para {city}-{uf}")
    return float(data[0]["lat"]), float(data[0]["lon"])

# =========================
# Locale lookup (cidade+UF -> locale_id)
# /locale/city?name=...&state=... [1](https://www.infolocale.fr/evenements/evenement-colleville-sur-mer-patrimoine-overlord-historical-days-2009191640)
# =========================
@st.cache_data(ttl=86400, show_spinner=False)
def resolve_locale_id(city: str, uf: str, token_previsao: str):
    url = f"{BASE_V1}/locale/city?name={quote(city)}&state={uf}&token={token_previsao}"
    ok, payload, status, err = http_get(url)
    if not ok:
        raise RuntimeError(f"Erro ao resolver locale_id ({city}-{uf}). HTTP {status}: {err}")
    if not payload:
        raise RuntimeError(f"Nenhum locale encontrado para {city}-{uf}")
    return int(payload[0]["id"])

def registrar_locale_no_token(locale_id: int, token_previsao: str):
    # PUT /api-manager/user-token/<token>/locales com localeId[] [2](https://www.youtube.com/watch?v=Rm1yjmj3yYc)[1](https://www.infolocale.fr/evenements/evenement-colleville-sur-mer-patrimoine-overlord-historical-days-2009191640)
    url = f"{API_MANAGER}/user-token/{token_previsao}/locales"
    data = {"localeId[]": str(locale_id)}
    return http_put_form(url, data=data)

# =========================
# Forecast (até 60 dias)
# /forecast/locale/:id/days/270 (fallback /days/15) [1](https://www.infolocale.fr/evenements/evenement-colleville-sur-mer-patrimoine-overlord-historical-days-2009191640)
# =========================
def fetch_forecast(locale_id: int, dias: int, token_previsao: str):
    dias = max(1, min(MAX_DIAS, int(dias)))

    url270 = f"{BASE_V1}/forecast/locale/{locale_id}/days/270?token={token_previsao}"
    ok, payload, status, err = http_get(url270)
    if ok and isinstance(payload, dict) and "data" in payload:
        return True, payload["data"][:dias], status, "", 270

    url15 = f"{BASE_V1}/forecast/locale/{locale_id}/days/15?token={token_previsao}"
    ok2, payload2, status2, err2 = http_get(url15)
    if ok2 and isinstance(payload2, dict) and "data" in payload2:
        return True, payload2["data"][:min(dias, 15)], status2, "", 15

    return False, None, (status2 if not ok else status), (err2 if not ok2 else err), None

def forecast_to_df(days_list: list, label: str, locale_id: int):
    rows = []
    for d in days_list:
        rain = d.get("rain", {}) or {}
        temp = d.get("temperature", {}) or {}
        hum = d.get("humidity", {}) or {}
        wind = d.get("wind", {}) or {}
        uv = d.get("uv", {}) or {}
        rows.append({
            "Ponto": label,
            "locale_id": locale_id,
            "Data": d.get("date_br") or d.get("date"),
            "Temp Min (°C)": temp.get("min"),
            "Temp Max (°C)": temp.get("max"),
            "Chuva (mm)": rain.get("precipitation"),
            "Prob Chuva (%)": rain.get("probability"),
            "Umidade Min (%)": hum.get("min"),
            "Umidade Max (%)": hum.get("max"),
            "Vento Médio (km/h)": wind.get("velocity_avg") or wind.get("speed"),
            "Rajada (km/h)": wind.get("gust_max") or wind.get("gust"),
            "UV Máx": uv.get("max"),
        })
    return pd.DataFrame(rows)

# =========================
# Histórico GEO/hourly
# =========================
def history_geo_hourly(lat: float, lon: float, from_dt: date, token_hist: str):
    from_str = from_dt.strftime("%Y-%m-%d")
    url = f"{BASE_V1}/history/geo/hourly?token={token_hist}&from={from_str}&latitude={lat}&longitude={lon}"
    return http_get(url)

def normalize_history_payload(payload):
    if payload is None:
        return pd.DataFrame()
    if isinstance(payload, dict) and isinstance(payload.get("data"), list):
        return pd.json_normalize(payload["data"])
    if isinstance(payload, list):
        return pd.json_normalize(payload)
    return pd.json_normalize(payload)

def pick_first_col(df: pd.DataFrame, candidates: list):
    for c in candidates:
        if c in df.columns:
            return c
    return None

def build_daily_summary(df_hourly: pd.DataFrame):
    if df_hourly.empty:
        return df_hourly
    time_col = pick_first_col(df_hourly, ["date", "datetime", "time", "timestamp"])
    if time_col is None:
        return pd.DataFrame({"warning": ["Sem coluna de tempo detectável para agregação diária."]})

    dt = pd.to_datetime(df_hourly[time_col], errors="coerce")
    df_hourly = df_hourly.assign(_day=dt.dt.date)

    rain_col = pick_first_col(df_hourly, ["rain.precipitation", "precipitation", "rain", "mm"])
    t_col = pick_first_col(df_hourly, ["temperature", "temp", "temperature.value", "temperatureC"])
    h_col = pick_first_col(df_hourly, ["humidity", "humidity.value", "rh"])
    agg = {}
    if rain_col: agg[rain_col] = "sum"
    if t_col: agg[t_col] = ["min", "max"]
    if h_col: agg[h_col] = "mean"

    g = df_hourly.groupby("_day").agg(agg)
    g.columns = ["_".join([str(x) for x in col if x]) if isinstance(col, tuple) else str(col) for col in g.columns]
    g = g.reset_index().rename(columns={"_day": "dia"})
    g["dia"] = g["dia"].astype(str)
    return g

# =========================
# XLSX
# =========================
def to_xlsx(sheets: dict):
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        for name, df in sheets.items():
            df.to_excel(writer, index=False, sheet_name=name[:31])
    return buf.getvalue()

# =========================
# UI: Seleção das cidades SMAC
# =========================
st.subheader("📍 Cidades SMAC (pré-carregadas)")
selected_cities = st.multiselect(
    "Selecione uma ou mais cidades",
    options=SMAC_CITIES,
    default=["Barbacena"] if "Barbacena" in SMAC_CITIES else []
)
if not selected_cities:
    st.stop()

tab_prev, tab_hist = st.tabs(["🔮 Previsão (até 60 dias)", "🕒 Histórico (Hourly + Diário)"])

# =========================
# PREVISÃO
# =========================
with tab_prev:
    dias_prev = st.slider("Dias de previsão", 1, MAX_DIAS, 15)
    if st.button("Gerar Previsão (XLSX)", use_container_width=True):
        all_df = []
        for city in selected_cities:
            uf = SMAC_CITY_STATE[city]
            label = f"{city}-{uf}"
            try:
                locale_id = resolve_locale_id(city, uf, TOKEN_PREVISAO)
                registrar_locale_no_token(locale_id, TOKEN_PREVISAO)
                ok, days, status, err, used = fetch_forecast(locale_id, dias_prev, TOKEN_PREVISAO)
                if not ok:
                    st.error(f"{label}: previsão falhou (HTTP {status})")
                    if DEBUG: st.code(err)
                    continue
                df = forecast_to_df(days, label, locale_id)
                df["endpoint_days_used"] = used
                all_df.append(df)
            except Exception as e:
                st.error(f"{label}: {e}")
                if DEBUG: st.code(str(e))

        if all_df:
            out = pd.concat(all_df, ignore_index=True)
            st.dataframe(out, use_container_width=True)
            st.download_button(
                "⬇️ Download Previsão (XLSX)",
                data=to_xlsx({"Previsao": out}),
                file_name=f"previsao_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )

# =========================
# HISTÓRICO
# =========================
with tab_hist:
    dias_hist = st.slider("Dias de histórico", 1, MAX_DIAS, 7)
    data_inicio = st.date_input("Data inicial", value=date.today() - timedelta(days=dias_hist))

    if st.button("Gerar Histórico (Hourly + Diário) (XLSX)", use_container_width=True):
        hourly_all = []
        daily_all = []

        for city in selected_cities:
            uf = SMAC_CITY_STATE[city]
            label = f"{city}-{uf}"

            # 1) tenta coordenadas do mapa SMAC
            key = (city, uf)
            latlon = coord_map.get(key)

            # 2) fallback: geocoding (pode ser bloqueado por whitelist)
            coords_source = "mapa_smac"
            if latlon is None:
                coords_source = "geocoding_fallback"
                try:
                    latlon = geocode_city(city, uf)
                except Exception as e:
                    st.error(f"{label}: falha ao obter coordenadas automaticamente ({coords_source})")
                    if DEBUG: st.code(str(e))
                    continue

            lat, lon = latlon

            dfs_point = []
            for i in range(dias_hist):
                d = data_inicio + timedelta(days=i)
                ok, payload, status, err = history_geo_hourly(lat, lon, d, TOKEN_HIST)
                if not ok:
                    st.warning(f"{label} em {d}: HTTP {status}")
                    if DEBUG: st.code(err)
                    if "Latitude and Longitude not allowed" in (err or ""):
                        st.error(f"{label}: coordenadas recusadas (whitelist). Fonte={coords_source}, lat/lon=({lat},{lon})")
                        break
                    continue

                dfh = normalize_history_payload(payload)
                dfh.insert(0, "Ponto", label)
                dfh.insert(1, "from_date", d.strftime("%Y-%m-%d"))
                dfh.insert(2, "lat", lat)
                dfh.insert(3, "lon", lon)
                dfh.insert(4, "coords_source", coords_source)
                dfs_point.append(dfh)

            if dfs_point:
                df_hourly = pd.concat(dfs_point, ignore_index=True)
                hourly_all.append(df_hourly)
                df_daily = build_daily_summary(df_hourly.copy())
                df_daily.insert(0, "Ponto", label)
                daily_all.append(df_daily)

        if hourly_all:
            out_hourly = pd.concat(hourly_all, ignore_index=True)
            out_daily = pd.concat(daily_all, ignore_index=True) if daily_all else pd.DataFrame()

            st.subheader("📊 Visualização na tela (completa)")
            modo = st.radio("Visualizar:", ["Resumo Diário", "Histórico Horário (Raw)"], horizontal=True)
            df_view = out_daily.copy() if modo == "Resumo Diário" else out_hourly.copy()

            with st.expander("Selecionar colunas para visualizar", expanded=False):
                cols = df_view.columns.tolist()
                selected_cols = st.multiselect("Colunas", cols, default=cols)

            st.dataframe(df_view[selected_cols], use_container_width=True, height=520)

            xlsx = to_xlsx({
                "Historico_Horario": out_hourly,
                "Resumo_Diario": out_daily if not out_daily.empty else pd.DataFrame({"info": ["Resumo diário indisponível."]})
            })

            st.download_button(
                "⬇️ Download Histórico (XLSX)",
                data=xlsx,
                file_name=f"historico_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
        else:
            st.warning("Nenhum histórico foi gerado. Veja as mensagens acima.")
