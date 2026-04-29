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
    TOKEN_PREVISAO = st.text_input(
        "Token Previsão",
        value=st.secrets.get("TOKEN_PREVISAO", TOKEN_PREVISAO_DEFAULT),
        type="password",
    )
    TOKEN_HIST = st.text_input(
        "Token Histórico",
        value=st.secrets.get("TOKEN_HISTORICO", TOKEN_HIST_DEFAULT),
        type="password",
    )
    st.divider()
    DEBUG = st.checkbox("Modo debug", value=False)

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
# Locale lookup (cidade+UF -> locale_id)
# /locale/city?name=...&state=... [2](https://www.infolocale.fr/evenements/evenement-colleville-sur-mer-patrimoine-overlord-historical-days-2009191640)
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

# Detalhes do locale por ID (inclui lat/lon em muitos contratos/retornos).
# Endpoint usado em wrappers: /locale/city/{id} [3](https://bing.com/search?q=apiadvisor+climatempo+history+weather+by+locale+daily)
@st.cache_data(ttl=86400, show_spinner=False)
def get_locale_details(locale_id: int, token_previsao: str):
    url = f"{BASE_V1}/locale/city/{locale_id}?token={token_previsao}"
    ok, payload, status, err = http_get(url)
    if not ok:
        raise RuntimeError(f"Erro em locale/city/{locale_id}. HTTP {status}: {err}")
    return payload

def extract_lat_lon(locale_payload) -> tuple[float | None, float | None]:
    """
    Tenta extrair lat/lon de diferentes formatos de resposta.
    """
    if locale_payload is None:
        return None, None

    if isinstance(locale_payload, list) and locale_payload:
        locale_payload = locale_payload[0]

    if isinstance(locale_payload, dict):
        lat = locale_payload.get("latitude") or locale_payload.get("lat")
        lon = locale_payload.get("longitude") or locale_payload.get("lon")
        if lat is not None and lon is not None:
            try:
                return float(lat), float(lon)
            except Exception:
                return None, None

    return None, None

# Registrar locale no token (quando necessário)
# PUT /api-manager/user-token/<token>/locales com localeId[] [1](https://www.youtube.com/watch?v=Rm1yjmj3yYc)[2](https://www.infolocale.fr/evenements/evenement-colleville-sur-mer-patrimoine-overlord-historical-days-2009191640)
def registrar_locale_no_token(locale_id: int, token_previsao: str):
    url = f"{API_MANAGER}/user-token/{token_previsao}/locales"
    data = {"localeId[]": str(locale_id)}
    return http_put_form(url, data=data)

# =========================
# Forecast (até 60 dias) por locale_id
# /forecast/locale/:id/days/270 (fallback /days/15) [2](https://www.infolocale.fr/evenements/evenement-colleville-sur-mer-patrimoine-overlord-historical-days-2009191640)
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
# Histórico GEO/hourly (sem usuário digitar coordenadas)
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
    wind_col = pick_first_col(df_hourly, ["wind.speed", "wind.gust", "wind.gust_max"])
    press_col = pick_first_col(df_hourly, ["pressure", "pressure.value", "pressure_hpa"])

    agg = {}
    if rain_col: agg[rain_col] = "sum"
    if t_col:    agg[t_col] = ["min", "max"]
    if h_col:    agg[h_col] = "mean"
    if wind_col: agg[wind_col] = "max"
    if press_col: agg[press_col] = ["min", "max", "mean"]

    g = df_hourly.groupby("_day").agg(agg)
    g.columns = ["_".join([str(x) for x in col if x]) if isinstance(col, tuple) else str(col) for col in g.columns]
    g = g.reset_index().rename(columns={"_day": "dia"})
    g["dia"] = g["dia"].astype(str)

    rename = {}
    if rain_col: rename[f"{rain_col}_sum"] = "chuva_total_mm"
    if t_col:
        rename[f"{t_col}_min"] = "temp_min"
        rename[f"{t_col}_max"] = "temp_max"
    if h_col: rename[f"{h_col}_mean"] = "umidade_media"
    if wind_col: rename[f"{wind_col}_max"] = "vento_max"
    if press_col:
        rename[f"{press_col}_min"] = "pressao_min"
        rename[f"{press_col}_max"] = "pressao_max"
        rename[f"{press_col}_mean"] = "pressao_media"

    return g.rename(columns=rename)

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
# UI: seleção
# =========================
st.subheader("📍 Cidades SMAC (pré-carregadas)")
selected_cities = st.multiselect(
    "Selecione uma ou mais cidades",
    options=SMAC_CITIES,
    default=["Barbacena"] if "Barbacena" in SMAC_CITIES else []
)

if not selected_cities:
    st.info("Selecione pelo menos uma cidade.")
    st.stop()

# Mapeamento automático
with st.expander("🔎 Mapeamento automático (cidade → UF → locale_id → coords do locale)", expanded=False):
    rows = []
    for city in selected_cities:
        uf = SMAC_CITY_STATE[city]
        try:
            locale_id = resolve_locale_id(city, uf, TOKEN_PREVISAO)
            details = get_locale_details(locale_id, TOKEN_PREVISAO)
            lat, lon = extract_lat_lon(details)
        except Exception as e:
            locale_id, lat, lon = None, None, None
            if DEBUG:
                st.code(str(e))
        rows.append({"cidade": city, "uf": uf, "locale_id": locale_id, "lat_locale": lat, "lon_locale": lon})
    st.dataframe(pd.DataFrame(rows), use_container_width=True)

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
            except Exception as e:
                st.error(f"{label}: falha ao resolver locale_id")
                if DEBUG: st.code(str(e))
                continue

            ok_reg, _, st_reg, err_reg = registrar_locale_no_token(locale_id, TOKEN_PREVISAO)
            if (not ok_reg) and DEBUG:
                st.warning(f"[DEBUG] registro locale falhou para {label}: HTTP {st_reg}")
                st.code(err_reg)

            ok, days, status, err, used = fetch_forecast(locale_id, dias_prev, TOKEN_PREVISAO)
            if not ok:
                st.error(f"Previsão falhou para {label}: HTTP {status}")
                if DEBUG: st.code(err)
                continue

            df = forecast_to_df(days, label, locale_id)
            df["endpoint_days_used"] = used
            all_df.append(df)

        if all_df:
            out = pd.concat(all_df, ignore_index=True)
            st.dataframe(out, use_container_width=True)
            xlsx = to_xlsx({"Previsao": out})
            st.download_button(
                "⬇️ Download Previsão (XLSX)",
                data=xlsx,
                file_name=f"previsao_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
        else:
            st.warning("Nenhuma previsão gerada.")

# =========================
# HISTÓRICO
# =========================
with tab_hist:
    dias_hist = st.slider("Dias de histórico", 1, MAX_DIAS, 7)
    data_inicio = st.date_input("Data inicial", value=date.today() - timedelta(days=dias_hist))

    if st.button("Gerar Histórico (Hourly + Diário) (XLSX)", use_container_width=True):
        hourly_all, daily_all = [], []

        for city in selected_cities:
            uf = SMAC_CITY_STATE[city]
            label = f"{city}-{uf}"

            # 1) resolve locale_id
            try:
                locale_id = resolve_locale_id(city, uf, TOKEN_PREVISAO)
            except Exception as e:
                st.error(f"{label}: falha ao resolver locale_id")
                if DEBUG: st.code(str(e))
                continue

            # 2) pega coords do locale (Climatempo) automaticamente
            try:
                details = get_locale_details(locale_id, TOKEN_PREVISAO)
                lat, lon = extract_lat_lon(details)
            except Exception as e:
                lat, lon = None, None
                if DEBUG: st.code(str(e))

            if lat is None or lon is None:
                st.error(f"{label}: não foi possível obter lat/lon do locale automaticamente.")
                st.info("Isso depende do payload de /locale/city/{id}. Se necessário, ajustamos para usar uma fonte alternativa.")
                continue

            # 3) loop diário no endpoint GEO/hourly
            dfs_point = []
            for i in range(dias_hist):
                d = data_inicio + timedelta(days=i)
                ok, payload, status, err = history_geo_hourly(lat, lon, d, TOKEN_HIST)
                if not ok:
                    st.warning(f"{label} em {d}: HTTP {status}")
                    if DEBUG: st.code(err)
                    if "Latitude and Longitude not allowed" in (err or ""):
                        st.error(f"{label}: coordenadas ({lat},{lon}) recusadas pelo token histórico (whitelist).")
                        break
                    continue

                dfh = normalize_history_payload(payload)
                dfh.insert(0, "Ponto", label)
                dfh.insert(1, "locale_id", locale_id)
                dfh.insert(2, "from_date", d.strftime("%Y-%m-%d"))
                dfh.insert(3, "lat", lat)
                dfh.insert(4, "lon", lon)
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

            # Visualização completa
            st.subheader("📊 Visualização na tela (completa)")
            modo = st.radio("Visualizar:", ["Resumo Diário", "Histórico Horário (Raw)"], horizontal=True)
            df_view = out_daily.copy() if modo == "Resumo Diário" else out_hourly.copy()

            with st.expander("Selecionar colunas para visualizar", expanded=False):
                cols_default = df_view.columns.tolist()
                selected_cols = st.multiselect("Colunas", cols_default, default=cols_default)

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
