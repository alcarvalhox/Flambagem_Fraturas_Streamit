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

TOKEN_PREVISAO = "531a8163c4464184b1e8ff89742d531f"
TOKEN_HIST = "8445618686be6cffc02c0954cbaada35"

MAX_DIAS = 60

# Candidatos de endpoint de histórico POR CIDADE/LOCALE (sem coordenadas)
# (Como o portal da Climatempo funciona por cidade/periodo, é o comportamento desejado.)
HISTORY_LOCALE_CANDIDATES = [
    # formatos comuns (você pode ajustar se o seu contrato usar outro)
    "/history/locale/{id}/days/{n}",
    "/history/locale/{id}/daily?from={from}&to={to}",
    "/history/locale/{id}?from={from}&to={to}",
    "/history/locale/{id}/daily/{from}/{to}",
]

# Endpoint conhecido GEO/Hourly (fallback)
HISTORY_GEO_HOURLY = "/history/geo/hourly?from={from}&latitude={lat}&longitude={lon}"

st.set_page_config(page_title="Climatempo • Previsão (60d) & Histórico", layout="wide")
st.title("🌦️ Climatempo • Previsão (até 60 dias) & Histórico (sem digitar coordenadas)")

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
# Geocoding automático (sem input de coords)
# =========================
@st.cache_data(ttl=86400, show_spinner=False)
def geocode_city(city: str, uf: str):
    params = {"q": f"{city}, {uf}, Brazil", "format": "json", "limit": 1}
    headers = {"User-Agent": "climatempo-streamlit-app"}
    r = requests.get(GEOCODE_URL, params=params, headers=headers, timeout=20)
    r.raise_for_status()
    data = r.json()
    if not data:
        raise ValueError("Geocoding não retornou coordenadas para esta cidade.")
    return float(data[0]["lat"]), float(data[0]["lon"])

# =========================
# Locale lookup (cidade -> locale_id)
# =========================
@st.cache_data(ttl=3600, show_spinner=False)
def buscar_cidades(city: str, uf: str):
    url = f"{BASE_V1}/locale/city?name={quote(city)}&state={uf}&token={TOKEN_PREVISAO}"
    ok, payload, status, err = http_get(url)
    if not ok:
        raise RuntimeError(f"Erro ao buscar cidade. HTTP {status}: {err}")
    return payload or []

def registrar_locale_no_token(locale_id: int):
    # Necessário em alguns planos: habilita o locale no token (evita "Access forbidden").
    # Esse fluxo é amplamente usado em integrações com a Climatempo. [1](https://github.com/guieiras-dev/home-assistant-climatempo/)[2](https://apiadvisor.climatempo.com.br/doc/index.html)
    url = f"{API_MANAGER}/user-token/{TOKEN_PREVISAO}/locales"
    data = {"localeId[]": str(locale_id)}
    return http_put_form(url, data=data)

# =========================
# Previsão (até 60 dias)
# =========================
def fetch_forecast(locale_id: int, dias: int):
    """
    A doc pública descreve /forecast/locale/:id/days/15 e /days/270. [2](https://apiadvisor.climatempo.com.br/doc/index.html)
    Não chamamos /days/60. Chamamos 270 e recortamos até 60.
    """
    dias = max(1, min(MAX_DIAS, int(dias)))

    url270 = f"{BASE_V1}/forecast/locale/{locale_id}/days/270?token={TOKEN_PREVISAO}"
    ok, payload, status, err = http_get(url270)
    if ok and isinstance(payload, dict) and "data" in payload:
        return True, payload["data"][:dias], status, "", 270

    url15 = f"{BASE_V1}/forecast/locale/{locale_id}/days/15?token={TOKEN_PREVISAO}"
    ok2, payload2, status2, err2 = http_get(url15)
    if ok2 and isinstance(payload2, dict) and "data" in payload2:
        return True, payload2["data"][:min(dias, 15)], status2, "", 15

    return False, None, status2 if not ok else status, err2 if not ok2 else err, None

def forecast_to_df(days_list: list, point_label: str, locale_id: int):
    rows = []
    for d in days_list:
        rain = d.get("rain", {}) or {}
        temp = d.get("temperature", {}) or {}
        hum = d.get("humidity", {}) or {}
        wind = d.get("wind", {}) or {}
        uv = d.get("uv", {}) or {}

        rows.append({
            "Ponto": point_label,
            "locale_id": locale_id,
            "Data": d.get("date_br") or d.get("date"),
            "Temp Min (°C)": temp.get("min"),
            "Temp Max (°C)": temp.get("max"),
            "Chuva (mm)": rain.get("precipitation"),
            "Prob Chuva (%)": rain.get("probability"),  # pode não existir em alguns retornos
            "Umidade Min (%)": hum.get("min"),
            "Umidade Max (%)": hum.get("max"),
            "Vento Médio (km/h)": wind.get("velocity_avg") or wind.get("speed"),
            "Rajada (km/h)": wind.get("gust_max") or wind.get("gust"),
            "UV Máx": uv.get("max"),
        })
    return pd.DataFrame(rows)

# =========================
# Histórico (sem coordenadas na UI)
# Estratégia:
# 1) Tenta histórico por locale (portal-like)
# 2) Fallback para GEO/Hourly com geocoding automático
# =========================
def build_url(path: str, token: str):
    if "?" in path:
        return f"{BASE_V1}{path}&token={token}"
    return f"{BASE_V1}{path}?token={token}"

def try_history_by_locale(locale_id: int, from_dt: date, to_dt: date):
    """
    Tenta encontrar um endpoint de histórico por cidade/locale.
    Retorna (ok, payload, status, err, endpoint_used)
    """
    from_str = from_dt.strftime("%Y-%m-%d")
    to_str = to_dt.strftime("%Y-%m-%d")
    days = (to_dt - from_dt).days + 1

    last_status, last_err = None, None

    for template in HISTORY_LOCALE_CANDIDATES:
        # ✅ CORREÇÃO: não usar from=... (palavra reservada). Use format_map com dict.
        path = template.format_map({
            "id": locale_id,
            "n": days,
            "from": from_str,
            "to": to_str
        })

        url = build_url(path, TOKEN_HIST)
        ok, payload, status, err = http_get(url)

        if ok:
            return True, payload, status, "", path

        last_status, last_err = status, err

    return False, None, last_status, last_err, None

def history_geo_hourly_from_city(city: str, uf: str, from_dt: date):
    """
    Fallback: usa geocoding automático e chama /history/geo/hourly
    """
    lat, lon = geocode_city(city, uf)
    from_str = from_dt.strftime("%Y-%m-%d")
    path = HISTORY_GEO_HOURLY.format(from=from_str, lat=lat, lon=lon)
    url = build_url(path, TOKEN_HIST)
    ok, payload, status, err = http_get(url)
    return ok, payload, status, err, (lat, lon), path

def normalize_history_payload(payload):
    # Tenta padrões comuns
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
    if t_col:    agg[t_col] = ["min", "max"]
    if h_col:    agg[h_col] = "mean"

    g = df_hourly.groupby("_day").agg(agg)
    g.columns = ["_".join([str(x) for x in col if x]) if isinstance(col, tuple) else str(col) for col in g.columns]
    g = g.reset_index().rename(columns={"_day": "dia"})
    g["dia"] = g["dia"].astype(str)

    # nomes amigáveis
    rename = {}
    if rain_col: rename[f"{rain_col}_sum"] = "chuva_total_mm"
    if t_col:
        rename[f"{t_col}_min"] = "temp_min"
        rename[f"{t_col}_max"] = "temp_max"
    if h_col: rename[f"{h_col}_mean"] = "umidade_media"

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
# STATE / UI
# =========================
st.session_state.setdefault("points", [])
st.session_state.setdefault("search_results", [])

with st.expander("📍 Adicionar cidades (multipontos)", expanded=True):
    c1, c2, c3 = st.columns([3, 1, 1])
    city_in = c1.text_input("Cidade", "Juiz de Fora")
    uf_in = c2.text_input("UF", "MG")
    buscar = c3.button("Buscar")

    if buscar:
        st.session_state.search_results = buscar_cidades(city_in.strip(), uf_in.strip().upper())

    if st.session_state.search_results:
        opts = {f"{c.get('name')} - {c.get('state')} (ID {c.get('id')})": c for c in st.session_state.search_results}
        sel = st.selectbox("Resultados", list(opts.keys()))
        chosen = opts[sel]

        if st.button("➕ Adicionar cidade selecionada"):
            st.session_state.points.append({
                "city": chosen.get("name"),
                "uf": chosen.get("state"),
                "locale_id": int(chosen.get("id")),
            })
            st.success("Cidade adicionada.")

if st.session_state.points:
    st.dataframe(pd.DataFrame(st.session_state.points), use_container_width=True)
else:
    st.info("Adicione pelo menos uma cidade.")

tab_prev, tab_hist = st.tabs(["🔮 Previsão (até 60 dias)", "🕒 Histórico (Portal-like, sem coordenadas)"])

# =========================
# TAB PREVISÃO
# =========================
with tab_prev:
    dias_prev = st.slider("Dias de previsão", 1, MAX_DIAS, 15)
    if st.button("Gerar Previsão XLSX", use_container_width=True):
        all_df = []
        for p in st.session_state.points:
            label = f"{p['city']}-{p['uf']}"

            # tenta registrar locale no token (se for necessário)
            registrar_locale_no_token(p["locale_id"])

            ok, days, status, err, used = fetch_forecast(p["locale_id"], dias_prev)
            if not ok:
                st.error(f"Previsão falhou para {label}: HTTP {status}")
                st.code(err)
                continue

            df = forecast_to_df(days, label, p["locale_id"])
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

# =========================
# TAB HISTÓRICO
# =========================
with tab_hist:
    dias_hist = st.slider("Dias de histórico", 1, MAX_DIAS, 7)
    data_inicio = st.date_input("Data inicial", value=date.today() - timedelta(days=dias_hist))
    data_fim = data_inicio + timedelta(days=dias_hist - 1)

    if st.button("Gerar Histórico XLSX (Hourly + Diário)", use_container_width=True):
        hourly_all = []
        daily_all = []

        for p in st.session_state.points:
            label = f"{p['city']}-{p['uf']}"

            # 1) tenta histórico por locale (SEM coordenadas)
            ok_loc, payload, status, err, endpoint_used = try_history_by_locale(p["locale_id"], data_inicio, data_fim)

            if ok_loc:
                df_hist = normalize_history_payload(payload)
                df_hist.insert(0, "Ponto", label)
                df_hist.insert(1, "endpoint_used", endpoint_used)
                hourly_all.append(df_hist)

                # se já vier diário, o resumo pode ser o próprio df (mas tentamos gerar)
                df_daily = build_daily_summary(df_hist.copy())
                df_daily.insert(0, "Ponto", label)
                daily_all.append(df_daily)
                continue

            # 2) fallback GEO/hourly com coordenadas automáticas (sem input do usuário)
            ok_geo, payload2, status2, err2, (lat, lon), endpoint_geo = history_geo_hourly_from_city(p["city"], p["uf"], data_inicio)

            if not ok_geo:
                st.error(f"Histórico falhou para {label}. HTTP {status2}")
                st.code(err2)

                # mensagem prática para o caso clássico:
                if "Latitude and Longitude not allowed" in (err2 or ""):
                    st.warning(
                        f"{label}: o token histórico não permite as coordenadas geocodificadas automaticamente (centro da cidade). "
                        "Isso indica whitelist de pontos no token. Para reproduzir o portal 100%, é necessário um endpoint histórico por cidade "
                        "habilitado no seu contrato (ou a lista de pontos autorizados por cidade)."
                    )
                continue

            df_hist2 = normalize_history_payload(payload2)
            df_hist2.insert(0, "Ponto", label)
            df_hist2.insert(1, "endpoint_used", endpoint_geo)
            df_hist2.insert(2, "lat", lat)
            df_hist2.insert(3, "lon", lon)
            hourly_all.append(df_hist2)

            df_daily2 = build_daily_summary(df_hist2.copy())
            df_daily2.insert(0, "Ponto", label)
            daily_all.append(df_daily2)

        if hourly_all:
            out_hourly = pd.concat(hourly_all, ignore_index=True)
            out_daily = pd.concat(daily_all, ignore_index=True) if daily_all else pd.DataFrame()

            st.subheader("Prévia do Resumo Diário")
            st.dataframe(out_daily.head(200), use_container_width=True)

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
