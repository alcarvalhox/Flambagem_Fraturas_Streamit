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

st.set_page_config(page_title="Climatempo • Previsão (60d) & Histórico GEO", layout="wide")
st.title("🌦️ Climatempo • Previsão (até 60 dias) & Histórico GEO (Hourly + Diário)")

# =========================
# SIDEBAR
# =========================
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
# GEO coding (cidade -> lat/lon sugeridos)
# =========================
@st.cache_data(ttl=86400, show_spinner=False)
def geocode_city(city: str, uf: str):
    params = {"q": f"{city}, {uf}, Brazil", "format": "json", "limit": 1}
    headers = {"User-Agent": "climatempo-streamlit-app"}
    r = requests.get(GEOCODE_URL, params=params, headers=headers, timeout=20)
    r.raise_for_status()
    data = r.json()
    if not data:
        raise ValueError("Não foi possível geocodificar a cidade.")
    return float(data[0]["lat"]), float(data[0]["lon"])

# =========================
# Locale lookup + register (previsão)
# =========================
def buscar_cidades(city: str, uf: str):
    url = f"{BASE_V1}/locale/city?name={quote(city)}&state={uf}&token={TOKEN_PREVISAO}"
    ok, payload, status, err = http_get(url)
    if not ok:
        raise RuntimeError(f"Erro ao buscar cidade: HTTP {status} - {err}")
    return payload or []

def registrar_locale_no_token(locale_id: int):
    # Recurso comum quando aparece "Access forbidden"
    url = f"{API_MANAGER}/user-token/{TOKEN_PREVISAO}/locales"
    data = {"localeId[]": str(locale_id)}
    return http_put_form(url, data=data)

# =========================
# Forecast (até 60 dias por locale)
# =========================
def fetch_forecast(locale_id: int, dias: int):
    dias = max(1, min(MAX_DIAS, int(dias)))

    # tenta 270 -> recorta 60
    url270 = f"{BASE_V1}/forecast/locale/{locale_id}/days/270?token={TOKEN_PREVISAO}"
    ok, payload, status, err = http_get(url270)
    if ok and isinstance(payload, dict) and "data" in payload:
        return True, payload["data"][:dias], status, "", 270

    # fallback 15
    url15 = f"{BASE_V1}/forecast/locale/{locale_id}/days/15?token={TOKEN_PREVISAO}"
    ok2, payload2, status2, err2 = http_get(url15)
    if ok2 and isinstance(payload2, dict) and "data" in payload2:
        return True, payload2["data"][:min(dias, 15)], status2, "", 15

    return False, None, (status2 if not ok else status), (err2 if not ok2 else err), None

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
            "Prob Chuva (%)": rain.get("probability"),  # pode não existir -> None (sem quebrar)
            "Umidade Min (%)": hum.get("min"),
            "Umidade Max (%)": hum.get("max"),
            "Vento Médio (km/h)": wind.get("velocity_avg") or wind.get("speed"),
            "Rajada (km/h)": wind.get("gust_max") or wind.get("gust"),
            "UV Máx": uv.get("max"),
        })
    return pd.DataFrame(rows)

# =========================
# History GEO/hourly + resumo diário
# =========================
def history_geo_hourly(lat: float, lon: float, from_date: date):
    from_str = from_date.strftime("%Y-%m-%d")
    url = (
        f"{BASE_V1}/history/geo/hourly"
        f"?token={TOKEN_HIST}"
        f"&from={from_str}"
        f"&latitude={lat}"
        f"&longitude={lon}"
    )
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
    if rain_col:
        agg[rain_col] = "sum"
    if t_col:
        agg[t_col] = ["min", "max"]
    if h_col:
        agg[h_col] = "mean"

    g = df_hourly.groupby("_day").agg(agg)
    g.columns = ["_".join([str(x) for x in col if x]) if isinstance(col, tuple) else str(col) for col in g.columns]
    g = g.reset_index()

    rename = {}
    if rain_col:
        rename[f"{rain_col}_sum"] = "chuva_total_mm"
    if t_col:
        rename[f"{t_col}_min"] = "temp_min"
        rename[f"{t_col}_max"] = "temp_max"
    if h_col:
        rename[f"{h_col}_mean"] = "umidade_media"

    g = g.rename(columns=rename).rename(columns={"_day": "dia"})
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
# STATE
# =========================
st.session_state.setdefault("points", [])
st.session_state.setdefault("search_results", [])

# =========================
# UI: Adicionar ponto
# =========================
with st.expander("📍 Pontos (Cidade → locale_id + lat/lon sugeridos)", expanded=True):
    col1, col2, col3 = st.columns([3, 1, 1])
    city_in = col1.text_input("Cidade", "Juiz de Fora")
    uf_in = col2.text_input("UF", "MG")
    buscar = col3.button("Buscar / Selecionar")

    if buscar:
        try:
            st.session_state.search_results = buscar_cidades(city_in.strip(), uf_in.strip().upper())
        except Exception as e:
            st.error(str(e))
            st.session_state.search_results = []

    if st.session_state.search_results:
        opts = {f"{c.get('name')} - {c.get('state')} (ID {c.get('id')})": c for c in st.session_state.search_results}
        key = st.selectbox("Resultados", list(opts.keys()))
        selected = opts[key]

        if st.button("➕ Adicionar ponto selecionado"):
            try:
                lat, lon = geocode_city(selected.get("name"), selected.get("state"))
                st.session_state.points.append({
                    "city": selected.get("name"),
                    "uf": selected.get("state"),
                    "locale_id": int(selected.get("id")),
                    "lat": lat,
                    "lon": lon,
                    # controle de permissão histórico:
                    "hist_allowed": None,
                    "hist_last_test_date": "",
                    "hist_last_status": "",
                    "hist_last_detail": "Lat/Lon sugeridos via geocoding. Se o histórico negar, substitua por coordenadas autorizadas do seu projeto.",
                    # guarda coords testadas para detectar alteração:
                    "_hist_coords_hash": f"{lat:.6f},{lon:.6f}"
                })
                st.success("Ponto adicionado.")
            except Exception as e:
                st.error(f"Falha ao geocodificar/adicionar: {e}")

# =========================
# UI: Editar/remover pontos + RESET automático de hist_allowed se lat/lon mudar
# =========================
if st.session_state.points:
    st.subheader("✅ Pontos cadastrados (edite lat/lon; ao mudar, histórico será re-testável automaticamente)")
    df_points = pd.DataFrame(st.session_state.points)

    edited = st.data_editor(
        df_points,
        use_container_width=True,
        num_rows="fixed",
        column_config={
            "lat": st.column_config.NumberColumn(format="%.6f"),
            "lon": st.column_config.NumberColumn(format="%.6f"),
            "hist_allowed": st.column_config.CheckboxColumn("hist_allowed"),
        }
    )

    # Detecta mudança de lat/lon e reseta hist_allowed automaticamente
    new_points = edited.to_dict(orient="records")
    for i, p in enumerate(new_points):
        lat = float(p["lat"])
        lon = float(p["lon"])
        new_hash = f"{lat:.6f},{lon:.6f}"
        old_hash = st.session_state.points[i].get("_hist_coords_hash", "")

        if new_hash != old_hash:
            # Coordenadas mudaram -> permitir novo teste
            p["hist_allowed"] = None
            p["hist_last_test_date"] = ""
            p["hist_last_status"] = ""
            p["hist_last_detail"] = "Coordenadas alteradas. Clique em 'Testar coordenadas' novamente."
            p["_hist_coords_hash"] = new_hash

    st.session_state.points = new_points

    colr1, colr2 = st.columns([2, 1])
    with colr1:
        idx_remove = st.selectbox("Remover ponto (índice)", options=list(range(len(st.session_state.points))))
    with colr2:
        if st.button("🗑️ Remover"):
            st.session_state.points.pop(int(idx_remove))
            st.rerun()
else:
    st.info("Adicione ao menos um ponto para usar previsão e/ou histórico.")

st.divider()
tab_prev, tab_hist = st.tabs(["🔮 Previsão (até 60 dias)", "🕒 Histórico GEO (Hourly + Diário)"])

# =========================
# TAB: PREVISÃO
# =========================
with tab_prev:
    dias_prev = st.slider("Dias de previsão", 1, MAX_DIAS, 15)
    gerar_prev = st.button("⚙️ Gerar Previsão (XLSX)", use_container_width=True)

    if gerar_prev:
        all_df = []
        for p in st.session_state.points:
            label = f"{p['city']}-{p['uf']}"

            # registra locale (se necessário) — não bloqueia o app se falhar
            ok_reg, _, st_reg, err_reg = registrar_locale_no_token(p["locale_id"])
            if (not ok_reg) and DEBUG:
                st.warning(f"[DEBUG] Registro locale falhou para {label}: HTTP {st_reg}")
                st.code(err_reg)

            ok, days, status, err, used = fetch_forecast(p["locale_id"], dias_prev)
            if not ok:
                st.error(f"Erro em {label}: HTTP {status}")
                if DEBUG:
                    st.code(err)
                continue

            df = forecast_to_df(days, label, p["locale_id"])
            df["endpoint_days_used"] = used
            all_df.append(df)

        if all_df:
            df_final = pd.concat(all_df, ignore_index=True)
            st.dataframe(df_final, use_container_width=True)

            xlsx = to_xlsx({"Previsao": df_final})
            ts = datetime.now().strftime("%Y%m%d_%H%M")
            st.download_button(
                "⬇️ Download Previsão (XLSX)",
                data=xlsx,
                file_name=f"previsao_{ts}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
        else:
            st.warning("Nenhuma previsão gerada.")

# =========================
# TAB: HISTÓRICO
# =========================
with tab_hist:
    dias_hist = st.slider("Dias de histórico", 1, MAX_DIAS, 7)
    data_inicio = st.date_input("Data inicial (from)", value=date.today() - timedelta(days=dias_hist))

    colh1, colh2 = st.columns([1.2, 1.8])
    testar_coords = colh1.button("🔎 Testar coordenadas (para Histórico)", use_container_width=True)
    gerar_hist = colh2.button("⚙️ Gerar Histórico (Hourly + Diário) XLSX", use_container_width=True)

    if testar_coords:
        for i, p in enumerate(st.session_state.points):
            label = f"{p['city']}-{p['uf']}"
            ok, payload, status, err = history_geo_hourly(float(p["lat"]), float(p["lon"]), data_inicio)

            st.session_state.points[i]["hist_last_test_date"] = data_inicio.strftime("%Y-%m-%d")
            st.session_state.points[i]["hist_last_status"] = f"HTTP {status}"

            if ok:
                st.success(f"{label}: ✅ coordenadas aceitas (teste OK)")
                st.session_state.points[i]["hist_allowed"] = True
                st.session_state.points[i]["hist_last_detail"] = "OK"
            else:
                st.error(f"{label}: ❌ coordenadas recusadas (HTTP {status})")
                st.session_state.points[i]["hist_allowed"] = False
                st.session_state.points[i]["hist_last_detail"] = err
                if DEBUG:
                    st.code(err)

        st.rerun()

    if gerar_hist:
        hourly_all = []
        daily_all = []

        for i, p in enumerate(st.session_state.points):
            label = f"{p['city']}-{p['uf']}"

            # se não testou ou coords mudaram, testa automaticamente 1x
            if p.get("hist_allowed") is None:
                ok_test, _, st_test, err_test = history_geo_hourly(float(p["lat"]), float(p["lon"]), data_inicio)
                st.session_state.points[i]["hist_last_test_date"] = data_inicio.strftime("%Y-%m-%d")
                st.session_state.points[i]["hist_last_status"] = f"HTTP {st_test}"

                if ok_test:
                    st.session_state.points[i]["hist_allowed"] = True
                    st.session_state.points[i]["hist_last_detail"] = "OK"
                else:
                    st.session_state.points[i]["hist_allowed"] = False
                    st.session_state.points[i]["hist_last_detail"] = err_test
                    st.warning(f"{label}: pulado (histórico não permitido). Ajuste lat/lon e teste novamente.")
                    if DEBUG:
                        st.code(err_test)
                    continue

            # se não permitido, pula
            if st.session_state.points[i].get("hist_allowed") is False:
                st.warning(f"{label}: pulado (histórico não permitido). Ajuste lat/lon e teste novamente.")
                continue

            # coleta hourly dia a dia
            dfs_point = []
            for j in range(dias_hist):
                d = data_inicio + timedelta(days=j)
                ok, payload, status, err = history_geo_hourly(float(p["lat"]), float(p["lon"]), d)
                if not ok:
                    st.warning(f"{label} em {d}: HTTP {status}")
                    if DEBUG:
                        st.code(err)

                    if "Latitude and Longitude not allowed" in (err or ""):
                        st.error(f"{label}: coordenadas NÃO permitidas — atualize lat/lon para coordenadas autorizadas.")
                        st.session_state.points[i]["hist_allowed"] = False
                        st.session_state.points[i]["hist_last_detail"] = err
                        break
                    continue

                dfh = normalize_history_payload(payload)
                dfh.insert(0, "Ponto", label)
                dfh.insert(1, "from_date", d.strftime("%Y-%m-%d"))
                dfh.insert(2, "lat", float(p["lat"]))
                dfh.insert(3, "lon", float(p["lon"]))
                dfs_point.append(dfh)

            if dfs_point:
                df_hourly = pd.concat(dfs_point, ignore_index=True)
                hourly_all.append(df_hourly)

                df_daily = build_daily_summary(df_hourly.copy())
                if "Ponto" not in df_daily.columns:
                    df_daily.insert(0, "Ponto", label)
                daily_all.append(df_daily)

        if hourly_all:
            out_hourly = pd.concat(hourly_all, ignore_index=True)
            out_daily = pd.concat(daily_all, ignore_index=True) if daily_all else pd.DataFrame()

            st.subheader("📌 Resumo diário (prévia)")
            st.dataframe(out_daily.head(200) if not out_daily.empty else out_hourly.head(200), use_container_width=True)

            xlsx = to_xlsx({
                "Historico_Horario": out_hourly,
                "Resumo_Diario": out_daily if not out_daily.empty else pd.DataFrame({"info": ["Resumo diário indisponível (colunas não detectadas)."]})
            })
            ts = datetime.now().strftime("%Y%m%d_%H%M")
            st.download_button(
                "⬇️ Download Histórico (XLSX)",
                data=xlsx,
                file_name=f"historico_{ts}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
        else:
            st.warning("Nenhum histórico foi gerado. Ajuste lat/lon para coordenadas permitidas e teste novamente.")
