import pandas as pd
import streamlit as st
from io import BytesIOimport requests
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

st.set_page_config(page_title="Climatempo • Opção A (60d) + Histórico GEO", layout="wide")
st.title("🌦️ Climatempo • Previsão (até 60 dias) & Histórico GEO (Hourly + Diário)")

# =========================
# SIDEBAR
# =========================
with st.sidebar:
    st.header("🔐 Tokens")
    TOKEN_PREVISAO = st.text_input("Token Previsão", value=st.secrets.get("TOKEN_PREVISAO", TOKEN_PREVISAO_DEFAULT), type="password")
    TOKEN_HIST = st.text_input("Token Histórico", value=st.secrets.get("TOKEN_HISTORICO", TOKEN_HIST_DEFAULT), type="password")
    st.divider()
    DEBUG = st.checkbox("Modo debug", value=False)
    st.caption("Dica: se o histórico der 'Latitude and Longitude not allowed', substitua lat/lon por coordenadas autorizadas do seu projeto.")

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
# GEO CODING (conversão automática cidade -> lat/lon)
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
# LOCALE lookup (para previsão por cidade)
# =========================
def buscar_cidades(city: str, uf: str):
    url = f"{BASE_V1}/locale/city?name={quote(city)}&state={uf}&token={TOKEN_PREVISAO}"
    ok, payload, status, err = http_get(url)
    if not ok:
        raise RuntimeError(f"Erro ao buscar cidade: HTTP {status} - {err}")
    return payload or []

def registrar_locale_no_token(locale_id: int):
    """
    Alguns planos exigem registrar o locale no token (senão dá 'Access forbidden').
    Endpoint via API Manager é usado amplamente para isso. [1](https://github.com/ficosta/Climatempo_API/blob/master/climatempo_api/climatempo.py)[2](https://br.meteored.com/)
    """
    url = f"{API_MANAGER}/user-token/{TOKEN_PREVISAO}/locales"
    data = {"localeId[]": str(locale_id)}
    return http_put_form(url, data=data)

# =========================
# PREVISÃO (até 60 dias via locale)
# =========================
def fetch_forecast(locale_id: int, dias: int):
    """
    A doc pública descreve forecast por cidade com rotas fixas como /days/15,
    e em alguns planos /days/270 também existe; não usamos /days/60. [3](https://br-prod.asyncgw.teams.microsoft.com/v1/objects/0-eus-d17-2bad2c4a4bc07a96e8ae80053f565549/views/original)
    """
    dias = max(1, min(MAX_DIAS, int(dias)))

    # tenta 270 (para permitir recorte até 60)
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
    """
    Correção do KeyError 'probability':
    usa .get() porque alguns dias podem não trazer rain.probability.
    """
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
            "Prob Chuva (%)": rain.get("probability"),  # pode ser None, e tudo bem
            "Umidade Min (%)": hum.get("min"),
            "Umidade Max (%)": hum.get("max"),
            "Vento Médio (km/h)": wind.get("velocity_avg") or wind.get("speed"),
            "Rajada (km/h)": wind.get("gust_max") or wind.get("gust"),
            "UV Máx": uv.get("max"),
        })
    return pd.DataFrame(rows)

# =========================
# HISTÓRICO GEO/HOURLY (Opção 2: hourly + resumo diário)
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
    """
    Normaliza resposta para DataFrame.
    - se payload['data'] for lista, usa ela
    - senão, normaliza o payload todo
    """
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
    """
    Cria resumo diário com robustez:
    - tenta descobrir coluna de data/hora
    - tenta descobrir coluna de precipitação
    - tenta descobrir temperatura e umidade
    """
    if df_hourly.empty:
        return df_hourly

    # detecta coluna de timestamp
    time_col = pick_first_col(df_hourly, ["date", "datetime", "time", "timestamp"])
    if time_col is None:
        # sem timestamp, não dá para agregar por dia
        out = pd.DataFrame({"warning": ["Sem coluna de tempo detectável para agregação diária."]})
        return out

    # converte para datetime e extrai dia
    dt = pd.to_datetime(df_hourly[time_col], errors="coerce")
    df_hourly = df_hourly.assign(_day=dt.dt.date)

    # precipitação (mm)
    rain_col = pick_first_col(df_hourly, ["rain.precipitation", "precipitation", "rain", "mm"])
    # temperatura
    t_col = pick_first_col(df_hourly, ["temperature", "temp", "temperature.value", "temperatureC"])
    # umidade
    h_col = pick_first_col(df_hourly, ["humidity", "humidity.value", "rh"])

    agg = {"_day": "first"}

    # monta agregações com fallback
    if rain_col:
        agg[rain_col] = "sum"
    if t_col:
        agg[t_col] = ["min", "max"]
    if h_col:
        agg[h_col] = "mean"

    g = df_hourly.groupby("_day").agg(agg)

    # “achata” colunas multiindex
    g.columns = ["_".join([str(x) for x in col if x]) if isinstance(col, tuple) else str(col) for col in g.columns]
    g = g.reset_index(drop=True)

    # renomeia para padrão amigável
    rename = {}
    if rain_col:
        rename[f"{rain_col}_sum"] = "chuva_total_mm"
    if t_col:
        rename[f"{t_col}_min"] = "temp_min"
        rename[f"{t_col}_max"] = "temp_max"
    if h_col:
        rename[f"{h_col}_mean"] = "umidade_media"

    g = g.rename(columns=rename)
    # recoloca a data diária
    g.insert(0, "dia", df_hourly.groupby("_day").size().index.astype(str))
    return g

# =========================
# XLSX export
# =========================
def to_xlsx(sheets: dict):
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        for name, df in sheets.items():
            df.to_excel(writer, index=False, sheet_name=name[:31])
    return buf.getvalue()

# =========================
# STATE: multipontos
# =========================
st.session_state.setdefault("points", [])  # lista de dicts

# =========================
# UI: adicionar pontos
# =========================
with st.expander("📍 Pontos (Cidade → locale_id + lat/lon)", expanded=True):
    col1, col2, col3 = st.columns([3, 1, 1])
    city_in = col1.text_input("Cidade", "Juiz de Fora")
    uf_in = col2.text_input("UF", "MG")
    buscar = col3.button("🔎 Buscar / Selecionar")

    selected = None
    if buscar:
        try:
            results = buscar_cidades(city_in.strip(), uf_in.strip().upper())
            st.session_state["search_results"] = results
        except Exception as e:
            st.error(str(e))
            st.session_state["search_results"] = []

    results = st.session_state.get("search_results", [])
    if results:
        opts = {f"{c.get('name')} - {c.get('state')} (ID {c.get('id')})": c for c in results}
        key = st.selectbox("Resultados", list(opts.keys()))
        selected = opts[key]

        add = st.button("➕ Adicionar ponto selecionado")
        if add and selected:
            try:
                lat, lon = geocode_city(selected.get("name"), selected.get("state"))
                st.session_state.points.append({
                    "city": selected.get("name"),
                    "uf": selected.get("state"),
                    "locale_id": int(selected.get("id")),
                    "lat": lat,
                    "lon": lon,
                    "hist_allowed": None,  # será definido ao testar
                    "note": "Lat/Lon sugeridos (geocoding). Se histórico negar, substitua por coordenadas autorizadas."
                })
                st.success("Ponto adicionado.")
            except Exception as e:
                st.error(f"Falha ao geocodificar/adicionar: {e}")

# =========================
# UI: editar/remover pontos
# =========================
if st.session_state.points:
    st.subheader("✅ Pontos cadastrados (edite lat/lon se necessário)")
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

    # Persistir edição
    st.session_state.points = edited.to_dict(orient="records")

    colr1, colr2 = st.columns([2, 1])
    with colr1:
        idx_remove = st.selectbox("Remover ponto (índice)", options=list(range(len(st.session_state.points))))
    with colr2:
        if st.button("🗑️ Remover"):
            st.session_state.points.pop(int(idx_remove))
            st.rerun()
else:
    st.info("Adicione ao menos um ponto.")

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

            # garante locale registrado (se necessário)
            ok_reg, _, st_reg, err_reg = registrar_locale_no_token(p["locale_id"])
            # não falha se já estiver registrado; se der erro, só loga em debug
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

            if ok:
                st.success(f"{label}: ✅ coordenadas aceitas (teste OK)")
                st.session_state.points[i]["hist_allowed"] = True
            else:
                st.error(f"{label}: ❌ (HTTP {status})")
                if DEBUG:
                    st.code(err)
                # marca como não permitido
                st.session_state.points[i]["hist_allowed"] = False

        st.rerun()

    if gerar_hist:
        hourly_all = []
        daily_all = []

        total_steps = len(st.session_state.points) * dias_hist
        done = 0
        prog = st.progress(0.0)

        for p in st.session_state.points:
            label = f"{p['city']}-{p['uf']}"

            # se nunca testou, testa automaticamente 1x
            if p.get("hist_allowed") is None:
                ok_test, _, st_test, err_test = history_geo_hourly(float(p["lat"]), float(p["lon"]), data_inicio)
                if ok_test:
                    p["hist_allowed"] = True
                else:
                    p["hist_allowed"] = False
                    st.error(f"{label}: histórico não autorizado (HTTP {st_test}) — ajuste lat/lon para coordenadas permitidas.")
                    if DEBUG:
                        st.code(err_test)
                    continue

            # se já está marcado como não permitido, pula
            if p.get("hist_allowed") is False:
                st.warning(f"{label}: pulado (histórico não permitido). Ajuste lat/lon e teste novamente.")
                continue

            # coleta hourly dia a dia
            dfs_point = []
            for i in range(dias_hist):
                d = data_inicio + timedelta(days=i)
                ok, payload, status, err = history_geo_hourly(float(p["lat"]), float(p["lon"]), d)
                done += 1
                prog.progress(min(done / total_steps, 1.0))

                if not ok:
                    st.warning(f"{label} em {d}: HTTP {status}")
                    if DEBUG:
                        st.code(err)

                    # se for o erro de coordenadas não permitidas, interrompe para este ponto
                    if "Latitude and Longitude not allowed" in (err or ""):
                        st.error(f"{label}: coordenadas NÃO permitidas — atualize lat/lon para as coordenadas autorizadas do seu projeto.")
                        p["hist_allowed"] = False
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
                # garante colunas do ponto
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


