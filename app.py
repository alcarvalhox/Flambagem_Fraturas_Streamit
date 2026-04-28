import streamlit as st
import requests
import pandas as pd
from io import BytesIO
from urllib.parse import quote
from datetime import datetime, timedelta, date

# =========================
# CONFIG
# =========================
BASE_URL = "http://apiadvisor.climatempo.com.br/api/v1"
API_MANAGER = "http://apiadvisor.climatempo.com.br/api-manager"

TOKEN_PREVISAO = "531a8163c4464184b1e8ff89742d531f"
TOKEN_HISTORICO = "8445618686be6cffc02c0954cbaada35"

st.set_page_config(page_title="Climatempo • Previsão & Histórico (Excel)", layout="wide")
st.title("🌦️ Climatempo • Previsão & Histórico (Excel)")

# =========================
# HTTP helpers
# =========================
def http_get(url: str, timeout: int = 30):
    try:
        r = requests.get(url, timeout=timeout)
        if r.status_code >= 400:
            return False, None, r.status_code, r.text
        if not r.text:
            return True, {}, r.status_code, ""
        return True, r.json(), r.status_code, ""
    except Exception as e:
        return False, None, -1, str(e)

def http_put_form(url: str, data: dict, timeout: int = 30):
    try:
        r = requests.put(url, data=data, timeout=timeout)
        if r.status_code >= 400:
            return False, None, r.status_code, r.text
        if not r.text:
            return True, {}, r.status_code, ""
        return True, r.json(), r.status_code, ""
    except Exception as e:
        return False, None, -1, str(e)

# =========================
# API functions
# =========================
@st.cache_data(ttl=3600, show_spinner=False)
def buscar_cidades(nome: str, uf: str):
    # Busca por nome + UF
    url = f"{BASE_URL}/locale/city?name={quote(nome)}&state={uf}&token={TOKEN_PREVISAO}"
    ok, payload, status, err = http_get(url)
    if not ok:
        raise RuntimeError(f"Erro ao buscar cidade. HTTP {status}: {err}")
    return payload or []

def registrar_locale_no_token(token: str, locale_id: int):
    # Alguns planos exigem registrar locale no token.
    url = f"{API_MANAGER}/user-token/{token}/locales"
    data = {"localeId[]": str(locale_id)}
    return http_put_form(url, data=data)

def fetch_previsao(locale_id: int, dias_desejados: int):
    """
    Previsão: endpoints fixos (15 ou 270). Nunca chamamos days/60.
    - se dias <=15 => days/15
    - se dias >15 => tenta days/270; se falhar, cai para days/15
    (Estrutura e uso do token via query param são descritos na doc pública.) [1](https://apiadvisor.climatempo.com.br/doc/index.html)
    """
    if dias_desejados <= 15:
        url = f"{BASE_URL}/forecast/locale/{locale_id}/days/15?token={TOKEN_PREVISAO}"
        ok, payload, status, err = http_get(url)
        return ok, payload, status, err, 15

    url270 = f"{BASE_URL}/forecast/locale/{locale_id}/days/270?token={TOKEN_PREVISAO}"
    ok, payload, status, err = http_get(url270)
    if ok:
        return True, payload, status, err, 270

    # fallback
    url15 = f"{BASE_URL}/forecast/locale/{locale_id}/days/15?token={TOKEN_PREVISAO}"
    ok2, payload2, status2, err2 = http_get(url15)
    return ok2, payload2, status2, (err or err2), 15

def fetch_historico_geo_hourly(lat: float, lon: float, from_date: date):
    """
    Histórico GEO / HOURLY:
    /history/geo/hourly?token=...&from=YYYY-MM-DD&latitude=...&longitude=...
    (endpoint fornecido pelo usuário)
    """
    from_str = from_date.strftime("%Y-%m-%d")
    url = (
        f"{BASE_URL}/history/geo/hourly"
        f"?token={TOKEN_HISTORICO}"
        f"&from={from_str}"
        f"&latitude={lat}"
        f"&longitude={lon}"
    )
    return http_get(url)

# =========================
# Data normalization
# =========================
def flatten_forecast(payload: dict, dias: int) -> pd.DataFrame:
    rows = []
    data_list = (payload or {}).get("data", [])[:dias]
    for d in data_list:
        rain = d.get("rain", {}) or {}
        temp = d.get("temperature", {}) or {}
        hum = d.get("humidity", {}) or {}
        press = d.get("pressure", {}) or {}
        wind = d.get("wind", {}) or {}
        uv = d.get("uv", {}) or {}
        sun = d.get("sun", {}) or {}

        txt = (d.get("text_icon", {}) or {}).get("text", {}) or {}
        phrase = (txt.get("phrase", {}) or {}).get("reduced")

        rows.append({
            "date": d.get("date"),
            "date_br": d.get("date_br"),
            "temp_min_c": temp.get("min"),
            "temp_max_c": temp.get("max"),
            "humidity_min_pct": hum.get("min"),
            "humidity_max_pct": hum.get("max"),
            "pressure_hpa": press.get("pressure") if isinstance(press, dict) else press,
            "rain_prob_pct": rain.get("probability"),
            "rain_mm": rain.get("precipitation"),
            "wind_avg_kmh": wind.get("velocity_avg") or wind.get("speed"),
            "wind_gust_kmh": wind.get("gust_max") or wind.get("gust"),
            "uv_max": uv.get("max"),
            "sunrise": sun.get("sunrise"),
            "sunset": sun.get("sunset"),
            "summary_pt": phrase
        })
    return pd.DataFrame(rows)

def flatten_history(payload) -> pd.DataFrame:
    """
    Histórico hourly pode vir como dict com listas internas OU lista direta.
    Como não temos schema público fácil, normalizamos genericamente com json_normalize.
    """
    if payload is None:
        return pd.DataFrame()

    # Caso típico: dict com alguma chave de lista (ex.: "data", "hourly", etc.)
    if isinstance(payload, dict):
        # tenta chaves prováveis
        for key in ["data", "hourly", "hours", "history"]:
            if key in payload and isinstance(payload[key], list):
                df = pd.json_normalize(payload[key])
                return df
        # fallback: normaliza o dict inteiro
        return pd.json_normalize(payload)

    # Caso: lista
    if isinstance(payload, list):
        return pd.json_normalize(payload)

    # fallback
    return pd.DataFrame({"raw": [str(payload)]})

def df_to_xlsx_bytes(df: pd.DataFrame, sheet_name: str):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    return output.getvalue()

# =========================
# UI - City selection
# =========================
st.session_state.setdefault("cidades", [])
st.session_state.setdefault("ultima_busca", [])

st.subheader("📍 Cidades")
c1, c2, c3, c4 = st.columns([3, 1, 1.2, 1.2])
with c1:
    nome_cidade = st.text_input("Cidade", "Juiz de Fora")
with c2:
    uf = st.text_input("UF", "MG")
with c3:
    buscar_btn = st.button("🔎 Buscar", use_container_width=True)
with c4:
    add_btn = st.button("➕ Adicionar à lista", use_container_width=True)

if buscar_btn:
    try:
        resultados = buscar_cidades(nome_cidade.strip(), uf.strip().upper())
        st.session_state["ultima_busca"] = resultados
        if resultados:
            st.success(f"{len(resultados)} resultado(s) encontrado(s). Selecione e adicione.")
        else:
            st.warning("Nenhuma cidade encontrada.")
    except Exception as e:
        st.error(str(e))

cidade_sel = None
if st.session_state["ultima_busca"]:
    opcoes = {f"{c.get('name')} - {c.get('state')} (ID {c.get('id')})": c for c in st.session_state["ultima_busca"]}
    escolha = st.selectbox("Resultados", list(opcoes.keys()))
    cidade_sel = opcoes[escolha]

if add_btn and cidade_sel:
    ja = any(x["id"] == cidade_sel["id"] for x in st.session_state["cidades"])
    if not ja:
        st.session_state["cidades"].append({
            "id": cidade_sel.get("id"),
            "name": cidade_sel.get("name"),
            "state": cidade_sel.get("state"),
            # latitude/longitude podem ou não vir no payload. Guardamos se existirem.
            "latitude": cidade_sel.get("latitude") or cidade_sel.get("lat"),
            "longitude": cidade_sel.get("longitude") or cidade_sel.get("lon"),
        })
        st.success("Cidade adicionada.")
    else:
        st.info("Cidade já está na lista.")

if st.session_state["cidades"]:
    st.write("**Cidades na lista:**")
    st.dataframe(pd.DataFrame(st.session_state["cidades"]), use_container_width=True)
else:
    st.info("Adicione uma ou mais cidades para gerar relatórios.")

st.divider()
tab_prev, tab_hist = st.tabs(["🔮 Previsão", "🕒 Histórico (GEO / Hourly)"])

# =========================
# TAB: Forecast
# =========================
with tab_prev:
    st.subheader("🔮 Previsão (até 60 dias ou limite do seu plano)")
    dias_prev = st.slider("Período de previsão (dias)", 1, 60, 15)

    b1, b2 = st.columns([1.4, 1.4])
    gerar_prev = b1.button("⚙️ Gerar Previsão", use_container_width=True)
    vincular_prev = b2.button("🔗 Vincular cidades ao token (previsão)", use_container_width=True)

    if vincular_prev and st.session_state["cidades"]:
        for c in st.session_state["cidades"]:
            ok, _, status, err = registrar_locale_no_token(TOKEN_PREVISAO, c["id"])
            if ok:
                st.success(f"Vinculada ao token de previsão: {c['name']}-{c['state']} (ID {c['id']})")
            else:
                st.error(f"Falha ao vincular (previsão) {c['name']} (ID {c['id']}). HTTP {status}")
                st.code(err)

    if gerar_prev:
        if not st.session_state["cidades"]:
            st.warning("Adicione pelo menos uma cidade.")
        else:
            all_dfs = []
            with st.spinner("Consultando previsões..."):
                for c in st.session_state["cidades"]:
                    ok, payload, status, err, endpoint_dias = fetch_previsao(c["id"], dias_prev)

                    # se falhar por acesso, tenta registrar e repetir 1x
                    if not ok and ("Access forbidden" in (err or "") or status in (400, 403)):
                        reg_ok, _, reg_status, reg_err = registrar_locale_no_token(TOKEN_PREVISAO, c["id"])
                        if reg_ok:
                            ok, payload, status, err, endpoint_dias = fetch_previsao(c["id"], dias_prev)
                        else:
                            st.error(f"Não foi possível registrar {c['name']} no token. HTTP {reg_status}")
                            st.code(reg_err)

                    if not ok:
                        st.error(f"Erro na previsão de {c['name']}-{c['state']} (ID {c['id']}). HTTP {status}")
                        st.code(err)
                        continue

                    df = flatten_forecast(payload, dias_prev)
                    df.insert(0, "city", f"{c['name']}-{c['state']}")
                    df.insert(1, "locale_id", c["id"])
                    df.insert(2, "endpoint_days_used", endpoint_dias)
                    all_dfs.append(df)

            if all_dfs:
                df_final = pd.concat(all_dfs, ignore_index=True)
                st.dataframe(df_final, use_container_width=True)

                xlsx = df_to_xlsx_bytes(df_final, "Previsao")
                ts = datetime.now().strftime("%Y%m%d_%H%M")
                st.download_button(
                    "⬇️ Download Previsão (XLSX)",
                    data=xlsx,
                    file_name=f"previsao_{ts}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
            else:
                st.info("Nenhuma previsão gerada.")

# =========================
# TAB: History GEO / Hourly
# =========================
with tab_hist:
    st.subheader("🕒 Histórico (GEO / Hourly) — até 60 dias (loop diário)")

    colA, colB = st.columns([1.1, 1.1])
    with colA:
        dias_hist = st.slider("Quantidade de dias de histórico", 1, 60, 7)
    with colB:
        data_final = st.date_input("Data final (inclusive)", value=date.today())

    st.caption(
        "O endpoint de histórico do seu contrato é por latitude/longitude e data 'from'. "
        "O app consulta dia a dia e consolida em um único Excel."
    )

    gerar_hist = st.button("⚙️ Gerar Histórico (GEO/Hourly)", use_container_width=True)

    if gerar_hist:
        if not st.session_state["cidades"]:
            st.warning("Adicione pelo menos uma cidade.")
            st.stop()

        all_hist = []
        progress = st.progress(0)
        total_steps = len(st.session_state["cidades"]) * dias_hist
        step = 0

        with st.spinner("Consultando histórico horário..."):
            for c in st.session_state["cidades"]:
                # Latitude/longitude: se não vierem do locale/city, pedir manualmente
                lat = c.get("latitude")
                lon = c.get("longitude")

                if lat is None or lon is None:
                    st.warning(
                        f"Latitude/Longitude não disponíveis para {c['name']}-{c['state']} (ID {c['id']}). "
                        "Informe manualmente abaixo."
                    )
                    lat = st.number_input(f"Latitude para {c['name']}-{c['state']}", value=-20.0, format="%.6f", key=f"lat_{c['id']}")
                    lon = st.number_input(f"Longitude para {c['name']}-{c['state']}", value=-44.0, format="%.6f", key=f"lon_{c['id']}")
                    # salva na sessão
                    c["latitude"], c["longitude"] = float(lat), float(lon)

                lat, lon = float(lat), float(lon)

                # intervalo de datas
                start_date = data_final - timedelta(days=dias_hist - 1)

                for i in range(dias_hist):
                    dia = start_date + timedelta(days=i)

                    ok, payload, status, err = fetch_historico_geo_hourly(lat, lon, dia)

                    step += 1
                    progress.progress(min(step / total_steps, 1.0))

                    if not ok:
                        st.error(f"Erro no histórico {c['name']}-{c['state']} em {dia} (HTTP {status})")
                        st.code(err)
                        continue

                    dfh = flatten_history(payload)

                    # adiciona metadados para consolidar
                    dfh.insert(0, "city", f"{c['name']}-{c['state']}")
                    dfh.insert(1, "locale_id", c["id"])
                    dfh.insert(2, "latitude", lat)
                    dfh.insert(3, "longitude", lon)
                    dfh.insert(4, "from_date", dia.strftime("%Y-%m-%d"))

                    all_hist.append(dfh)

        if all_hist:
            df_hist_final = pd.concat(all_hist, ignore_index=True)
            st.dataframe(df_hist_final, use_container_width=True)

            xlsx = df_to_xlsx_bytes(df_hist_final, "Historico_Hourly")
            ts = datetime.now().strftime("%Y%m%d_%H%M")
            st.download_button(
                "⬇️ Download Histórico (XLSX)",
                data=xlsx,
                file_name=f"historico_geo_hourly_{ts}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
        else:
            st.warning("Nenhum histórico foi retornado. Verifique datas/lat/lon.")
