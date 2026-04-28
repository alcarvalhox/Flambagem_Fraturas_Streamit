import streamlit as st
import requests
import pandas as pd
from io import BytesIO
from urllib.parse import quote
from datetime import datetime

# =========================
# CONFIGURAÇÃO
# =========================
BASE_URL = "http://apiadvisor.climatempo.com.br/api/v1"
API_MANAGER = "http://apiadvisor.climatempo.com.br/api-manager"

# Tokens (recomendado mover para st.secrets; deixei default para facilitar)
DEFAULT_TOKEN_PREVISAO = "531a8163c4464184b1e8ff89742d531f"
DEFAULT_TOKEN_HISTORICO = "8445618686be6cffc02c0954cbaada35"

# =========================
# FUNÇÕES UTILITÁRIAS HTTP
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
    """
    API Manager normalmente exige application/x-www-form-urlencoded
    (data=...) e parâmetro localeId[].
    """
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
# FUNÇÕES DE NEGÓCIO
# =========================
@st.cache_data(ttl=3600, show_spinner=False)
def buscar_cidades(nome: str, uf: str, token: str):
    # /locale/city?name=...&state=...&token=...
    url = f"{BASE_URL}/locale/city?name={quote(nome)}&state={uf}&token={token}"
    ok, payload, status, err = http_get(url)
    if not ok:
        raise RuntimeError(f"Erro ao buscar cidade. HTTP {status}: {err}")
    return payload or []

def registrar_locale(token: str, locale_id: int):
    """
    Vincula locale ao token (necessário em alguns planos).
    PUT /api-manager/user-token/<TOKEN>/locales com localeId[].
    """
    url = f"{API_MANAGER}/user-token/{token}/locales"
    data = {"localeId[]": str(locale_id)}
    return http_put_form(url, data=data)

def fetch_previsao(token: str, locale_id: int, dias_desejados: int):
    """
    A API expõe endpoints fixos de previsão (ex.: 15 dias e 270 dias).
    Então NÃO chamamos /days/60. Em vez disso:
      - se dias <= 15 => /days/15 e recorta
      - se dias > 15  => tenta /days/270 e recorta
      - se /days/270 não estiver disponível no plano, cai para /days/15
    (Essas rotas constam na doc pública.) [1](https://apiadvisor.climatempo.com.br/doc/index.html)
    """
    if dias_desejados <= 15:
        url = f"{BASE_URL}/forecast/locale/{locale_id}/days/15?token={token}"
        ok, payload, status, err = http_get(url)
        return ok, payload, status, err, 15

    url270 = f"{BASE_URL}/forecast/locale/{locale_id}/days/270?token={token}"
    ok, payload, status, err = http_get(url270)
    if ok:
        return True, payload, status, err, 270

    # fallback
    url15 = f"{BASE_URL}/forecast/locale/{locale_id}/days/15?token={token}"
    ok2, payload2, status2, err2 = http_get(url15)
    return ok2, payload2, status2, (err or err2), 15

def fetch_historico_auto(token: str, locale_id: int, dias: int, template_path: str, extra_candidates: list):
    """
    Histórico: como o endpoint pode variar por contrato, tentamos:
    1) template configurado no sidebar
    2) lista de candidates comuns
    Retorna (ok, payload, status, err, endpoint_usado)
    """
    tried = []

    def make_url(path: str):
        # path deve começar com "/"
        if "?" in path:
            return f"{BASE_URL}{path}&token={token}"
        return f"{BASE_URL}{path}?token={token}"

    # 1) tenta template
    path = template_path.format(id=locale_id, n=dias)
    url = make_url(path)
    ok, payload, status, err = http_get(url)
    tried.append((url, status))
    if ok:
        return True, payload, status, err, path, tried

    # 2) tenta candidates
    for cand in extra_candidates:
        path2 = cand.format(id=locale_id, n=dias)
        url2 = make_url(path2)
        ok2, payload2, status2, err2 = http_get(url2)
        tried.append((url2, status2))
        if ok2:
            return True, payload2, status2, err2, path2, tried

    return False, None, status, err, None, tried

def normalizar_payload_para_df(payload, dias_desejados: int):
    """
    Normaliza retornos no formato:
    - dict com chave "data": lista
    - lista direta
    - dict genérico (json_normalize)
    """
    if payload is None:
        return pd.DataFrame()

    if isinstance(payload, dict) and isinstance(payload.get("data"), list):
        data_list = payload.get("data", [])[:dias_desejados]
        return flatten_lista_data(data_list)

    if isinstance(payload, list):
        return flatten_lista_data(payload[:dias_desejados])

    # fallback genérico
    df = pd.json_normalize(payload)
    return df

def flatten_lista_data(data_list):
    """
    Extrai variáveis meteorológicas típicas (quando existirem) para um formato tabular.
    Funciona bem para forecast (documentado) e para histórico caso a estrutura seja parecida.
    """
    rows = []
    for d in data_list:
        rain = d.get("rain", {}) or {}
        temp = d.get("temperature", {}) or {}
        hum = d.get("humidity", {}) or {}
        press = d.get("pressure", {}) or {}
        wind = d.get("wind", {}) or {}
        uv = d.get("uv", {}) or {}
        sun = d.get("sun", {}) or {}
        clouds = d.get("cloud_coverage", {}) or {}

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
            "cloud_low_pct": clouds.get("low"),
            "cloud_mid_pct": clouds.get("mid"),
            "cloud_high_pct": clouds.get("high"),
            "summary_pt": phrase
        })
    return pd.DataFrame(rows)

def df_to_xlsx_bytes(df: pd.DataFrame, sheet_name: str):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    return output.getvalue()

# =========================
# UI
# =========================
st.set_page_config(page_title="Climatempo • Previsão & Histórico (Excel)", layout="wide")
st.title("🌦️ Climatempo • Previsão & Histórico (Excel)")

with st.sidebar:
    st.header("🔐 Tokens / Ajustes")
    token_prev = st.text_input("Token Previsão", value=st.secrets.get("TOKEN_PREVISAO", DEFAULT_TOKEN_PREVISAO), type="password")
    token_hist = st.text_input("Token Histórico", value=st.secrets.get("TOKEN_HISTORICO", DEFAULT_TOKEN_HISTORICO), type="password")

    st.divider()
    st.subheader("🕒 Histórico — endpoint")
    st.caption("Como o endpoint pode variar por contrato, ajuste o template se necessário.")
    history_template = st.text_input(
        "Template do histórico (path)",
        value=st.secrets.get("HISTORY_TEMPLATE", "/history/locale/{id}/days/{n}"),
        help="Ex.: /history/locale/{id}/days/{n}"
    )

    st.divider()
    show_debug = st.checkbox("Mostrar detalhes de erro (debug)", value=False)

# Sessão
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
        resultados = buscar_cidades(nome_cidade.strip(), uf.strip().upper(), token_prev)
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
        st.session_state["cidades"].append({"id": cidade_sel["id"], "name": cidade_sel.get("name"), "state": cidade_sel.get("state")})
        st.success("Cidade adicionada.")
    else:
        st.info("Cidade já está na lista.")

if st.session_state["cidades"]:
    st.write("**Cidades na lista:**")
    st.dataframe(pd.DataFrame(st.session_state["cidades"]), use_container_width=True)

    r1, r2 = st.columns([2, 1.2])
    with r1:
        remover_id = st.selectbox("Remover cidade (ID)", options=[c["id"] for c in st.session_state["cidades"]])
    with r2:
        if st.button("🗑️ Remover selecionada", use_container_width=True):
            st.session_state["cidades"] = [c for c in st.session_state["cidades"] if c["id"] != remover_id]
            st.success("Removida.")
else:
    st.info("Adicione uma ou mais cidades para gerar relatórios.")

st.divider()
tab_prev, tab_hist = st.tabs(["🔮 Previsão", "🕒 Histórico"])

# =========================
# ABA PREVISÃO
# =========================
with tab_prev:
    st.subheader("🔮 Previsão (até 60 dias ou limite do seu plano)")
    dias_prev = st.slider("Período de previsão (dias)", 1, 60, 15)

    b1, b2 = st.columns([1.3, 1.3])
    gerar_prev = b1.button("⚙️ Gerar Previsão", use_container_width=True)
    vincular_prev = b2.button("🔗 Vincular cidades ao token (previsão)", use_container_width=True)

    st.caption(
        "Se aparecer erro de acesso (Access forbidden), é necessário registrar a cidade no token via API Manager. "
        "O app permite fazer isso com o botão acima. [2](https://www.tempo.com/)[3](https://github.com/adinan-cenci/climatempo-api)"
    )

    if vincular_prev:
        if not st.session_state["cidades"]:
            st.warning("Adicione pelo menos uma cidade.")
        else:
            for c in st.session_state["cidades"]:
                ok, payload, status, err = registrar_locale(token_prev, c["id"])
                if ok:
                    st.success(f"Vinculada ao token de previsão: {c['name']}-{c['state']} (ID {c['id']})")
                else:
                    st.error(f"Falha ao vincular (previsão) {c['name']} (ID {c['id']}). HTTP {status}")
                    if show_debug:
                        st.code(err)

    if gerar_prev:
        if not st.session_state["cidades"]:
            st.warning("Adicione pelo menos uma cidade.")
        else:
            all_dfs = []
            avisos = []
            with st.spinner("Consultando previsões..."):
                for c in st.session_state["cidades"]:
                    ok, payload, status, err, endpoint_dias = fetch_previsao(token_prev, c["id"], dias_prev)

                    # Se falhar por acesso, tenta registrar e repetir 1x
                    if not ok and ("Access forbidden" in (err or "") or status in (400, 403)):
                        reg_ok, _, reg_status, reg_err = registrar_locale(token_prev, c["id"])
                        if reg_ok:
                            ok, payload, status, err, endpoint_dias = fetch_previsao(token_prev, c["id"], dias_prev)
                        else:
                            st.error(f"Não foi possível registrar {c['name']} no token de previsão. HTTP {reg_status}")
                            if show_debug:
                                st.code(reg_err)

                    if not ok:
                        st.error(f"Erro na previsão de {c['name']}-{c['state']} (ID {c['id']}). HTTP {status}")
                        if show_debug:
                            st.code(err)
                        continue

                    total = len((payload or {}).get("data", []))
                    if total < dias_prev:
                        avisos.append(f"{c['name']}-{c['state']}: solicitado {dias_prev}, retornado {total} (endpoint /days/{endpoint_dias}).")

                    df = normalizar_payload_para_df(payload, dias_prev)
                    df.insert(0, "city", f"{c['name']}-{c['state']}")
                    df.insert(1, "locale_id", c["id"])
                    all_dfs.append(df)

            if all_dfs:
                df_final = pd.concat(all_dfs, ignore_index=True)
                st.dataframe(df_final, use_container_width=True)

                if avisos:
                    st.warning("Algumas cidades retornaram menos dias do que o solicitado:")
                    for a in avisos:
                        st.write("- " + a)

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
# ABA HISTÓRICO
# =========================
with tab_hist:
    st.subheader("🕒 Histórico (até 60 dias ou limite do seu plano)")
    dias_hist = st.slider("Período de histórico (dias)", 1, 60, 15)

    cA, cB = st.columns([1.3, 1.3])
    gerar_hist = cA.button("⚙️ Gerar Histórico", use_container_width=True)
    vincular_hist = cB.button("🔗 Vincular cidades ao token (histórico)", use_container_width=True)

    st.caption(
        "Assim como na previsão, o histórico pode exigir que o locale esteja registrado no token. "
        "O registro é via API Manager com localeId[]. [2](https://www.tempo.com/)[3](https://github.com/adinan-cenci/climatempo-api)"
    )

    # candidates comuns (caso o template padrão não funcione)
    history_candidates = [
        "/historico/locale/{id}/days/{n}",
        "/historical/locale/{id}/days/{n}",
        "/history/locale/{id}/day/{n}",
        "/history/locale/{id}/days/{n}",  # repetido para segurança
    ]

    if vincular_hist:
        if not st.session_state["cidades"]:
            st.warning("Adicione pelo menos uma cidade.")
        else:
            for c in st.session_state["cidades"]:
                ok, payload, status, err = registrar_locale(token_hist, c["id"])
                if ok:
                    st.success(f"Vinculada ao token de histórico: {c['name']}-{c['state']} (ID {c['id']})")
                else:
                    st.error(f"Falha ao vincular (histórico) {c['name']} (ID {c['id']}). HTTP {status}")
                    if show_debug:
                        st.code(err)

    if gerar_hist:
        if not st.session_state["cidades"]:
            st.warning("Adicione pelo menos uma cidade.")
        else:
            all_dfs = []
            with st.spinner("Consultando histórico..."):
                for c in st.session_state["cidades"]:
                    ok, payload, status, err, endpoint_used, tried = fetch_historico_auto(
                        token_hist, c["id"], dias_hist, history_template, history_candidates
                    )

                    # Se falhar por acesso, tenta registrar e repetir 1x com o template
                    if not ok and ("Access forbidden" in (err or "") or status in (400, 403)):
                        reg_ok, _, reg_status, reg_err = registrar_locale(token_hist, c["id"])
                        if reg_ok:
                            ok, payload, status, err, endpoint_used, tried = fetch_historico_auto(
                                token_hist, c["id"], dias_hist, history_template, history_candidates
                            )
                        else:
                            st.error(f"Não foi possível registrar {c['name']} no token de histórico. HTTP {reg_status}")
                            if show_debug:
                                st.code(reg_err)

                    if not ok:
                        st.error(f"Erro no histórico de {c['name']}-{c['state']} (ID {c['id']}). HTTP {status}")
                        if show_debug:
                            st.code(err)
                            st.write("Tentativas (URL, status):")
                            for u, s in tried:
                                st.write(f"- {u}  →  {s}")
                        continue

                    df = normalizar_payload_para_df(payload, dias_hist)
                    df.insert(0, "city", f"{c['name']}-{c['state']}")
                    df.insert(1, "locale_id", c["id"])
                    df.insert(2, "endpoint_used", endpoint_used or "")
                    all_dfs.append(df)

            if all_dfs:
                df_final = pd.concat(all_dfs, ignore_index=True)
                st.dataframe(df_final, use_container_width=True)

                xlsx = df_to_xlsx_bytes(df_final, "Historico")
                ts = datetime.now().strftime("%Y%m%d_%H%M")
                st.download_button(
                    "⬇️ Download Histórico (XLSX)",
                    data=xlsx,
                    file_name=f"historico_{ts}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
            else:
                st.info("Nenhum histórico gerado. Ative o debug e ajuste o template do histórico no menu lateral.")
``
