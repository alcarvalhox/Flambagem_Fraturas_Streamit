import streamlit as st
import requests
import pandas as pd
from io import BytesIO
from urllib.parse import quote
from datetime import datetime

# =========================
# CONFIGURAÇÕES
# =========================
BASE_URL = "http://apiadvisor.climatempo.com.br/api/v1"

# Tokens fornecidos pelo usuário (recomendado mover para st.secrets)
DEFAULT_TOKEN_PREVISAO = "531a8163c4464184b1e8ff89742d531f"
DEFAULT_TOKEN_HISTORICO = "8445618686be6cffc02c0954cbaada35"

st.set_page_config(page_title="Climatempo • Previsão & Histórico", layout="wide")

# =========================
# UTILITÁRIOS DE API
# =========================
def api_get_json(url: str, timeout: int = 30):
    """
    Faz GET e retorna (ok, payload, status_code, text_ou_erro)
    - ok=True => payload é dict/list (json)
    - ok=False => payload é None; text_ou_erro tem detalhes
    """
    try:
        r = requests.get(url, timeout=timeout)
        if r.status_code >= 400:
            return False, None, r.status_code, r.text
        # pode vir vazio em alguns endpoints
        if not r.text:
            return True, {}, r.status_code, ""
        return True, r.json(), r.status_code, ""
    except Exception as e:
        return False, None, -1, str(e)

def api_put_form(url: str, data: dict, timeout: int = 30):
    """
    PUT (form urlencoded). Usado para vincular locale ao token em alguns planos.
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
# FUNÇÕES DE DOMÍNIO
# =========================
@st.cache_data(ttl=3600, show_spinner=False)
def buscar_cidades(nome: str, uf: str, token: str):
    """
    Retorna lista de possíveis cidades para (nome, uf).
    Endpoint /locale/city?name=...&state=...&token=...
    """
    url = f"{BASE_URL}/locale/city?name={quote(nome)}&state={uf}&token={token}"
    ok, payload, status, err = api_get_json(url)
    if not ok:
        raise RuntimeError(f"Erro ao buscar cidade. HTTP {status}: {err}")
    if not payload:
        return []
    return payload  # lista

def vincular_locale_ao_token(locale_id: int, token: str):
    """
    PUT api-manager/user-token/<TOKEN>/locales com form localeId[]=<id>
    (necessário em alguns projetos/planos).
    """
    url = f"http://apiadvisor.climatempo.com.br/api-manager/user-token/{token}/locales"
    data = {"localeId[]": str(locale_id)}
    return api_put_form(url, data=data)

def fetch_previsao(locale_id: int, dias_desejados: int, token: str):
    """
    A documentação pública expõe rotas fixas como /days/15 e /days/270.
    Estratégia:
      - se dias <= 15: chama /days/15 e recorta
      - se dias > 15: tenta /days/270 e recorta
      - se /days/270 falhar (plano/token), cai pra /days/15
    """
    if dias_desejados <= 15:
        url = f"{BASE_URL}/forecast/locale/{locale_id}/days/15?token={token}"
        ok, payload, status, err = api_get_json(url)
        return ok, payload, status, err, 15

    # tenta 270 para permitir recorte até 60 (ou mais, se seu plano permitir)
    url270 = f"{BASE_URL}/forecast/locale/{locale_id}/days/270?token={token}"
    ok, payload, status, err = api_get_json(url270)
    if ok:
        return True, payload, status, err, 270

    # fallback 15
    url15 = f"{BASE_URL}/forecast/locale/{locale_id}/days/15?token={token}"
    ok2, payload2, status2, err2 = api_get_json(url15)
    return ok2, payload2, status2, (err or err2), 15

def fetch_historico(locale_id: int, dias_desejados: int, token: str, template: str):
    """
    Como o endpoint de histórico pode variar por contrato/plano,
    o app permite configurar um template.
    Ex: /history/locale/{id}/days/{n}
    """
    path = template.format(id=locale_id, n=dias_desejados)
    url = f"{BASE_URL}{path}?token={token}" if "?" not in path else f"{BASE_URL}{path}&token={token}"
    return api_get_json(url)

def normalizar_para_df(payload: dict, dias_desejados: int):
    """
    Converte retorno (que normalmente tem chave 'data': [...]) para DataFrame.
    Funciona bem para forecast 15/270 (estrutura típica).
    Para histórico, se vier diferente, ao menos mostra colunas possíveis.
    """
    rows = []
    data_list = payload.get("data", [])
    data_list = data_list[:dias_desejados]  # recorte no app (60 ou menos)

    for d in data_list:
        rain = d.get("rain", {}) or {}
        temp = d.get("temperature", {}) or {}
        hum = d.get("humidity", {}) or {}
        press = d.get("pressure", {}) or {}
        wind = d.get("wind", {}) or {}
        uv = d.get("uv", {}) or {}
        clouds = d.get("cloud_coverage", {}) or {}
        sun = d.get("sun", {}) or {}

        # texto, quando existir
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
            "cloud_low_pct": clouds.get("low"),
            "cloud_mid_pct": clouds.get("mid"),
            "cloud_high_pct": clouds.get("high"),
            "sunrise": sun.get("sunrise"),
            "sunset": sun.get("sunset"),
            "summary_pt": phrase
        })

    df = pd.DataFrame(rows)
    return df

def df_to_xlsx_bytes(df: pd.DataFrame, sheet_name: str = "Dados"):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    return output.getvalue()

# =========================
# UI
# =========================
st.title("🌦️ Climatempo • Previsão & Histórico (Excel)")

with st.sidebar:
    st.header("🔐 Tokens / Ajustes")
    token_prev = st.text_input("Token Previsão", value=st.secrets.get("TOKEN_PREVISAO", DEFAULT_TOKEN_PREVISAO), type="password")
    token_hist = st.text_input("Token Histórico", value=st.secrets.get("TOKEN_HISTORICO", DEFAULT_TOKEN_HISTORICO), type="password")

    st.divider()
    st.subheader("🕒 Endpoint de Histórico")
    st.caption("Se o seu contrato usa outro caminho, ajuste aqui.")
    history_template = st.text_input(
        "Template do histórico (path)",
        value=st.secrets.get("HISTORY_TEMPLATE", "/history/locale/{id}/days/{n}"),
        help="Ex.: /history/locale/{id}/days/{n}"
    )

    st.divider()
    st.caption("Dica: se estiver dando erro 400/403, pode ser necessário vincular a cidade ao token.")
    st.session_state.setdefault("show_debug", False)
    st.session_state["show_debug"] = st.checkbox("Mostrar detalhes de erro (debug)", value=st.session_state["show_debug"])

st.subheader("📍 Cidades")

colA, colB, colC, colD = st.columns([3, 1, 2, 2])
with colA:
    nome_cidade = st.text_input("Cidade", value="Juiz de Fora")
with colB:
    uf = st.text_input("UF", value="MG")
with colC:
    btn_buscar = st.button("🔎 Buscar")
with colD:
    btn_add = st.button("➕ Adicionar à lista")

# Lista de cidades na sessão
st.session_state.setdefault("cidades", [])  # lista de dicts: {id,name,state}
st.session_state.setdefault("ultima_busca", [])

if btn_buscar:
    try:
        resultados = buscar_cidades(nome_cidade.strip(), uf.strip().upper(), token_prev)
        st.session_state["ultima_busca"] = resultados
        if resultados:
            st.success(f"{len(resultados)} resultado(s) encontrado(s). Selecione abaixo e clique em 'Adicionar'.")
        else:
            st.warning("Nenhuma cidade encontrada para esse nome/UF.")
    except Exception as e:
        st.error(str(e))

# seleção do resultado da busca
if st.session_state["ultima_busca"]:
    opcoes = {
        f"{c.get('name')} - {c.get('state')} (ID {c.get('id')})": c
        for c in st.session_state["ultima_busca"]
    }
    escolha = st.selectbox("Resultados", list(opcoes.keys()))
    cidade_sel = opcoes[escolha]
else:
    cidade_sel = None

if btn_add and cidade_sel:
    ja = any(x["id"] == cidade_sel["id"] for x in st.session_state["cidades"])
    if not ja:
        st.session_state["cidades"].append({
            "id": cidade_sel["id"],
            "name": cidade_sel.get("name"),
            "state": cidade_sel.get("state")
        })
        st.success("Cidade adicionada.")
    else:
        st.info("Essa cidade já está na lista.")

# mostrar cidades adicionadas
if st.session_state["cidades"]:
    st.write("**Cidades na lista:**")
    df_cidades = pd.DataFrame(st.session_state["cidades"])
    st.dataframe(df_cidades, use_container_width=True)

    colX, colY = st.columns([2, 3])
    with colX:
        remover_id = st.selectbox("Remover cidade (ID)", options=[c["id"] for c in st.session_state["cidades"]])
    with colY:
        if st.button("🗑️ Remover selecionada"):
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

    col1, col2, col3 = st.columns([1.2, 1.2, 3])
    with col1:
        gerar_prev = st.button("⚙️ Gerar Previsão", use_container_width=True)
    with col2:
        vincular = st.button("🔗 Vincular cidade ao token (opcional)", use_container_width=True)
    with col3:
        st.caption("Se a previsão falhar com 403/400, tente vincular. Alguns planos exigem isso.")

    if vincular:
        if not st.session_state["cidades"]:
            st.warning("Adicione pelo menos uma cidade na lista para vincular.")
        else:
            for c in st.session_state["cidades"]:
                ok, payload, status, err = vincular_locale_ao_token(c["id"], token_prev)
                if ok:
                    st.success(f"Vinculada: {c['name']}-{c['state']} (ID {c['id']})")
                else:
                    st.error(f"Falha ao vincular {c['name']} (ID {c['id']}). HTTP {status}")
                    if st.session_state["show_debug"]:
                        st.code(err)

    if gerar_prev:
        if not st.session_state["cidades"]:
            st.warning("Adicione pelo menos uma cidade na lista.")
        else:
            all_rows = []
            avisos = []
            with st.spinner("Consultando previsões..."):
                for c in st.session_state["cidades"]:
                    ok, payload, status, err, endpoint_dias = fetch_previsao(c["id"], dias_prev, token_prev)
                    if not ok:
                        st.error(f"Erro na previsão de {c['name']}-{c['state']} (ID {c['id']}). HTTP {status}")
                        if st.session_state["show_debug"]:
                            st.code(err)
                        continue

                    total_retornado = len((payload or {}).get("data", []))
                    if total_retornado < dias_prev:
                        avisos.append(f"{c['name']}-{c['state']}: solicitado {dias_prev}, retornado {total_retornado} (endpoint /days/{endpoint_dias}).")

                    df = normalizar_para_df(payload, dias_prev)
                    df.insert(0, "city", f"{c['name']}-{c['state']}")
                    df.insert(1, "locale_id", c["id"])
                    all_rows.append(df)

            if all_rows:
                df_prev_final = pd.concat(all_rows, ignore_index=True)
                st.dataframe(df_prev_final, use_container_width=True)

                if avisos:
                    st.warning("Algumas cidades retornaram menos dias do que o solicitado:")
                    for a in avisos:
                        st.write("- " + a)

                xlsx = df_to_xlsx_bytes(df_prev_final, sheet_name="Previsao")
                ts = datetime.now().strftime("%Y%m%d_%H%M")
                st.download_button(
                    "⬇️ Download Previsão (XLSX)",
                    data=xlsx,
                    file_name=f"previsao_{ts}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
            else:
                st.info("Nenhuma previsão gerada (todas falharam). Veja mensagens acima.")

# =========================
# ABA HISTÓRICO
# =========================
with tab_hist:
    st.subheader("🕒 Histórico (até 60 dias ou limite do seu plano)")

    dias_hist = st.slider("Período de histórico (dias)", 1, 60, 15)

    gerar_hist = st.button("⚙️ Gerar Histórico", use_container_width=True)

    st.caption("Se der erro, altere o template do endpoint no menu lateral e veja o debug.")

    if gerar_hist:
        if not st.session_state["cidades"]:
            st.warning("Adicione pelo menos uma cidade na lista.")
        else:
            all_rows = []
            with st.spinner("Consultando histórico..."):
                for c in st.session_state["cidades"]:
                    ok, payload, status, err = fetch_historico(c["id"], dias_hist, token_hist, history_template)
                    if not ok:
                        st.error(f"Erro no histórico de {c['name']}-{c['state']} (ID {c['id']}). HTTP {status}")
                        if st.session_state["show_debug"]:
                            st.code(err)
                        continue

                    df = normalizar_para_df(payload, dias_hist)
                    df.insert(0, "city", f"{c['name']}-{c['state']}")
                    df.insert(1, "locale_id", c["id"])
                    all_rows.append(df)

            if all_rows:
                df_hist_final = pd.concat(all_rows, ignore_index=True)
                st.dataframe(df_hist_final, use_container_width=True)

                xlsx = df_to_xlsx_bytes(df_hist_final, sheet_name="Historico")
                ts = datetime.now().strftime("%Y%m%d_%H%M")
                st.download_button(
                    "⬇️ Download Histórico (XLSX)",
                    data=xlsx,
                    file_name=f"historico_{ts}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
            else:
                st.info("Nenhum histórico gerado (todas falharam). Veja mensagens acima.")
