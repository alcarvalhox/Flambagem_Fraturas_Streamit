import os
import sys
import re
import time
import tempfile
import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Optional, Union

import streamlit as st
import pandas as pd
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError


# ============================================================
# 0) Bootstrap: garantir Chromium do Playwright (Streamlit Cloud)
# ============================================================

@st.cache_resource
def ensure_playwright_chromium():
    sentinel_dir = Path.home() / ".cache" / "mrs_playwright"
    sentinel_dir.mkdir(parents=True, exist_ok=True)
    sentinel_file = sentinel_dir / "chromium_installed.ok"

    if sentinel_file.exists():
        return True

    lock_file = sentinel_dir / "install.lock"
    start = time.time()
    while lock_file.exists() and time.time() - start < 120:
        time.sleep(1.0)

    try:
        try:
            lock_file.write_text("locked", encoding="utf-8")
        except Exception:
            pass

        env = os.environ.copy()

        cmd = [sys.executable, "-m", "playwright", "install", "chromium", "--only-shell"]
        result = subprocess.run(cmd, env=env, capture_output=True, text=True)

        if result.returncode != 0:
            cmd2 = [sys.executable, "-m", "playwright", "install", "chromium"]
            result2 = subprocess.run(cmd2, env=env, capture_output=True, text=True)
            if result2.returncode != 0:
                raise RuntimeError(
                    "Falha ao instalar Chromium do Playwright.\n"
                    f"Tentativa headless shell (STDERR):\n{result.stderr}\n\n"
                    f"Tentativa completa (STDERR):\n{result2.stderr}"
                )

        sentinel_file.write_text("ok", encoding="utf-8")
        return True
    finally:
        try:
            if lock_file.exists():
                lock_file.unlink()
        except Exception:
            pass


# ============================================================
# 1) UI / Tema
# ============================================================

APP_TITLE = "Painel de Previsões de Flambagens e Fraturas"
APP_SUBTITLE = "Automação SMAC/Climatempo • Exportação de Relatórios (Previsão / Histórico)"

PRIMARY_BLUE = "#063B5C"
ACCENT_YELLOW = "#F6B300"
WHITE = "#FFFFFF"
DARK_TEXT = "#0B2233"

st.set_page_config(page_title=APP_TITLE, page_icon="📈", layout="wide")


def inject_css():
    st.markdown(
        f"""
        <style>
            .stApp {{
                background: linear-gradient(180deg, {PRIMARY_BLUE} 0%, #052F49 55%, #041F30 100%);
                color: {WHITE};
            }}
            .block-container {{
                padding-top: 1.2rem;
                padding-bottom: 2rem;
            }}
            .mrs-card {{
                background: rgba(255,255,255,0.08);
                border: 1px solid rgba(255,255,255,0.14);
                border-radius: 14px;
                padding: 16px 18px;
                box-shadow: 0 8px 24px rgba(0,0,0,0.25);
            }}
            .mrs-header {{
                display: flex;
                align-items: center;
                justify-content: space-between;
                gap: 18px;
                background: rgba(0,0,0,0.12);
                border: 1px solid rgba(255,255,255,0.12);
                border-radius: 16px;
                padding: 14px 18px;
                margin-bottom: 18px;
            }}
            .mrs-title {{
                font-size: 30px;
                font-weight: 800;
                margin: 0;
                color: {WHITE};
                text-align: center;
            }}
            .mrs-subtitle {{
                font-size: 14px;
                opacity: 0.92;
                margin-top: 6px;
                text-align: center;
            }}

            /* Botões */
            div.stButton > button {{
                background: {ACCENT_YELLOW};
                color: {DARK_TEXT};
                border: none;
                border-radius: 10px;
                padding: 0.62rem 1rem;
                font-weight: 800;
                width: 100%;
            }}

            /* Download */
            .stDownloadButton > button {{
                background: #1dd1a1 !important;
                color: {PRIMARY_BLUE} !important;
                font-weight: 900 !important;
                border-radius: 10px !important;
            }}

            /* Base: tudo branco no card */
            .mrs-card, .mrs-card * {{
                color: #FFFFFF !important;
            }}

            /* Inputs: preto no fundo branco */
            .mrs-card .stTextInput input,
            .mrs-card .stDateInput input {{
                background: #FFFFFF !important;
                color: {DARK_TEXT} !important;
                border-radius: 10px !important;
            }}

            /* Select / MultiSelect: preto no fundo branco */
            .mrs-card div[data-baseweb="select"] div[role="combobox"] {{
                background: #FFFFFF !important;
                color: {DARK_TEXT} !important;
                border-radius: 10px !important;
            }}
            .mrs-card div[data-baseweb="select"] div[role="combobox"] * {{
                color: {DARK_TEXT} !important;
            }}
            .mrs-card div[data-baseweb="select"] div[role="listbox"] * {{
                color: {DARK_TEXT} !important;
            }}

            /* Toggle: força branco (texto) */
            .mrs-card div[data-testid="stToggle"] label,
            .mrs-card div[data-testid="stToggle"] p,
            .mrs-card div[data-testid="stToggle"] span {{
                color: #FFFFFF !important;
            }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def build_header():
    col1, col2, col3 = st.columns([1.2, 4.2, 1.2], vertical_alignment="center")
    with col1:
        try:
            st.image("logo.png", width=90)
        except Exception:
            st.caption("logo.png não encontrado")
    with col2:
        st.markdown(f"<div class='mrs-title'>{APP_TITLE}</div>", unsafe_allow_html=True)
        st.markdown(f"<div class='mrs-subtitle'>{APP_SUBTITLE}</div>", unsafe_allow_html=True)
    with col3:
        try:
            st.image("flambagem.jpg", width=170)
        except Exception:
            st.caption("flambagem.jpg não encontrado")


# ============================================================
# 2) Séries do SMAC (fixas, conforme seu anexo)
# ============================================================

SERIES_SMAC = [
    "Índice de Ångström",
    "Probabilidade",
    "Visibilidade Mínima",
    "Umidade Média",
    "Pressão MSL Média",
    "Visibilidade Média",
    "Umidade Mínima",
    "Pressão MSL Mínima",
    "Visibilidade Máxima",
    "Umidade Máxima",
    "Pressão MSL Máxima",
    "Velocidade do vento",
    "Raios",
    "Temperatura Média",
    "Velocidade mínima do vento",
    "Índice de nível de raios",
    "Temperatura Mínima",
    "Velocidade máxima do vento",
    "Chuva",
    "Temperatura Máxima",
]


# ============================================================
# 3) Automação SMAC/Climatempo
# ============================================================

@dataclass
class SmacConfig:
    base_url: str = "https://smac.climatempo.io"
    login_path: str = "/login"
    forecast_path: str = "/forecast"
    headless: bool = True
    timeout_ms: int = 45_000
    accept_downloads: bool = True


@dataclass
class ExportOptions:
    modelo: Optional[str] = "CT2W"
    periodicidade: str = "Diário"
    habilitar_grafico: bool = True
    habilitar_tabela: bool = True
    habilitar_mapas: bool = False


def _safe_filename(name: str) -> str:
    name = re.sub(r"[^\w\-\.]+", "_", name, flags=re.UNICODE).strip("_")
    return name or "export.xlsx"


class SmacExporter:
    def __init__(self, cfg: Optional[SmacConfig] = None):
        self.cfg = cfg or SmacConfig()
        self._pw = None
        self._browser = None
        self._context = None
        self._page = None

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.stop()

    @property
    def page(self):
        if not self._page:
            raise RuntimeError("Playwright não iniciado.")
        return self._page

    def start(self):
        if self._pw:
            return
        self._pw = sync_playwright().start()
        self._browser = self._pw.chromium.launch(headless=self.cfg.headless)
        self._context = self._browser.new_context(accept_downloads=self.cfg.accept_downloads)
        self._page = self._context.new_page()
        self._page.set_default_timeout(self.cfg.timeout_ms)

    def stop(self):
        try:
            if self._context:
                self._context.close()
            if self._browser:
                self._browser.close()
            if self._pw:
                self._pw.stop()
        finally:
            self._pw = None
            self._browser = None
            self._context = None
            self._page = None

    def login(self, username: str, password: str):
        p = self.page
        p.goto(f"{self.cfg.base_url}{self.cfg.login_path}", wait_until="domcontentloaded")
        p.get_by_placeholder(re.compile("Usuário|Usuario|user|email|login", re.I)).fill(username)
        p.get_by_placeholder(re.compile("Senha|password", re.I)).fill(password)
        p.get_by_role("button", name=re.compile("Entrar|Login|Sign in", re.I)).click()
        try:
            p.wait_for_url(re.compile(r".*/(?!login).*"), timeout=20_000)
        except PWTimeoutError:
            p.wait_for_timeout(2_000)

    def goto_forecast(self):
        self.page.goto(f"{self.cfg.base_url}{self.cfg.forecast_path}", wait_until="networkidle")

    # =========================
    # Menu ☰ (drawer lateral)
    # =========================
    def open_nav_drawer(self):
        p = self.page
        # Clique no canto sup esquerdo (ícone ☰)
        p.mouse.click(18, 70)
        p.get_by_text(re.compile("Acontece agora|Previsão|Histórico", re.I)).first.wait_for(timeout=8000)

    def goto_section(self, section_name: str):
        p = self.page
        self.goto_forecast()
        p.wait_for_timeout(700)
        self.open_nav_drawer()
        p.get_by_text(re.compile(rf"^{re.escape(section_name)}$", re.I)).first.click()
        p.wait_for_timeout(900)

    # =========================
    # Filtros topo conforme prints
    # =========================
    def set_top_filters(self, tipo: str, local: str, data_ini: str, data_fim: str):
        p = self.page

        # Select tipo (Cidade/Pátio/Pontos Monitorados)
        self._select_first_dropdown(choose_text=tipo)

        # Select local (com "Procurar")
        self._select_second_dropdown(choose_text=local)

        # Período (inputs + Buscar)
        self._set_period_and_search(data_ini, data_fim)

    def _select_first_dropdown(self, choose_text: str):
        p = self.page
        # O primeiro dropdown mostra "Cidade" por padrão (print).
        # então clicamos no botão que contém "Cidade" e escolhemos a opção.
        btn = p.get_by_role("button", name=re.compile(r"^Cidade$", re.I))
        if btn.count() == 0:
            btn = p.get_by_text(re.compile(r"^Cidade$", re.I))
        btn.first.click()
        p.wait_for_timeout(200)
        p.get_by_text(re.compile(rf"^{re.escape(choose_text)}$", re.I)).first.click()
        p.wait_for_timeout(300)

    def _select_second_dropdown(self, choose_text: str):
        p = self.page
        # O segundo dropdown mostra "Alumínio" por padrão (print).
        # tentamos clicar no botão com o texto atual, senão clique por coordenada no 2º dropdown.
        btn = p.get_by_role("button", name=re.compile(r"Alumínio", re.I))
        if btn.count() > 0:
            btn.first.click()
        else:
            # fallback coordenada aproximada do 2º select no topo
            p.mouse.click(650, 155)

        p.wait_for_timeout(250)

        search = p.get_by_placeholder(re.compile("Procurar|Pesquisar|Search", re.I))
        if search.count() > 0:
            search.first.fill(choose_text)
            p.wait_for_timeout(250)

        p.get_by_text(re.compile(rf"^{re.escape(choose_text)}$", re.I)).first.click()
        p.wait_for_timeout(300)

    def _set_period_and_search(self, data_ini: str, data_fim: str):
        p = self.page

        # botão Período
        p.get_by_role("button", name=re.compile(r"Per[ií]odo", re.I)).first.click()
        p.wait_for_timeout(250)

        buscar_btn = p.get_by_role("button", name=re.compile("Buscar", re.I)).first
        buscar_btn.wait_for(timeout=8000)

        # inputs dentro do popover
        pop = buscar_btn.locator("xpath=ancestor::div[2]")
        inputs = pop.locator("input")
        if inputs.count() < 2:
            pop = buscar_btn.locator("xpath=ancestor::div[3]")
            inputs = pop.locator("input")

        if inputs.count() >= 2:
            inputs.nth(0).fill(data_ini)
            inputs.nth(1).fill(data_fim)

        buscar_btn.click()
        p.wait_for_timeout(900)

    # =========================
    # SÉRIES - NOVO: aplicar seleção na legenda
    # =========================
    def apply_series_selection(self, mode: str, series: Optional[List[str]] = None):
        """
        mode: "ALL" ou "MANUAL"
        series: lista de séries selecionadas (texto exato)
        """
        p = self.page

        # rola para legenda
        p.mouse.wheel(0, 1500)
        p.wait_for_timeout(300)

        btn_all = p.get_by_text(re.compile(r"Selecionar\s+Todos", re.I))
        btn_none = p.get_by_text(re.compile(r"Desmarcar\s+Todos", re.I))

        if mode == "ALL":
            if btn_all.count() > 0:
                btn_all.first.click()
                p.wait_for_timeout(200)
            return

        # MANUAL
        if btn_none.count() > 0:
            btn_none.first.click()
            p.wait_for_timeout(200)

        series = series or []
        not_found = []

        for s in series:
            # tenta clicar pelo texto da legenda
            loc = p.get_by_text(re.compile(rf"^{re.escape(s)}$", re.I))
            if loc.count() == 0:
                loc = p.get_by_text(re.compile(re.escape(s), re.I))
            try:
                if loc.count() > 0:
                    loc.first.click(timeout=3000)
                    p.wait_for_timeout(80)
                else:
                    not_found.append(s)
            except Exception:
                not_found.append(s)

        # fallback: se algo não encontrado, não quebra o fluxo
        if not_found:
            print(f"[AVISO] Não encontrei algumas séries para clicar: {not_found}")

    # =========================
    # Engrenagem / Export
    # =========================
    def open_settings_menu(self):
        p = self.page
        trigger = p.locator("#basic-button")
        if trigger.count() > 0:
            trigger.first.click()
        else:
            # fallback clique no ícone ao lado do Período
            p.mouse.click(950, 155)

        menu = p.get_by_role("menu")
        menu.wait_for(state="visible", timeout=10_000)
        return menu

    def apply_export_options(self, menu, opts: ExportOptions):
        p = self.page

        if opts.modelo:
            combo = menu.get_by_role("combobox")
            combo.first.click()
            opt = p.get_by_role("option", name=re.compile(re.escape(opts.modelo), re.I))
            if opt.count() > 0:
                opt.first.click()
            else:
                p.get_by_text(re.compile(rf"^{re.escape(opts.modelo)}$", re.I)).first.click()

        menu.locator("label").filter(has_text=re.compile(opts.periodicidade, re.I)).first.click()
        self._set_switch(menu, "Habilitar Gráfico", opts.habilitar_grafico)
        self._set_switch(menu, "Habilitar Tabela", opts.habilitar_tabela)
        self._set_switch(menu, "Habilitar Mapas", opts.habilitar_mapas)

    def _set_switch(self, menu, label_text: str, desired: bool):
        item = menu.locator("li[role='menuitem']").filter(has_text=re.compile(label_text, re.I)).first
        sw = item.get_by_role("switch")
        sw.wait_for(state="visible", timeout=5_000)
        current = sw.get_attribute("aria-checked")
        is_on = (current == "true")
        if is_on != desired:
            sw.click()
            self.page.wait_for_timeout(120)

    def export_excel(self, menu, out_path: Union[str, Path]) -> Path:
        p = self.page
        out_path = Path(out_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)

        with p.expect_download(timeout=60_000) as dl:
            menu.get_by_role("button", name=re.compile("Exportar", re.I)).first.click()

        download = dl.value
        suggested = download.suggested_filename or out_path.name
        final_name = _safe_filename(suggested)
        final_path = out_path.parent / final_name
        download.save_as(str(final_path))
        return final_path


# ============================================================
# 4) Preview Excel
# ============================================================

def read_excel_preview(xlsx_path: Path, max_rows: int = 200):
    xls = pd.ExcelFile(xlsx_path, engine="openpyxl")
    sheets = xls.sheet_names
    data = {}
    for sh in sheets:
        df = pd.read_excel(xlsx_path, sheet_name=sh, engine="openpyxl")
        data[sh] = df.head(max_rows)
    return data, sheets


# ============================================================
# 5) App Streamlit
# ============================================================

def main():
    inject_css()
    with st.spinner("Preparando ambiente (Playwright/Chromium)..."):
        ensure_playwright_chromium()

    st.markdown("<div class='mrs-header'>", unsafe_allow_html=True)
    build_header()
    st.markdown("</div>", unsafe_allow_html=True)

    if "last_file" not in st.session_state:
        st.session_state.last_file = None
    if "last_error" not in st.session_state:
        st.session_state.last_error = None

    left, right = st.columns([2.4, 1.0], gap="large")

    with left:
        st.markdown("<div class='mrs-card'>", unsafe_allow_html=True)
        st.subheader("1) Configurações do Relatório")

        section = st.selectbox("Seção (menu ☰)", options=["Previsão", "Histórico"], index=0)

        st.markdown("**Credenciais (SMAC/Climatempo)**")
        user = st.secrets.get("SMAC_USER", "mrs")
        password = st.secrets.get("SMAC_PASS", "")

        with st.expander("🔐 Ajustar credenciais (opcional)", expanded=False):
            user = st.text_input("Usuário", value=user)
            password = st.text_input("Senha", value=password, type="password")

        st.divider()

        st.markdown("**Filtros do topo (SMAC)**")
        tipo = st.selectbox("Tipo", ["Cidade", "Pátio", "Pontos Monitorados"], index=0)
        local = st.text_input("Local", value="Alumínio")

        c1, c2 = st.columns(2)
        with c1:
            dt_ini = st.date_input("Data inicial", value=datetime.today())
        with c2:
            dt_fim = st.date_input("Data final", value=datetime.today())
        data_ini = dt_ini.strftime("%d/%m/%Y")
        data_fim = dt_fim.strftime("%d/%m/%Y")

        st.divider()

        st.markdown("**Opções de Exportação (engrenagem)**")
        modelo = st.text_input("Modelo", value="CT2W")
        periodicidade = st.selectbox("Periodicidade", ["Horário", "Diário", "Mensal"], index=1)

        g1, g2, g3 = st.columns(3)
        with g1:
            habilitar_grafico = st.toggle("Habilitar Gráfico", value=True)
        with g2:
            habilitar_tabela = st.toggle("Habilitar Tabela", value=True)
        with g3:
            habilitar_mapas = st.toggle("Habilitar Mapas", value=False)

        st.divider()

        st.markdown("**Séries do Rodapé (Legenda)**")
        series_mode = st.radio("Modo de séries", ["Todas (ALL)", "Selecionar manualmente"], index=0, horizontal=True)

        selected_series: List[str] = []
        if series_mode == "Selecionar manualmente":
            selected_series = st.multiselect(
                "Escolha as séries (SMAC)",
                options=SERIES_SMAC,
                default=["Chuva", "Temperatura Média"] if "Chuva" in SERIES_SMAC else []
            )

        st.markdown("</div>", unsafe_allow_html=True)

    with right:
        st.markdown("<div class='mrs-card'>", unsafe_allow_html=True)
        st.subheader("2) Executar e Baixar (última etapa)")

        filtros_ok = bool(local.strip()) and (dt_fim >= dt_ini)
        creds_ok = bool(user) and bool(password)
        ready_to_run = filtros_ok and creds_ok

        if not creds_ok:
            st.warning("Credenciais ausentes. Configure em Secrets (recomendado).")
        if not filtros_ok:
            st.warning("Revise os filtros (Local/Período).")

        with st.form("run_form", clear_on_submit=False):
            headless = st.toggle("Rodar em modo invisível (headless)", value=True)
            submitted = st.form_submit_button("🚀 Gerar Relatório e Exportar Excel", disabled=not ready_to_run, use_container_width=True)

        if submitted:
            st.session_state.last_error = None
            st.session_state.last_file = None

            with st.spinner("Executando automação (login → filtros → séries → exportar)…"):
                try:
                    cfg = SmacConfig(headless=headless)
                    opts = ExportOptions(
                        modelo=modelo.strip() or None,
                        periodicidade=periodicidade,
                        habilitar_grafico=habilitar_grafico,
                        habilitar_tabela=habilitar_tabela,
                        habilitar_mapas=habilitar_mapas
                    )

                    tmp_dir = Path(tempfile.gettempdir()) / "mrs_smac_exports"
                    tmp_dir.mkdir(parents=True, exist_ok=True)
                    out_path = tmp_dir / "relatorio.xlsx"

                    with SmacExporter(cfg) as ex:
                        ex.login(user, password)
                        ex.goto_section(section)
                        ex.set_top_filters(tipo=tipo, local=local, data_ini=data_ini, data_fim=data_fim)

                        if series_mode == "Todas (ALL)":
                            ex.apply_series_selection("ALL")
                        else:
                            ex.apply_series_selection("MANUAL", selected_series)

                        menu = ex.open_settings_menu()
                        ex.apply_export_options(menu, opts)
                        final_file = ex.export_excel(menu, out_path)

                    st.session_state.last_file = str(final_file)
                    st.success("Relatório exportado com sucesso!")

                except Exception as e:
                    st.session_state.last_error = str(e)
                    st.error(f"Falha na exportação: {e}")

        if st.session_state.last_error:
            st.error(st.session_state.last_error)

        if st.session_state.last_file:
            xlsx_path = Path(st.session_state.last_file)

            st.markdown("**3) Visualização rápida**")
            try:
                data, sheets = read_excel_preview(xlsx_path, max_rows=150)
                sheet = st.selectbox("Aba do Excel", options=sheets, index=0, key="sheet_select")
                st.dataframe(data[sheet], use_container_width=True, height=320)
            except Exception as e:
                st.warning(f"Não foi possível pré-visualizar: {e}")

            st.markdown("**4) Download do Excel**")
            file_bytes = xlsx_path.read_bytes()
            st.download_button(
                label="⬇️ Baixar Excel",
                data=file_bytes,
                file_name=xlsx_path.name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )

        st.markdown("</div>", unsafe_allow_html=True)

    st.caption("MRS • Automação de relatórios SMAC/Climatempo • Previsões de flambagens e fraturas")


if __name__ == "__main__":
    main()
