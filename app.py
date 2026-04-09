import re
import io
import time
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Optional, Union, Tuple, Dict, Any

import streamlit as st
import pandas as pd

from playwright.sync_api import (
    sync_playwright,
    TimeoutError as PWTimeoutError,
)


# ============================================================
# 0) Configurações e tema do App
# ============================================================

APP_TITLE = "Painel de Previsões de Flambagens e Fraturas"
APP_SUBTITLE = "Automação SMAC/Climatempo • Exportação de Relatórios (Previsão / Histórico)"

PRIMARY_BLUE = "#063B5C"     # Azul escuro (base)
ACCENT_YELLOW = "#F6B300"    # Amarelo (botões)
WHITE = "#FFFFFF"
LIGHT_BG = "#F5F7FA"

st.set_page_config(
    page_title=APP_TITLE,
    page_icon="📈",
    layout="wide",
)

def inject_css():
    st.markdown(
        f"""
        <style>
            /* Fundo geral */
            .stApp {{
                background: linear-gradient(180deg, {PRIMARY_BLUE} 0%, #052F49 55%, #041F30 100%);
                color: {WHITE};
            }}

            /* Remove padding extra superior */
            .block-container {{
                padding-top: 1.2rem;
                padding-bottom: 2rem;
            }}

            /* Cards */
            .mrs-card {{
                background: rgba(255,255,255,0.08);
                border: 1px solid rgba(255,255,255,0.14);
                border-radius: 14px;
                padding: 16px 18px;
                box-shadow: 0 8px 24px rgba(0,0,0,0.25);
            }}

            /* Header superior */
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
                letter-spacing: 0.5px;
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

            /* Botões Streamlit */
            div.stButton > button {{
                background: {ACCENT_YELLOW};
                color: #0B2233;
                border: none;
                border-radius: 10px;
                padding: 0.62rem 1rem;
                font-weight: 800;
                width: 100%;
                transition: transform .06s ease-in-out, filter .15s ease-in-out;
                box-shadow: 0 10px 18px rgba(0,0,0,0.22);
            }}
            div.stButton > button:hover {{
                filter: brightness(1.02);
                transform: translateY(-1px);
            }}

            /* Inputs */
            .stTextInput input, .stSelectbox div, .stDateInput input {{
                border-radius: 10px !important;
            }}

            /* Dataframes */
            .stDataFrame {{
                background: {WHITE};
                border-radius: 12px;
                padding: 8px;
            }}

            /* Download button */
            .stDownloadButton > button {{
                background: #1dd1a1 !important;
                color: #063B5C !important;
                font-weight: 900 !important;
                border-radius: 10px !important;
            }}

            /* Mensagens */
            .stAlert {{
                border-radius: 12px;
            }}
        </style>
        """,
        unsafe_allow_html=True,
    )


# ============================================================
# 1) Automação SMAC/Climatempo (Playwright)
# ============================================================

@dataclass
class SmacConfig:
    base_url: str = "https://smac.climatempo.io"
    login_path: str = "/login"
    forecast_path: str = "/forecast"
    headless: bool = True
    timeout_ms: int = 40_000
    accept_downloads: bool = True
    debug: bool = False


@dataclass
class ExportOptions:
    modelo: Optional[str] = "CT2W"
    periodicidade: str = "Diário"  # Horário | Diário | Mensal
    habilitar_grafico: bool = True
    habilitar_tabela: bool = True
    habilitar_mapas: bool = False


class SmacExporter:
    """
    Motor de automação (Playwright) que:
      - login
      - navega para seção (Previsão ou Histórico) via menu ☰
      - aplica filtros topo
      - seleciona séries do rodapé
      - configura engrenagem (menu #basic-button)
      - exporta Excel
    """

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
            raise RuntimeError("Playwright não iniciado. Use with SmacExporter(...) ou chame start().")
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

    # ---------------- Etapa A: Login ----------------

    def login(self, username: str, password: str):
        p = self.page
        p.goto(f"{self.cfg.base_url}{self.cfg.login_path}", wait_until="domcontentloaded")
        p.get_by_placeholder(re.compile("Usuário|Usuario|user|email|login", re.I)).fill(username)
        p.get_by_placeholder(re.compile("Senha|password", re.I)).fill(password)
        p.get_by_role("button", name=re.compile("Entrar|Login|Sign in", re.I)).click()

        # aguarda sair do login
        try:
            p.wait_for_url(re.compile(r".*/(?!login).*"), timeout=20_000)
        except PWTimeoutError:
            p.wait_for_timeout(2_000)

    # ---------------- Etapa B: Ir para Forecast ----------------

    def goto_forecast(self):
        self.page.goto(f"{self.cfg.base_url}{self.cfg.forecast_path}", wait_until="networkidle")

    # ---------------- Etapa C: Menu ☰ (3 riscos) -> Previsão / Histórico ----------------

    def goto_section(self, section_name: str):
        """
        section_name: "Previsão" ou "Histórico"
        Estratégia:
          1) clica no botão do menu ☰ (com aria-label ou por fallback de posição)
          2) clica no item do menu com o texto (Previsão/Histórico)
        """
        p = self.page

        # Tentativa 1: botão de menu por aria-label / role
        menu_btn_candidates = [
            p.locator("button[aria-label*='menu' i]"),
            p.locator("button[aria-label*='Menu' i]"),
            p.get_by_role("button", name=re.compile("menu|Menu|☰", re.I)),
        ]

        clicked = False
        for cand in menu_btn_candidates:
            if cand.count() > 0:
                cand.first.click()
                clicked = True
                break

        # Fallback: clique no canto superior esquerdo (onde costuma estar o ☰)
        if not clicked:
            vp = p.viewport_size
            if vp:
                p.mouse.click(28, 90)
            else:
                p.mouse.click(28, 90)
            p.wait_for_timeout(300)

        # Agora clicar no item do menu
        # (geralmente aparece um drawer/lista)
        p.get_by_text(re.compile(rf"^{re.escape(section_name)}$", re.I)).first.click()

        # espera a tela atualizar
        p.wait_for_timeout(1_000)

    # ---------------- Etapa D: Filtros topo ----------------

    def set_top_filters(self, cidade: str, unidade: Optional[str], data_ini: str, data_fim: str):
        p = self.page
        datetime.strptime(data_ini, "%d/%m/%Y")
        datetime.strptime(data_fim, "%d/%m/%Y")

        self._select_combo_by_label("Cidade", cidade)

        if unidade:
            self._select_combo_by_label(unidade, unidade, allow_same_label=True)

        # Período (abre datepicker)
        p.get_by_text(re.compile(r"\bPeríodo\b", re.I)).first.click()
        p.wait_for_timeout(250)

        filled = False
        for ph in [re.compile(r"dd\/mm\/aaaa", re.I), re.compile(r"dd\/mm\/yyyy", re.I)]:
            loc = p.get_by_placeholder(ph)
            if loc.count() >= 2:
                loc.nth(0).fill(data_ini)
                loc.nth(1).fill(data_fim)
                filled = True
                break

        if not filled:
            p.keyboard.type(data_ini)
            p.keyboard.press("Tab")
            p.keyboard.type(data_fim)

        for nome in ["Aplicar", "OK", "Confirmar", "Salvar"]:
            btn = p.get_by_role("button", name=re.compile(nome, re.I))
            if btn.count() > 0:
                btn.first.click()
                break
        else:
            p.mouse.click(10, 10)

        p.wait_for_timeout(900)

    def _select_combo_by_label(self, label_text: str, value: str, allow_same_label: bool = False):
        p = self.page

        label = p.get_by_text(re.compile(rf"\b{re.escape(label_text)}\b", re.I)).first
        if label.count() == 0:
            label = p.get_by_placeholder(re.compile(label_text, re.I)).first

        try:
            label.click()
        except Exception:
            label.locator("..").click()

        p.wait_for_timeout(200)

        candidates = [
            p.get_by_role("option", name=re.compile(re.escape(value), re.I)),
            p.get_by_role("menuitem", name=re.compile(re.escape(value), re.I)),
            p.get_by_text(re.compile(rf"^{re.escape(value)}$", re.I)),
            p.get_by_text(re.compile(re.escape(value), re.I)),
        ]

        for c in candidates:
            if c.count() > 0:
                c.first.click()
                p.wait_for_timeout(300)
                return

        if allow_same_label:
            label.click()
            p.wait_for_timeout(200)
            opt = p.get_by_text(re.compile(re.escape(value), re.I))
            if opt.count() > 0:
                opt.first.click()
                p.wait_for_timeout(300)
                return

        raise RuntimeError(f"Não consegui selecionar '{value}' no combo '{label_text}'.")

    # ---------------- Etapa E: Séries do rodapé ----------------

    def list_available_series(self) -> List[str]:
        """
        Lista séries detectadas no rodapé.
        Como você confirmou que fica em negrito ao hover/click, é DOM clicável.
        Usamos heurística: elementos com cursor pointer no terço inferior.
        """
        p = self.page
        p.mouse.wheel(0, 1200)
        p.wait_for_timeout(200)

        blacklist = [
            "Cidade", "Período", "Exportar", "Configurar limiares", "Selecionar Todos", "Desmarcar Todos",
            "Modelos", "Horário", "Diário", "Mensal", "Habilitar Gráfico", "Habilitar Tabela", "Habilitar Mapas"
        ]

        series = p.evaluate(
            """(blacklist) => {
                const vpH = window.innerHeight || 800;
                const minY = vpH * 0.60;
                const out = new Set();

                const isVisible = (el) => {
                    const r = el.getBoundingClientRect();
                    if (!r || r.width < 5 || r.height < 5) return false;
                    const style = window.getComputedStyle(el);
                    if (style.visibility === 'hidden' || style.display === 'none' || style.opacity === '0') return false;
                    if (r.top < minY) return false;
                    return true;
                };

                const bad = (t) => {
                    const tt = (t || '').trim();
                    if (tt.length < 2 || tt.length > 70) return true;
                    return blacklist.some(b => tt.toLowerCase() === b.toLowerCase());
                };

                const candidates = Array.from(document.querySelectorAll('span, label, div, p, li'));
                for (const el of candidates) {
                    if (!isVisible(el)) continue;
                    const t = (el.innerText || el.textContent || '').trim();
                    if (!t || bad(t)) continue;

                    const style = window.getComputedStyle(el);
                    const clickable = (style.cursor === 'pointer') || !!el.onclick || el.getAttribute('role') === 'button';
                    if (!clickable) continue;

                    out.add(t);
                }
                return Array.from(out);
            }""",
            blacklist
        )

        clean = sorted({s.strip() for s in series if s and s.strip()})
        return clean

    def set_series(self, series: Union[str, Iterable[str]] = "ALL"):
        """
        series:
          - "ALL": seleciona todas (via botão)
          - "NONE": desmarca todas
          - lista: desmarca todas e marca as selecionadas
        """
        p = self.page
        p.mouse.wheel(0, 1200)
        p.wait_for_timeout(250)

        btn_select_all = p.get_by_text(re.compile(r"Selecionar\s+Todos", re.I))
        btn_unselect_all = p.get_by_text(re.compile(r"Desmarcar\s+Todos", re.I))

        if isinstance(series, str):
            mode = series.strip().upper()
            series_list = []
        else:
            mode = "LIST"
            series_list = [str(s).strip() for s in series if str(s).strip()]

        if mode == "ALL":
            if btn_select_all.count() > 0:
                btn_select_all.first.click()
                p.wait_for_timeout(250)
            return

        if mode in ("NONE", "EMPTY"):
            if btn_unselect_all.count() > 0:
                btn_unselect_all.first.click()
                p.wait_for_timeout(250)
            return

        # LIST
        if btn_unselect_all.count() > 0:
            btn_unselect_all.first.click()
            p.wait_for_timeout(250)

        not_found = []
        for name in series_list:
            loc = p.get_by_text(re.compile(rf"^{re.escape(name)}$", re.I))
            if loc.count() == 0:
                loc = p.get_by_text(re.compile(rf"\b{re.escape(name)}\b", re.I))
            if loc.count() > 0:
                loc.first.click()
                p.wait_for_timeout(100)
            else:
                not_found.append(name)

        if not_found:
            # fallback para ALL (não quebra pipeline)
            if btn_select_all.count() > 0:
                btn_select_all.first.click()
            print(f"[AVISO] Séries não encontradas no DOM: {not_found}. Fallback: ALL.")

    # ---------------- Etapa F: Engrenagem (menu #basic-button) e opções ----------------

    def open_settings_menu(self):
        p = self.page
        trigger = p.locator("#basic-button")
        if trigger.count() > 0:
            trigger.first.click()
        else:
            # fallback: clique aproximado no botão ao lado de Período
            vp = p.viewport_size
            if vp:
                p.mouse.click(vp["width"] - 60, 110)
            else:
                p.mouse.click(1200, 110)

        menu = p.get_by_role("menu")
        menu.wait_for(state="visible", timeout=10_000)
        return menu

    def apply_export_options(self, menu, opts: ExportOptions):
        p = self.page

        # Modelo (combobox)
        if opts.modelo:
            combo = menu.get_by_role("combobox")
            combo.first.click()

            opt = p.get_by_role("option", name=re.compile(re.escape(opts.modelo), re.I))
            if opt.count() > 0:
                opt.first.click()
            else:
                p.get_by_text(re.compile(rf"^{re.escape(opts.modelo)}$", re.I)).first.click()

        # Periodicidade (label)
        menu.locator("label").filter(has_text=re.compile(opts.periodicidade, re.I)).first.click()

        # Switches (role=switch + aria-checked)
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

    # ---------------- Etapa G: Exportar e salvar Excel ----------------

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
# 2) Funções auxiliares do Streamlit (leitura do Excel / UI)
# ============================================================

def read_excel_preview(xlsx_path: Path, max_rows: int = 200) -> Tuple[Dict[str, pd.DataFrame], List[str]]:
    """
    Lê todas as abas (se possível) e retorna um dict {sheet: df_preview} + nomes das abas.
    """
    xls = pd.ExcelFile(xlsx_path, engine="openpyxl")
    sheets = xls.sheet_names
    data = {}
    for sh in sheets:
        df = pd.read_excel(xlsx_path, sheet_name=sh, engine="openpyxl")
        data[sh] = df.head(max_rows)
    return data, sheets


def build_header():
    col1, col2, col3 = st.columns([1.2, 4.2, 1.2], vertical_alignment="center")
    with col1:
        # Logo MRS
        try:
            st.image("logo.png", width=90)
        except Exception:
            st.caption("logo.png não encontrado")
    with col2:
        st.markdown(f"<div class='mrs-title'>{APP_TITLE}</div>", unsafe_allow_html=True)
        st.markdown(f"<div class='mrs-subtitle'>{APP_SUBTITLE}</div>", unsafe_allow_html=True)
    with col3:
        # Imagem do veículo
        try:
            st.image("flambagem.jpg", width=170)
        except Exception:
            st.caption("flambagem.jpg não encontrado")


# ============================================================
# 3) App Streamlit
# ============================================================

def main():
    inject_css()

    # Header
    st.markdown("<div class='mrs-header'>", unsafe_allow_html=True)
    build_header()
    st.markdown("</div>", unsafe_allow_html=True)

    # Estado
    if "series_available" not in st.session_state:
        st.session_state.series_available = []
    if "last_file" not in st.session_state:
        st.session_state.last_file = None
    if "last_error" not in st.session_state:
        st.session_state.last_error = None

    # Layout principal
    left, right = st.columns([2.4, 1.0], gap="large")

    # ---------------- Painel de Configurações (Esquerda) ----------------
    with left:
        st.markdown("<div class='mrs-card'>", unsafe_allow_html=True)
        st.subheader("1) Configurações do Relatório")

        # Seção (menu ☰: Previsão ou Histórico)
        section = st.selectbox(
            "Seção no menu ☰ (3 riscos)",
            options=["Previsão", "Histórico"],
            index=0,
            help="Escolha qual página o robô deve abrir pelo menu ☰."
        )

        # Credenciais (recomendo usar st.secrets em produção)
        st.markdown("**Credenciais (SMAC/Climatempo)**")
        user = st.text_input("Usuário", value="", placeholder="Digite seu usuário", key="user")
        password = st.text_input("Senha", value="", type="password", placeholder="Digite sua senha", key="pass")

        st.divider()

        # Filtros topo
        st.markdown("**Filtros do Relatório**")
        cidade = st.text_input("Cidade", value="Juiz de Fora", help="Ex.: Juiz de Fora")
        unidade = st.text_input("Unidade", value="Alumínio", help="Ex.: Alumínio (se aplicável)")

        c1, c2 = st.columns(2)
        with c1:
            dt_ini = st.date_input("Data inicial", value=datetime.today())
        with c2:
            dt_fim = st.date_input("Data final", value=datetime.today())

        # Formato DD/MM/AAAA
        data_ini = dt_ini.strftime("%d/%m/%Y")
        data_fim = dt_fim.strftime("%d/%m/%Y")

        st.divider()

        # Opções engrenagem (Export)
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

        # Séries do rodapé
        st.markdown("**Séries do Rodapé (Legenda)**")
        series_mode = st.radio(
            "Modo de séries",
            options=["Todas (ALL)", "Selecionar manualmente"],
            index=0,
            horizontal=True,
            help="ALL usa Selecionar Todos. Manual lista e permite escolher."
        )

        # Botão para carregar séries disponíveis (faz uma automação leve até a tela)
        if st.button("🔎 Carregar séries disponíveis", use_container_width=True):
            st.session_state.last_error = None
            if not user or not password:
                st.warning("Informe Usuário e Senha antes de carregar as séries.")
            else:
                with st.spinner("Abrindo SMAC e detectando séries do rodapé..."):
                    try:
                        cfg = SmacConfig(headless=True, debug=False)
                        with SmacExporter(cfg) as ex:
                            ex.login(user, password)
                            ex.goto_forecast()
                            ex.goto_section(section)
                            ex.set_top_filters(cidade=cidade, unidade=unidade, data_ini=data_ini, data_fim=data_fim)
                            st.session_state.series_available = ex.list_available_series()

                        if st.session_state.series_available:
                            st.success(f"Séries detectadas: {len(st.session_state.series_available)}")
                        else:
                            st.warning("Não foi possível detectar séries automaticamente. Você pode seguir com ALL.")
                    except Exception as e:
                        st.session_state.last_error = str(e)
                        st.error(f"Falha ao carregar séries: {e}")

        selected_series: Union[str, List[str]] = "ALL"
        if series_mode == "Selecionar manualmente":
            if not st.session_state.series_available:
                st.info("Clique em **Carregar séries disponíveis** para preencher a lista.")
            selected_series = st.multiselect(
                "Escolha as séries",
                options=st.session_state.series_available,
                default=[],
                help="Se vazio, o export seguirá com ALL (fallback)."
            )

        st.markdown("</div>", unsafe_allow_html=True)

    # ---------------- Ações e Resultado (Direita) ----------------
    with right:
        st.markdown("<div class='mrs-card'>", unsafe_allow_html=True)
        st.subheader("2) Executar e Baixar")

        headless = st.toggle("Rodar em modo invisível (headless)", value=True)
        debug = st.toggle("Gerar prints de debug", value=False, help="Salva screenshots em caso de falha (útil para suporte).")

        # Botão principal
        run = st.button("🚀 Gerar Relatório e Exportar Excel", use_container_width=True)

        # Status / Erros
        if st.session_state.last_error:
            st.error(st.session_state.last_error)

        # Execução
        if run:
            st.session_state.last_error = None
            st.session_state.last_file = None

            if not user or not password:
                st.warning("Informe Usuário e Senha para executar.")
            else:
                with st.spinner("Executando automação (login → filtros → séries → exportar)…"):
                    try:
                        cfg = SmacConfig(headless=headless, debug=debug)
                        opts = ExportOptions(
                            modelo=modelo.strip() or None,
                            periodicidade=periodicidade,
                            habilitar_grafico=habilitar_grafico,
                            habilitar_tabela=habilitar_tabela,
                            habilitar_mapas=habilitar_mapas
                        )

                        # arquivo temporário
                        tmp_dir = Path(tempfile.gettempdir()) / "mrs_smac_exports"
                        tmp_dir.mkdir(parents=True, exist_ok=True)
                        out_path = tmp_dir / "relatorio.xlsx"

                        with SmacExporter(cfg) as ex:
                            ex.login(user, password)
                            ex.goto_forecast()

                            # Menu ☰ -> Previsão / Histórico
                            ex.goto_section(section)

                            # Filtros
                            ex.set_top_filters(cidade=cidade, unidade=unidade, data_ini=data_ini, data_fim=data_fim)

                            # Séries
                            if series_mode == "Todas (ALL)":
                                ex.set_series("ALL")
                            else:
                                # se vazio, usa ALL como fallback
                                ex.set_series(selected_series if selected_series else "ALL")

                            # Engrenagem e opções
                            menu = ex.open_settings_menu()
                            ex.apply_export_options(menu, opts)

                            # Exportar
                            final_file = ex.export_excel(menu, out_path)

                        st.session_state.last_file = str(final_file)
                        st.success("Relatório exportado com sucesso!")

                    except Exception as e:
                        st.session_state.last_error = str(e)
                        st.error(f"Falha na exportação: {e}")

        # Preview + download
        if st.session_state.last_file:
            xlsx_path = Path(st.session_state.last_file)

            st.markdown("**3) Visualização rápida**")
            try:
                data, sheets = read_excel_preview(xlsx_path, max_rows=150)
                sheet = st.selectbox("Aba do Excel", options=sheets, index=0)
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

    # Rodapé
    st.caption("MRS • Automação de relatórios SMAC/Climatempo • Previsões de flambagens e fraturas")


if __name__ == "__main__":
    main()
