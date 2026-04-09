import os
import sys
import re
import io
import time
import tempfile
import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Optional, Union, Tuple, Dict

import streamlit as st
import pandas as pd

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError


# ============================================================
# 0) Bootstrap: instalar Chromium do Playwright automaticamente
# ============================================================
# Por que isso é necessário?
# - O Playwright (Python) precisa que os binários do browser sejam instalados via "playwright install ...". [2](https://playwright.dev/python/docs/browsers)
# - No Streamlit Community Cloud, você não roda terminal manualmente; então fazemos runtime install (1x por instância). [3](https://discuss.streamlit.io/t/installing-playwright-on-streamlit-cloud/30979)

@st.cache_resource
def ensure_playwright_chromium():
    """
    Instala o Chromium do Playwright automaticamente (uma vez por instância do app).
    """
    # Sentinel para evitar reinstalar se o cache do Streamlit resetar parcialmente
    sentinel_dir = Path.home() / ".cache" / "mrs_playwright"
    sentinel_dir.mkdir(parents=True, exist_ok=True)
    sentinel_file = sentinel_dir / "chromium_installed.ok"

    if sentinel_file.exists():
        return True

    # "Lock" simples: evita 2 usuários instalarem ao mesmo tempo na mesma instância
    lock_file = sentinel_dir / "install.lock"
    start = time.time()
    while lock_file.exists() and time.time() - start < 120:
        time.sleep(1.0)

    try:
        lock_file.write_text("locked", encoding="utf-8")
    except Exception:
        # se não conseguir criar lock, segue sem lock
        pass

    try:
        cmd = [sys.executable, "-m", "playwright", "install", "chromium"]
        env = os.environ.copy()

        # Dica: você pode fixar o local dos browsers se precisar, mas em geral o cache padrão funciona.
        # env.setdefault("PLAYWRIGHT_BROWSERS_PATH", "0")

        result = subprocess.run(cmd, env=env, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(
                "Falha ao instalar Chromium do Playwright.\n"
                f"STDOUT:\n{result.stdout}\n\nSTDERR:\n{result.stderr}"
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
# 1) Configurações visuais do App
# ============================================================

APP_TITLE = "Painel de Previsões de Flambagens e Fraturas"
APP_SUBTITLE = "Automação SMAC/Climatempo • Exportação de Relatórios (Previsão / Histórico)"

PRIMARY_BLUE = "#063B5C"
ACCENT_YELLOW = "#F6B300"
WHITE = "#FFFFFF"

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

            .stDownloadButton > button {{
                background: #1dd1a1 !important;
                color: #063B5C !important;
                font-weight: 900 !important;
                border-radius: 10px !important;
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
# 2) Automação SMAC/Climatempo (Playwright)
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
    periodicidade: str = "Diário"  # Horário | Diário | Mensal
    habilitar_grafico: bool = True
    habilitar_tabela: bool = True
    habilitar_mapas: bool = False


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

    def goto_section(self, section_name: str):
        p = self.page

        candidates = [
            p.locator("button[aria-label*='menu' i]"),
            p.get_by_role("button", name=re.compile("menu|Menu|☰", re.I)),
        ]
        clicked = False
        for c in candidates:
            if c.count() > 0:
                c.first.click()
                clicked = True
                break

        if not clicked:
            # fallback: canto sup. esquerdo (onde costuma estar o ☰)
            p.mouse.click(28, 90)
            p.wait_for_timeout(300)

        p.get_by_text(re.compile(rf"^{re.escape(section_name)}$", re.I)).first.click()
        p.wait_for_timeout(900)

    def set_top_filters(self, cidade: str, unidade: Optional[str], data_ini: str, data_fim: str):
        p = self.page
        datetime.strptime(data_ini, "%d/%m/%Y")
        datetime.strptime(data_fim, "%d/%m/%Y")

        self._select_combo_by_label("Cidade", cidade)
        if unidade:
            self._select_combo_by_label(unidade, unidade, allow_same_label=True)

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

    def list_available_series(self) -> List[str]:
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

        return sorted({s.strip() for s in series if s and s.strip()})

    def set_series(self, series: Union[str, Iterable[str]] = "ALL"):
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
            if btn_select_all.count() > 0:
                btn_select_all.first.click()
            print(f"[AVISO] Séries não encontradas no DOM: {not_found}. Fallback: ALL.")

    def open_settings_menu(self):
        p = self.page
        trigger = p.locator("#basic-button")
        if trigger.count() > 0:
            trigger.first.click()
        else:
            vp = p.viewport_size
            p.mouse.click((vp["width"] - 60) if vp else 1200, 110)

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


def _safe_filename(name: str) -> str:
    name = re.sub(r"[^\w\-\.]+", "_", name, flags=re.UNICODE).strip("_")
    return name or "export.xlsx"


# ============================================================
# 3) Auxiliares do app (preview excel)
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
# 4) App Streamlit
# ============================================================

def main():
    inject_css()

    # 1) Instala Chromium automaticamente na primeira execução do container
    # (sem isso, Playwright pode falhar por não achar o executável do browser). [2](https://playwright.dev/python/docs/browsers)[3](https://discuss.streamlit.io/t/installing-playwright-on-streamlit-cloud/30979)
    with st.spinner("Preparando ambiente (Playwright/Chromium)..."):
        ensure_playwright_chromium()

    st.markdown("<div class='mrs-header'>", unsafe_allow_html=True)
    build_header()
    st.markdown("</div>", unsafe_allow_html=True)

    if "series_available" not in st.session_state:
        st.session_state.series_available = []
    if "last_file" not in st.session_state:
        st.session_state.last_file = None
    if "last_error" not in st.session_state:
        st.session_state.last_error = None

    left, right = st.columns([2.4, 1.0], gap="large")

    with left:
        st.markdown("<div class='mrs-card'>", unsafe_allow_html=True)
        st.subheader("1) Configurações do Relatório")

        section = st.selectbox(
            "Seção no menu ☰ (3 riscos)",
            options=["Previsão", "Histórico"],
            index=0,
            help="Escolha qual página o robô deve abrir pelo menu ☰."
        )

        st.markdown("**Credenciais (SMAC/Climatempo)**")
        user = st.text_input("Usuário", value="", placeholder="Digite seu usuário")
        password = st.text_input("Senha", value="", type="password", placeholder="Digite sua senha")

        st.divider()

        st.markdown("**Filtros do Relatório**")
        cidade = st.text_input("Cidade", value="Juiz de Fora")
        unidade = st.text_input("Unidade", value="Alumínio")

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
        series_mode = st.radio(
            "Modo de séries",
            options=["Todas (ALL)", "Selecionar manualmente"],
            index=0,
            horizontal=True
        )

        if st.button("🔎 Carregar séries disponíveis", use_container_width=True):
            st.session_state.last_error = None
            if not user or not password:
                st.warning("Informe Usuário e Senha antes de carregar as séries.")
            else:
                with st.spinner("Abrindo SMAC e detectando séries do rodapé..."):
                    try:
                        cfg = SmacConfig(headless=True)
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
                default=[]
            )

        st.markdown("</div>", unsafe_allow_html=True)

    with right:
        st.markdown("<div class='mrs-card'>", unsafe_allow_html=True)
        st.subheader("2) Executar e Baixar")

        headless = st.toggle("Rodar em modo invisível (headless)", value=True)

        run = st.button("🚀 Gerar Relatório e Exportar Excel", use_container_width=True)

        if st.session_state.last_error:
            st.error(st.session_state.last_error)

        if run:
            st.session_state.last_error = None
            st.session_state.last_file = None

            if not user or not password:
                st.warning("Informe Usuário e Senha para executar.")
            else:
                with st.spinner("Executando automação (login → menu → filtros → séries → exportar)…"):
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
                            ex.goto_forecast()
                            ex.goto_section(section)
                            ex.set_top_filters(cidade=cidade, unidade=unidade, data_ini=data_ini, data_fim=data_fim)

                            if series_mode == "Todas (ALL)":
                                ex.set_series("ALL")
                            else:
                                ex.set_series(selected_series if selected_series else "ALL")

                            menu = ex.open_settings_menu()
                            ex.apply_export_options(menu, opts)
                            final_file = ex.export_excel(menu, out_path)

                        st.session_state.last_file = str(final_file)
                        st.success("Relatório exportado com sucesso!")

                    except Exception as e:
                        st.session_state.last_error = str(e)
                        st.error(f"Falha na exportação: {e}")

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

    st.caption("MRS • Automação de relatórios SMAC/Climatempo • Previsões de flambagens e fraturas")


if __name__ == "__main__":
    main()
