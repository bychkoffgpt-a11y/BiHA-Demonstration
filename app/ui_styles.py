from __future__ import annotations

import streamlit as st

_BASE_PAGE_CSS = """
<style>
    header[data-testid="stHeader"] {
        height: 0 !important;
        min-height: 0 !important;
        background: transparent !important;
        border-bottom: none !important;
    }

    div[data-testid="stToolbar"] {
        display: none !important;
    }

    div[data-testid="stDecoration"] {
        display: none !important;
    }

    div[data-testid="stAppViewContainer"] > .main {
        padding-top: 0 !important;
    }

    div.block-container {
        padding-top: 0.35rem !important;
        padding-bottom: 0.6rem;
        max-width: 100% !important;
    }
</style>
"""


def apply_base_page_styles(extra_css: str = "") -> None:
    css = _BASE_PAGE_CSS
    if extra_css.strip():
        css = f"{css}\n<style>\n{extra_css}\n</style>"
    st.markdown(css, unsafe_allow_html=True)
