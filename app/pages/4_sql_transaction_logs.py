from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

from ui_styles import apply_base_page_styles

LOG_FILE = Path("logs/biha_demo.log")
LOG_MARKER = "Workload transaction failed"


@st.cache_data(show_spinner=False)
def load_sql_log_lines(log_path: str) -> list[str]:
    path = Path(log_path)
    if not path.exists():
        return []
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    return [line for line in lines if LOG_MARKER in line]


@st.cache_data(show_spinner=False)
def sort_log_lines_desc(lines: tuple[str, ...]) -> list[str]:
    def sort_key(line: str) -> pd.Timestamp:
        ts_part = line.split(" | ", 1)[0].strip()
        parsed = pd.to_datetime(ts_part, errors="coerce")
        if pd.isna(parsed):
            return pd.Timestamp.min
        return parsed

    return sorted(lines, key=sort_key, reverse=True)


st.set_page_config(page_title="SQL логи транзакций", layout="wide")
apply_base_page_styles()
st.title("SQL логи транзакций")
st.caption("Отдельная страница с ошибками SQL-транзакций генератора нагрузки.")

refresh_col, _ = st.columns([1, 5])
with refresh_col:
    if st.button("Обновить", use_container_width=True):
        load_sql_log_lines.clear()
        sort_log_lines_desc.clear()
        st.rerun()

wg = st.session_state.get("workload_generator")

if wg is not None:
    recent_errors = wg.recent_errors_snapshot()
    st.subheader("Ошибки текущего запуска")
    if recent_errors:
        recent_errors_df = pd.DataFrame(recent_errors).sort_values(by="ts", ascending=False)
        st.dataframe(recent_errors_df, width="stretch", height=320, hide_index=True)
    else:
        st.info("В текущем запуске ошибок SQL-транзакций пока нет.")

st.subheader("Последние ошибки из файла логов")
line_limit = st.slider("Количество строк", min_value=20, max_value=500, value=120, step=20)

if LOG_FILE.exists():
    sql_lines = load_sql_log_lines(str(LOG_FILE))
    if sql_lines:
        latest_sql_lines = sort_log_lines_desc(tuple(sql_lines))[:line_limit]
        st.caption(f"Показаны {len(latest_sql_lines)} самых свежих записей, сверху — новые события.")
        st.code("\n".join(latest_sql_lines), language="text")
    else:
        st.info("В файле логов не найдено записей об ошибках SQL-транзакций.")
else:
    st.info(f"Файл логов пока не создан: {LOG_FILE}")
