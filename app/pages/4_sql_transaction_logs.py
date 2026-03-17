from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

LOG_FILE = Path("logs/biha_demo.log")
LOG_MARKER = "Workload transaction failed"

st.set_page_config(page_title="SQL логи транзакций", layout="wide")
st.title("SQL логи транзакций")
st.caption("Отдельная страница с ошибками SQL-транзакций генератора нагрузки.")

wg = st.session_state.get("workload_generator")

if wg is not None:
    recent_errors = wg.recent_errors_snapshot()
    st.subheader("Ошибки текущего запуска")
    if recent_errors:
        st.dataframe(pd.DataFrame(recent_errors), width="stretch", height=320, hide_index=True)
    else:
        st.info("В текущем запуске ошибок SQL-транзакций пока нет.")
else:
    st.warning("Генератор нагрузки ещё не инициализирован. Сначала откройте страницу Cluster Demo.")

st.subheader("Последние ошибки из файла логов")
line_limit = st.slider("Количество строк", min_value=20, max_value=500, value=120, step=20)

if LOG_FILE.exists():
    lines = LOG_FILE.read_text(encoding="utf-8", errors="replace").splitlines()
    sql_lines = [line for line in lines if LOG_MARKER in line]
    if sql_lines:
        st.code("\n".join(sql_lines[-line_limit:]), language="text")
    else:
        st.info("В файле логов не найдено записей об ошибках SQL-транзакций.")
else:
    st.info(f"Файл логов пока не создан: {LOG_FILE}")
