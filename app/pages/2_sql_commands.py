import streamlit as st

from workload_profiles import (
    PG_LIKE_FILL_ACCOUNTS_SQL,
    PG_LIKE_FILL_BRANCHES_SQL,
    PG_LIKE_FILL_TELLERS_SQL,
    PG_LIKE_READ_SQL,
    PG_LIKE_SCHEMA_SQL,
    PG_LIKE_TRUNCATE_SQL,
    PG_LIKE_WRITE_SQL,
)

st.set_page_config(page_title="SQL команды нагрузки", layout="wide")
st.title("SQL команды инициализации и нагрузки")
st.caption("Все SQL-скрипты, используемые приложением для подготовки базы и генерации нагрузки.")

commands = {
    "1) Создание схемы pgbench-like": PG_LIKE_SCHEMA_SQL,
    "2) Очистка таблиц": PG_LIKE_TRUNCATE_SQL,
    "3) Наполнение branches": PG_LIKE_FILL_BRANCHES_SQL,
    "4) Наполнение tellers": PG_LIKE_FILL_TELLERS_SQL,
    "5) Наполнение accounts": PG_LIKE_FILL_ACCOUNTS_SQL,
    "6) Транзакция чтения": PG_LIKE_READ_SQL,
    "7) Транзакция чтение-запись": PG_LIKE_WRITE_SQL,
}

for title, sql in commands.items():
    st.subheader(title)
    st.code(sql.strip(), language="sql")
