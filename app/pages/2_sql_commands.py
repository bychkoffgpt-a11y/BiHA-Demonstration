import streamlit as st

from workload_profiles import (
    PG_LIKE_FILL_ACCOUNTS_SQL,
    PG_LIKE_FILL_BRANCHES_SQL,
    PG_LIKE_FILL_TELLERS_SQL,
    PG_LIKE_READ_SQL,
    PG_LIKE_READ_SCRIPT_SQL,
    PG_LIKE_SCHEMA_SQL,
    PG_LIKE_TRUNCATE_SQL,
    PG_LIKE_WRITE_AUXILIARY_SCRIPT_SQL,
    PG_LIKE_WRITE_AUXILIARY_SQL,
    PG_LIKE_WRITE_SQL,
    PG_LIKE_WRITE_SCRIPT_SQL,
    WRITE_AUXILIARY_EVERY,
)

st.set_page_config(page_title="SQL команды нагрузки", layout="wide")
st.title("SQL команды инициализации и нагрузки")
st.caption("Все SQL-скрипты, используемые приложением для подготовки базы и генерации нагрузки.")
st.info(
    "Ниже показаны два представления: pgbench-скрипты для документации и эквивалентные SQL-команды, "
    "которые приложение выполняет через psycopg."
)
st.caption(
    f"Для снижения числа блокировок write-нагрузка разделена на базовую часть "
    f"(в каждой транзакции) и дополнительную часть (примерно в каждой {WRITE_AUXILIARY_EVERY}-й транзакции)."
)

st.subheader("Профиль чтения — pgbench-скрипт")
st.code(PG_LIKE_READ_SCRIPT_SQL, language="sql")

st.subheader("Профиль чтения+записи — pgbench-скрипт")
st.code(PG_LIKE_WRITE_SCRIPT_SQL, language="sql")

st.subheader("Профиль чтения+записи — дополнительный pgbench-скрипт")
st.code(PG_LIKE_WRITE_AUXILIARY_SCRIPT_SQL, language="sql")

commands = {
    "1) Создание схемы pgbench-like": PG_LIKE_SCHEMA_SQL,
    "2) Очистка таблиц": PG_LIKE_TRUNCATE_SQL,
    "3) Наполнение branches": PG_LIKE_FILL_BRANCHES_SQL,
    "4) Наполнение tellers": PG_LIKE_FILL_TELLERS_SQL,
    "5) Наполнение accounts": PG_LIKE_FILL_ACCOUNTS_SQL,
    "6) Транзакция чтения — эквивалент для приложения": PG_LIKE_READ_SQL,
    "7) Транзакция чтение-запись — базовая часть": PG_LIKE_WRITE_SQL,
    "8) Транзакция чтение-запись — дополнительная часть": PG_LIKE_WRITE_AUXILIARY_SQL,
}

for title, sql in commands.items():
    st.subheader(title)
    st.code(sql.strip(), language="sql")
