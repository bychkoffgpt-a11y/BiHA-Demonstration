import streamlit as st

from ui_styles import apply_base_page_styles
from workload_profiles import (
    PG_LIKE_FILL_ACCOUNTS_SQL,
    PG_LIKE_FILL_BRANCHES_SQL,
    PG_LIKE_FILL_TELLERS_SQL,
    PG_LIKE_READ_AUXILIARY_SCRIPT_SQL,
    PG_LIKE_READ_AUXILIARY_SQL,
    PG_LIKE_READ_PRIMARY_SCRIPT_SQL,
    PG_LIKE_READ_PRIMARY_SQL,
    PG_LIKE_READ_SQL,
    PG_LIKE_SCHEMA_SQL,
    PG_LIKE_TRUNCATE_SQL,
    PG_LIKE_WRITE_AUXILIARY_SCRIPT_SQL,
    PG_LIKE_WRITE_AUXILIARY_SQL,
    PG_LIKE_WRITE_SQL,
    PG_LIKE_WRITE_SCRIPT_SQL,
    READ_AUXILIARY_EVERY,
    WRITE_AUXILIARY_EVERY,
)

st.set_page_config(page_title="SQL-команды нагрузки", layout="wide")
apply_base_page_styles()
st.title("SQL команды инициализации и нагрузки")
st.caption("Все SQL-команды, которые приложение использует для подготовки базы и генерации нагрузки.")
st.info(
    "Ниже показаны два представления: сценарии в формате pgbench для документации и эквивалентные SQL-команды, "
    "которые приложение выполняет через psycopg."
)
st.caption(
    f"Профиль чтения разделён на базовую часть (3 из 4 транзакций) и дополнительную часть "
    f"(в каждой {READ_AUXILIARY_EVERY}-й транзакции)."
)
st.caption(
    f"Для снижения числа блокировок нагрузка записи разделена на базовую часть "
    f"(в каждой транзакции) и дополнительную часть (примерно в каждой {WRITE_AUXILIARY_EVERY}-й транзакции)."
)
st.success("Во всех нагрузочных сценариях убран `count(*)`: для чтения оставлены суммы/средние/максимумы, для истории — сумма изменений и время последнего события.")

st.subheader("Профиль чтения — базовый сценарий pgbench")
st.code(PG_LIKE_READ_PRIMARY_SCRIPT_SQL, language="sql")

st.subheader("Профиль чтения — дополнительный сценарий pgbench")
st.code(PG_LIKE_READ_AUXILIARY_SCRIPT_SQL, language="sql")

st.subheader("Профиль чтения и записи — базовый сценарий pgbench")
st.code(PG_LIKE_WRITE_SCRIPT_SQL, language="sql")

st.subheader("Профиль чтения и записи — дополнительный сценарий pgbench")
st.code(PG_LIKE_WRITE_AUXILIARY_SCRIPT_SQL, language="sql")

commands = {
    "1) Создание схемы в стиле pgbench": PG_LIKE_SCHEMA_SQL,
    "2) Очистка таблиц": PG_LIKE_TRUNCATE_SQL,
    "3) Наполнение таблицы branches": PG_LIKE_FILL_BRANCHES_SQL,
    "4) Наполнение таблицы tellers": PG_LIKE_FILL_TELLERS_SQL,
    "5) Наполнение таблицы accounts": PG_LIKE_FILL_ACCOUNTS_SQL,
    "6) Транзакция чтения — полный набор SQL-команд": PG_LIKE_READ_SQL,
    "7) Транзакция чтения — базовая часть": PG_LIKE_READ_PRIMARY_SQL,
    "8) Транзакция чтения — дополнительная часть": PG_LIKE_READ_AUXILIARY_SQL,
    "9) Транзакция записи — базовая часть": PG_LIKE_WRITE_SQL,
    "10) Транзакция записи — дополнительная часть": PG_LIKE_WRITE_AUXILIARY_SQL,
}

for title, sql in commands.items():
    st.subheader(title)
    st.code(sql.strip(), language="sql")
