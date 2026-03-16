import streamlit as st

st.set_page_config(page_title="SQL команды нагрузки", layout="wide")
st.title("SQL команды нагрузки (SQL load commands)")
st.caption("Информационная страница со списком SQL, который выполняет генератор нагрузки (Informational page with SQL executed by the load generator).")

commands = {
    "Подготовка таблицы (Table setup)": """
CREATE TABLE IF NOT EXISTS biha_demo_load (
    id bigserial PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT now(),
    payload text NOT NULL
);
""",
    "Чтение (Read transaction)": """
SELECT pg_sleep(0.01), 1;
""",
    "Запись (Write transaction)": """
INSERT INTO biha_demo_load(payload)
VALUES ('demo-<timestamp>');
""",
}

for title, sql in commands.items():
    st.subheader(title)
    st.code(sql.strip(), language="sql")
