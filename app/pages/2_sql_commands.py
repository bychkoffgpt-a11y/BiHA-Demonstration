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
    "Транзакция чтения (Read transaction)": """
SELECT id, payload, created_at
FROM biha_demo_load
ORDER BY id DESC
LIMIT 100;
""",
    "Транзакция чтения-записи (Read-write transaction, 3 commands)": """
BEGIN;
INSERT INTO biha_demo_load(payload)
VALUES ('demo-<timestamp>')
RETURNING id;

SELECT id, payload, created_at
FROM biha_demo_load
WHERE id = <id_from_insert>;

UPDATE biha_demo_load
SET payload = payload || '-updated'
WHERE id = <id_from_insert>;
COMMIT;
""",
}

for title, sql in commands.items():
    st.subheader(title)
    st.code(sql.strip(), language="sql")
