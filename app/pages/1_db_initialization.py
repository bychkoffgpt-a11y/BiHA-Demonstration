from __future__ import annotations

import threading
import time
import os
from pathlib import Path

import streamlit as st

from cluster_demo import load_cluster_config, select_node_for_workload
from ui_styles import apply_base_page_styles
from workload_profiles import initialize_pg_like_dataset

st.set_page_config(page_title="Инициализация БД", layout="wide")
apply_base_page_styles()
st.title("Инициализация и наполнение БД в стиле pgbench")
st.caption("Создание схемы и генерация данных по целевому размеру БД в ГБ.")

if "db_init_state" not in st.session_state:
    st.session_state.db_init_state = {
        "running": False,
        "progress": 0.0,
        "stage": "Ожидание",
        "eta_sec": None,
        "eta_updated_at": None,
        "started_at": None,
        "error": None,
        "result": None,
    }

state = st.session_state.db_init_state

cfg_path = Path(st.text_input("Путь к конфигурационному файлу", "config/cluster.json"))
target_size_gb = st.number_input("Целевой размер БД, ГБ", min_value=0.1, max_value=500.0, value=1.0, step=0.1)
cpu_count = os.cpu_count() or 1
workers = st.number_input(
    "Количество потоков заполнения",
    min_value=1,
    max_value=cpu_count,
    value=min(4, cpu_count),
    step=1,
    help=f"Не более числа ядер сервера приложения ({cpu_count}).",
)

cluster = None
node = None
config_error: str | None = None

try:
    cluster = load_cluster_config(cfg_path)
except Exception as exc:
    config_error = str(exc)
    st.error(f"Не удалось прочитать конфиг: {exc}")

if cluster is not None:
    node = select_node_for_workload(cluster.nodes, "rw-master", write_tx=True)
    if not node:
        st.warning("В конфиге нет узлов")
    else:
        st.info(f"Целевая нода для инициализации: {node.name}")


def run_init() -> None:
    state["running"] = True
    state["progress"] = 0.0
    state["stage"] = "Запуск"
    state["eta_sec"] = None
    state["eta_updated_at"] = None
    state["started_at"] = time.time()
    state["error"] = None
    state["result"] = None

    def cb(progress: float, stage: str, eta_sec: float | None) -> None:
        state["progress"] = max(0.0, min(1.0, progress))
        state["stage"] = stage
        state["eta_sec"] = eta_sec
        state["eta_updated_at"] = time.time() if eta_sec is not None else None

    try:
        result = initialize_pg_like_dataset(
            node.dsn,
            float(target_size_gb),
            worker_count=int(workers),
            progress_cb=cb,
        )
        state["result"] = result
    except Exception as exc:
        state["error"] = str(exc)
    finally:
        state["running"] = False


can_start = config_error is None and node is not None and not state["running"]
if st.button("Старт создания/наполнения БД", type="primary", disabled=not can_start):
    threading.Thread(target=run_init, daemon=True).start()
    st.rerun()

if state["running"]:
    st.progress(float(state["progress"]), text=f"{state['stage']}")
    eta = state.get("eta_sec")
    eta_updated_at = state.get("eta_updated_at")
    if eta is not None and eta_updated_at is not None:
        countdown_eta = max(0, eta - (time.time() - eta_updated_at))
        st.caption(f"Осталось примерно: {int(countdown_eta)} сек")
    st.caption("Процесс выполняется...")
    time.sleep(1)
    st.rerun()
else:
    st.progress(float(state["progress"]), text=f"{state['stage']}")

if state.get("error"):
    st.error(f"Ошибка: {state['error']}")

if state.get("result"):
    sizing = state["result"]
    st.success("Инициализация завершена")
    st.json(
        {
            "branch_count": sizing.branch_count,
            "teller_count": sizing.teller_count,
            "account_count": sizing.account_count,
            "target_size_gb": sizing.target_size_gb,
        }
    )
