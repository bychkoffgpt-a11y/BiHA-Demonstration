from __future__ import annotations

import pandas as pd
import streamlit as st

from orchestration.demo_runner import RunStatus, get_demo_runner
from ui_styles import apply_base_page_styles


st.set_page_config(page_title="Orchestration demo runner", layout="wide")
apply_base_page_styles()
st.title("Orchestration / Demo Runner")
st.caption("Выполнение демонстрационных сценариев как цепочки шагов с тайм-аутами и проверкой состояния")

runner = get_demo_runner()
scenarios = runner.list_scenarios()

if not scenarios:
    st.warning("Сценарии не зарегистрированы")
    st.stop()

scenario_options = {f"{scenario.name} ({scenario.id})": scenario for scenario in scenarios}
selected_label = st.selectbox("Сценарий", options=list(scenario_options.keys()))
selected_scenario = scenario_options[selected_label]

st.markdown(f"**Описание:** {selected_scenario.description}")
st.markdown(f"**Критерий успеха:** {selected_scenario.success_criteria}")

if "scenario_run_id" not in st.session_state:
    st.session_state["scenario_run_id"] = None

col_start, col_stop, col_refresh = st.columns([1, 1, 1])
with col_start:
    if st.button("▶️ Запустить сценарий", type="primary", use_container_width=True):
        st.session_state["scenario_run_id"] = runner.start_scenario(selected_scenario.id)
with col_stop:
    if st.button("⏹ Остановить сценарий", use_container_width=True):
        run_id = st.session_state.get("scenario_run_id")
        if run_id:
            runner.stop_scenario(run_id)
with col_refresh:
    st.button("🔄 Обновить", use_container_width=True)

run_id = st.session_state.get("scenario_run_id")
if not run_id:
    st.info("Выберите сценарий и нажмите «Запустить сценарий»")
    st.stop()

run = runner.get_run_status(run_id)

status_color = {
    RunStatus.PENDING: "gray",
    RunStatus.RUNNING: "blue",
    RunStatus.SUCCEEDED: "green",
    RunStatus.FAILED: "red",
    RunStatus.CANCELLED: "orange",
}.get(run.status, "gray")

st.markdown(f"### Run ID: `{run.run_id}`")
st.markdown(f"**Статус:** :{status_color}[{run.status.value}]")

if run.current_step_index >= 0 and run.current_step_index < len(run.step_logs):
    active_step = run.step_logs[run.current_step_index]
    st.markdown(f"**Текущий шаг:** #{active_step.index + 1} `{active_step.action_type}` → `{active_step.target_node}`")
else:
    st.markdown("**Текущий шаг:** не начат")

if run.error_reason:
    st.error(f"Причина ошибки/остановки: {run.error_reason}")

rows = []
for log in run.step_logs:
    rows.append(
        {
            "Шаг": log.index + 1,
            "Action": log.action_type,
            "Target": log.target_node,
            "Ожидаемый результат": log.expected_result,
            "Фактический результат": log.actual_result,
            "Статус шага": log.status,
            "Ошибка": log.error_reason,
        }
    )

st.subheader("Лог шагов")
st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

if run.status == RunStatus.SUCCEEDED:
    st.success("Итоговый verdict: SCENARIO PASSED")
elif run.status == RunStatus.FAILED:
    st.error("Итоговый verdict: SCENARIO FAILED")
elif run.status == RunStatus.CANCELLED:
    st.warning("Итоговый verdict: SCENARIO CANCELLED")
else:
    st.info("Итоговый verdict: SCENARIO IN PROGRESS")
