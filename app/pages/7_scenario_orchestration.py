from __future__ import annotations

import json
import re

import pandas as pd
import streamlit as st

from orchestration.demo_runner import RunStatus, get_demo_runner, get_scenario_catalog_status
from orchestration.reporting import build_and_save_report_bundle, is_run_finished
from orchestration.planned_switchover_ui import (
    PLANNED_SWITCHOVER_SCENARIO_ID,
    build_params_override_for_planned_switchover,
)
from ui_styles import apply_base_page_styles


st.set_page_config(page_title="Orchestration demo runner", layout="wide")
apply_base_page_styles()
st.title("Orchestration / Demo Runner")
st.caption("Выполнение демонстрационных сценариев как цепочки шагов с тайм-аутами и проверкой состояния")

runner = get_demo_runner()
catalog_status = get_scenario_catalog_status()

if catalog_status.error:
    st.error(f"Ошибка загрузки сценариев: {catalog_status.error}")
elif catalog_status.fallback_used:
    st.warning("Каталог config/demo_scenarios пуст. Используется встроенный fallback-сценарий.")

scenarios = runner.list_scenarios()

if not scenarios:
    st.warning("Сценарии не зарегистрированы")
    st.stop()

scenario_options = {f"{scenario.name} ({scenario.id})": scenario for scenario in scenarios}
selected_label = st.selectbox("Сценарий", options=list(scenario_options.keys()))
selected_scenario = scenario_options[selected_label]

st.markdown(f"**Описание:** {selected_scenario.description}")
st.markdown(f"**Критерий успеха:** {selected_scenario.success_criteria}")



params_override: dict[str, str] = {}
planned_switchover_result = None
if selected_scenario.id == PLANNED_SWITCHOVER_SCENARIO_ID:
    selected_target_master = st.session_state.get("scenario_orchestration_target_master")
    planned_switchover_result = build_params_override_for_planned_switchover(
        selected_scenario,
        selected_target_master=selected_target_master,
    )

    if planned_switchover_result.fetch_error:
        st.error(f"Не удалось получить список standby-узлов: {planned_switchover_result.fetch_error}")
    if planned_switchover_result.warning_message:
        st.warning(planned_switchover_result.warning_message)
    if planned_switchover_result.validation_error and not planned_switchover_result.available_slaves:
        st.error(planned_switchover_result.validation_error)

    available_slaves = planned_switchover_result.available_slaves
    if available_slaves:
        if st.session_state.get("scenario_orchestration_target_master") not in available_slaves:
            st.session_state["scenario_orchestration_target_master"] = available_slaves[0]
        selected_target_master = st.selectbox(
            "Целевой standby для planned_switchover",
            options=available_slaves,
            key="scenario_orchestration_target_master",
            help="Выберите актуальный standby-узел на момент старта сценария.",
        )
        planned_switchover_result = build_params_override_for_planned_switchover(
            selected_scenario,
            selected_target_master=selected_target_master,
        )

    params_override = planned_switchover_result.params_override

if "scenario_run_id" not in st.session_state:
    st.session_state["scenario_run_id"] = None

if "scenario_report_artifacts" not in st.session_state:
    st.session_state["scenario_report_artifacts"] = {}


col_start, col_stop, col_refresh = st.columns([1, 1, 1])
with col_start:
    if st.button("▶️ Запустить сценарий", type="primary", width="stretch"):
        if planned_switchover_result and planned_switchover_result.validation_error:
            st.error(planned_switchover_result.validation_error)
        else:
            st.session_state["scenario_run_id"] = runner.start_scenario(
                selected_scenario.id,
                params_override=params_override or None,
            )
with col_stop:
    if st.button("⏹ Остановить сценарий", width="stretch"):
        run_id = st.session_state.get("scenario_run_id")
        if run_id:
            runner.stop_scenario(run_id)
with col_refresh:
    st.button("🔄 Обновить", width="stretch")

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
hints: list[str] = []
NOT_APPLICABLE_TEXT = "не применимо/не логируется"


def _to_display_value(value: object) -> str:
    if value is None:
        return NOT_APPLICABLE_TEXT
    if isinstance(value, str):
        normalized = value.strip()
        return normalized or NOT_APPLICABLE_TEXT
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def _extract_hint(error_reason: object, actual_result: object) -> str | None:
    hint_candidates: list[str] = []
    if isinstance(error_reason, str):
        match = re.search(r"hint=([a-z0-9_]+)", error_reason)
        if match:
            hint_candidates.append(match.group(1))
    if isinstance(actual_result, dict):
        actual_hint = actual_result.get("hint")
        if isinstance(actual_hint, str):
            hint_candidates.append(actual_hint)
        last_observed = actual_result.get("last_observed")
        if isinstance(last_observed, dict):
            observed_hint = last_observed.get("hint")
            if isinstance(observed_hint, str):
                hint_candidates.append(observed_hint)
    for candidate in hint_candidates:
        normalized = "_".join(candidate.strip().lower().split())
        if normalized:
            return normalized
    return None


def _extract_executed_commands(action_result: object) -> str:
    if not isinstance(action_result, dict):
        return NOT_APPLICABLE_TEXT

    commands: list[str] = []
    details_fallback: str | None = None

    fault = action_result.get("fault_injection")
    if isinstance(fault, dict):
        fault_commands = fault.get("executed_commands")
        if isinstance(fault_commands, list):
            commands.extend(str(command).strip() for command in fault_commands if str(command).strip())

    orchestration = action_result.get("orchestration")
    if isinstance(orchestration, dict):
        executed_command = orchestration.get("executed_command")
        if isinstance(executed_command, str) and executed_command.strip():
            commands.append(executed_command.strip())
        details = orchestration.get("details")
        if isinstance(details, str) and details.strip():
            details_fallback = details.strip()
        elif details is not None:
            details_fallback = _to_display_value(details)

        diagnostics = orchestration.get("diagnostics")
        if isinstance(diagnostics, list):
            for entry in diagnostics:
                if not isinstance(entry, dict):
                    continue
                output = entry.get("output")
                if not isinstance(output, str):
                    continue
                for line in output.splitlines():
                    if line.lower().startswith("command:"):
                        command = line.split(":", 1)[1].strip()
                        if command:
                            commands.append(command)

    traced_commands = action_result.get("executed_commands")
    if isinstance(traced_commands, list):
        for entry in traced_commands:
            if isinstance(entry, dict):
                command = str(entry.get("command", "")).strip()
                command_type = str(entry.get("type", "")).strip()
                if command:
                    commands.append(f"[{command_type}] {command}" if command_type else command)
            elif isinstance(entry, str) and entry.strip():
                commands.append(entry.strip())

    unique_commands = list(dict.fromkeys(commands))
    if unique_commands:
        return "\n".join(unique_commands)

    if details_fallback:
        return f"(нет команд: {details_fallback})"

    if action_result:
        return "(нет команд: read-only проверка метрик)"

    return NOT_APPLICABLE_TEXT


run_level_hint = _extract_hint(run.error_reason, None)
if run_level_hint:
    hints.append(run_level_hint)


for log in run.step_logs:
    hint = _extract_hint(log.error_reason, log.actual_result)
    if hint:
        hints.append(hint)
    rows.append(
        {
            "Шаг": log.index + 1,
            "Action": log.action_type,
            "Target": log.target_node,
            "Timeout (sec)": log.timeout,
            "Команды (SSH/Cluster)": _extract_executed_commands(log.action_result),
            "Ожидаемый результат": _to_display_value(log.expected_result),
            "Фактический результат": _to_display_value(log.actual_result),
            "Статус шага": _to_display_value(log.status),
            "Ошибка": _to_display_value(log.error_reason),
        }
    )

st.subheader("Лог шагов")
st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)

if hints:
    st.subheader("Диагностические hints")
    for hint in sorted(set(hints)):
        st.code(hint, language="text")

if run.status == RunStatus.SUCCEEDED:
    st.success("Итоговый verdict: SCENARIO PASSED")
elif run.status == RunStatus.FAILED:
    st.error("Итоговый verdict: SCENARIO FAILED")
elif run.status == RunStatus.CANCELLED:
    st.warning("Итоговый verdict: SCENARIO CANCELLED")
else:
    st.info("Итоговый verdict: SCENARIO IN PROGRESS")

if is_run_finished(run.status):
    report_cache = st.session_state["scenario_report_artifacts"]
    report_info = report_cache.get(run.run_id)
    if report_info is None:
        report_info = build_and_save_report_bundle(run)
        report_cache[run.run_id] = report_info

    st.subheader("Отчёт по завершённому run")
    st.caption("Отчёт автоматически сформирован в форматах HTML/PDF/JSON с артефактами метрик и логов.")
    bundle_path = report_info["bundle_path"]
    with open(bundle_path, "rb") as report_file:
        st.download_button(
            "Скачать отчёт",
            data=report_file.read(),
            file_name=report_info["bundle_name"],
            mime="application/zip",
            width="stretch",
            type="primary",
        )
    st.code(
        f"JSON: {report_info['json_path']}\n"
        f"HTML: {report_info['html_path']}\n"
        f"PDF: {report_info['pdf_path']}\n",
        language="text",
    )
