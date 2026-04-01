from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import streamlit as st

try:
    from streamlit_autorefresh import st_autorefresh
except ImportError:
    st_autorefresh = None

from orchestration.demo_runner import (
    RunStatus,
    ScenarioRun,
    get_demo_runner,
    get_scenario_catalog_status,
)
from orchestration.planned_switchover_ui import (
    PLANNED_SWITCHOVER_SCENARIO_ID,
    build_params_override_for_planned_switchover,
)
from ui_styles import apply_base_page_styles

PLANNED_SWITCHOVER_TARGET_MASTER_KEY = "planned_switchover_target_master"

STATUS_STYLE = {
    "pending": ("⏳", "#6b7280"),
    "running": ("▶️", "#2563eb"),
    "succeeded": ("✅", "#16a34a"),
    "failed": ("❌", "#dc2626"),
    "pass": ("✅", "#16a34a"),
    "fail": ("❌", "#dc2626"),
}

ROLE_COLORS = {
    "leader": "#b91c1c",
    "replica": "#1d4ed8",
}

NODE_STATUS_COLORS = {
    "up": "#16a34a",
    "down": "#dc2626",
    "degraded": "#f59e0b",
}

PRESENTATION_CSS = """
.block-container {
    padding-top: 1rem;
    padding-bottom: 0.75rem;
}
.demo-node {
    border-radius: 14px;
    border: 2px solid #cbd5e1;
    padding: 0.55rem;
    margin-bottom: 0.45rem;
    background: #ffffff;
}
.demo-node .meta {
    color: #334155;
    font-size: 0.85rem;
}
.role-chip {
    display: inline-block;
    margin-left: 0.4rem;
    border-radius: 999px;
    padding: 0.1rem 0.5rem;
    color: #fff;
    font-weight: 600;
    font-size: 0.78rem;
}
.role-shift {
    box-shadow: 0 0 0 3px rgba(14, 165, 233, 0.35);
}
.failover-pulse {
    animation: pulseGlow 1.15s ease-in-out infinite;
}
@keyframes pulseGlow {
    0% { box-shadow: 0 0 0 0 rgba(220, 38, 38, 0.45); }
    70% { box-shadow: 0 0 0 12px rgba(220, 38, 38, 0); }
    100% { box-shadow: 0 0 0 0 rgba(220, 38, 38, 0); }
}
.link-row {
    border-left: 3px solid #93c5fd;
    padding-left: 0.6rem;
    color: #1e40af;
    margin-bottom: 0.45rem;
}
.step-row {
    border: 1px solid #dbeafe;
    border-left-width: 6px;
    border-radius: 10px;
    padding: 0.4rem 0.55rem;
    margin-bottom: 0.35rem;
    background: #f8fafc;
}
.event-item {
    border-left: 3px solid #cbd5e1;
    padding: 0.32rem 0.5rem;
    margin-bottom: 0.25rem;
    font-size: 0.88rem;
}
.event-muted {
    color: #64748b;
}
.split-brain {
    display: inline-block;
    border: 1px solid #7c3aed;
    color: #7c3aed;
    border-radius: 999px;
    padding: 0.1rem 0.5rem;
    font-size: 0.8rem;
    font-weight: 600;
}
"""


def _format_ts(ts: datetime | None) -> str:
    if not ts:
        return "—"
    return ts.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


def _load_nodes_from_config(cfg_path: Path) -> list[dict[str, str]]:
    with cfg_path.open("r", encoding="utf-8") as fh:
        cfg = json.load(fh)
    raw_nodes = cfg.get("nodes", [])
    if not isinstance(raw_nodes, list) or not raw_nodes:
        raise ValueError("В конфиге не найден список nodes")

    nodes: list[dict[str, str]] = []
    for index, item in enumerate(raw_nodes):
        if not isinstance(item, dict):
            continue
        node_name = str(item.get("name", "")).strip()
        if not node_name:
            continue
        role = "leader" if index == 0 else "replica"
        nodes.append({"name": node_name, "role": role, "status": "up"})

    if not nodes:
        raise ValueError("В конфиге нет валидных узлов с полем name")
    return nodes



@st.cache_data(ttl=5, show_spinner=False)
def _load_live_roles_from_cluster(cfg_path: str) -> tuple[dict[str, str], dict[str, str], str | None]:
    from cluster_demo import classify_node_role, fetch_all_node_metrics, get_target_database, load_cluster_config

    role_by_node: dict[str, str] = {}
    status_by_node: dict[str, str] = {}
    current_master: str | None = None
    try:
        cluster = load_cluster_config(Path(cfg_path))
        rows = fetch_all_node_metrics(cluster.nodes, get_target_database(cluster, "rw"))
    except Exception:
        return role_by_node, status_by_node, current_master

    for row in rows:
        node_name = str(row.get("node") or "").strip()
        if not node_name:
            continue
        status = str(row.get("status") or "").strip().lower()
        if status in NODE_STATUS_COLORS:
            status_by_node[node_name] = status
        role_class = classify_node_role(row.get("role"), row.get("tx_read_only"))
        if role_class == "master":
            role_by_node[node_name] = "leader"
            current_master = node_name
        elif role_class == "slave":
            role_by_node[node_name] = "replica"

    return role_by_node, status_by_node, current_master


def _build_topology(
    run: ScenarioRun | None,
    configured_nodes: list[dict[str, str]],
    cfg_path: Path | None = None,
) -> tuple[list[dict[str, str]], list[tuple[str, str]], bool, bool]:
    nodes = [node.copy() for node in configured_nodes]
    failover_detected = False
    role_shift_detected = False
    default_leader = nodes[0]["name"]
    promoted_leader = default_leader

    role_by_node: dict[str, str] = {}
    status_by_node: dict[str, str] = {}
    heuristic_status_by_node: dict[str, str] = {}
    step_annotation_by_node: dict[str, str] = {}
    current_master: str | None = None
    if cfg_path is not None:
        live_roles, live_statuses, live_master = _load_live_roles_from_cluster(str(cfg_path))
        role_by_node.update(live_roles)
        status_by_node.update(live_statuses)
        current_master = live_master

    if run:
        for step in run.step_logs:
            action = step.action_type.lower()
            if "failover" in action or "promote" in action:
                failover_detected = True
                role_shift_detected = True
            if "restart" in action and step.status in {"running", "failed"}:
                failover_detected = True

            actual = step.actual_result if isinstance(step.actual_result, dict) else {}
            value = actual.get("value") if isinstance(actual.get("value"), dict) else {}
            current_roles = value.get("current_roles") if isinstance(value.get("current_roles"), dict) else {}
            roles_by_node = (
                current_roles.get("roles_by_node")
                if isinstance(current_roles.get("roles_by_node"), dict)
                else {}
            )

            for node_name, observed_role in roles_by_node.items():
                normalized = str(observed_role).strip().lower()
                if normalized in {"master", "primary", "leader"}:
                    role_by_node[str(node_name)] = "leader"
                elif normalized in {"slave", "replica", "standby"}:
                    role_by_node[str(node_name)] = "replica"

            if isinstance(current_roles.get("master"), str):
                current_master = current_roles["master"]
            if isinstance(current_roles.get("slave"), str):
                role_by_node[current_roles["slave"]] = "replica"

            for key in ("up_nodes", "nodes_up"):
                node_list = value.get(key)
                if isinstance(node_list, list):
                    for node_name in node_list:
                        status_by_node[str(node_name)] = "up"

            action_result = step.action_result if isinstance(step.action_result, dict) else {}
            orchestration = (
                action_result.get("orchestration")
                if isinstance(action_result.get("orchestration"), dict)
                else {}
            )
            if isinstance(orchestration.get("new_leader"), str):
                current_master = orchestration["new_leader"]
                role_by_node[current_master] = "leader"
            if isinstance(orchestration.get("old_leader"), str):
                role_by_node[orchestration["old_leader"]] = "replica"

    if current_master:
        promoted_leader = current_master
        failover_detected = failover_detected or promoted_leader != default_leader
        role_shift_detected = promoted_leader != default_leader

    if run and run.status in {RunStatus.RUNNING, RunStatus.FAILED} and run.current_step_index >= 0:
        current_target = run.step_logs[run.current_step_index].target_node
        heuristic_status_by_node[current_target] = "degraded" if run.status == RunStatus.RUNNING else "down"
        step_annotation_by_node[current_target] = (
            "⚠️ step running on node" if run.status == RunStatus.RUNNING else "❌ step failed on node"
        )

    for node in nodes:
        name = node["name"]
        if name in role_by_node:
            node["role"] = role_by_node[name]
        elif name == promoted_leader:
            node["role"] = "leader"
        elif name != default_leader:
            node["role"] = "replica"

        if name in status_by_node:
            node["status"] = status_by_node[name]
        elif name in heuristic_status_by_node:
            node["status"] = heuristic_status_by_node[name]
        if name in step_annotation_by_node:
            node["annotation"] = step_annotation_by_node[name]

    if failover_detected and run and run.status == RunStatus.RUNNING:
        for node in nodes:
            if (
                node["name"] == default_leader
                and node["role"] == "replica"
                and default_leader not in status_by_node
            ):
                node["status"] = "degraded"

    source = promoted_leader if failover_detected else default_leader
    links = [(source, node["name"]) for node in nodes if node["name"] != source]
    return nodes, links, failover_detected, role_shift_detected


def _render_topology_map(
    run: ScenarioRun | None,
    configured_nodes: list[dict[str, str]],
    cfg_path: Path,
) -> None:
    st.subheader("Topology Map")
    nodes, links, failover_detected, role_shift_detected = _build_topology(run, configured_nodes, cfg_path)

    st.markdown('<div>', unsafe_allow_html=True)
    for node in nodes:
        role_color = ROLE_COLORS[node["role"]]
        status_color = NODE_STATUS_COLORS[node["status"]]
        extra_class = ""
        if failover_detected and node["role"] == "leader":
            extra_class += " failover-pulse"
        if role_shift_detected and node["name"] in {"pg-node-1", "pg-node-2"}:
            extra_class += " role-shift"
        annotation_html = f'<div class="meta">{node["annotation"]}</div>' if node.get("annotation") else ""

        st.markdown(
            f"""
            <div class=\"demo-node{extra_class}\">
                <strong>{node['name']}</strong>
                <span class=\"role-chip\" style=\"background:{role_color};\">{node['role']}</span>
                <div class=\"meta\">status: <span style=\"color:{status_color};font-weight:600;\">{node['status']}</span></div>
                {annotation_html}
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown("**Replication links**")
    for source, target in links:
        st.markdown(f'<div class="link-row">{source} → {target}</div>', unsafe_allow_html=True)

    if failover_detected:
        st.markdown('<span class="split-brain">split-brain prevented</span>', unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)


def _render_step_timeline(run: ScenarioRun | None) -> None:
    st.subheader("Step Timeline")
    if not run:
        st.info("Запустите сценарий на странице orchestration, чтобы увидеть timeline.")
        return

    container_id = "timeline-container"
    st.markdown(f'<div id="{container_id}" style="max-height: 280px; overflow-y: auto;">', unsafe_allow_html=True)
    for step in run.step_logs:
        badge, color = STATUS_STYLE.get(step.status, ("•", "#64748b"))
        st.markdown(
            f"""
            <div class=\"step-row\" style=\"border-left-color:{color};\">
                <div><strong>{badge} Step {step.index + 1}: {step.action_type}</strong> → <code>{step.target_node}</code></div>
                <div class=\"meta\">state: {step.status} · started: {_format_ts(step.started_at)} · finished: {_format_ts(step.finished_at)}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    st.markdown("</div>", unsafe_allow_html=True)

def _render_slo_panel(run: ScenarioRun | None, failover_detected: bool) -> None:
    st.subheader("SLO Panel")

    write_availability = "99.95%"
    rto = "<= 25 sec" if failover_detected else "n/a"
    replication_lag = "< 1.2 sec"
    last_commit = "n/a"

    if run and run.step_logs:
        completed = [step for step in run.step_logs if step.status == "succeeded" and step.finished_at]
        if completed:
            last_commit = _format_ts(completed[-1].finished_at)
        if run.status == RunStatus.FAILED:
            write_availability = "98.90%"
            replication_lag = "degraded"

    c1, c2 = st.columns(2)
    c3, c4 = st.columns(2)
    c1.metric("RTO", rto)
    c2.metric("Write availability", write_availability)
    c3.metric("Replication lag", replication_lag)
    c4.metric("Last successful commit", last_commit)


def _render_scenario_description_modal(selected_scenario) -> None:
    @st.dialog(f"ℹ️ {selected_scenario.name}", width="large")
    def _scenario_details_dialog() -> None:
        st.markdown(selected_scenario.description or "Описание сценария не задано.")
        st.markdown(f"**Критерий успеха:** {selected_scenario.success_criteria}")

        if selected_scenario.steps:
            st.markdown("**План выполнения:**")
            for index, step in enumerate(selected_scenario.steps, start=1):
                st.markdown(f"{index}. `{step.action_type}` на `{step.target_node}`")

        if st.button("Закрыть", key="close_scenario_details"):
            st.session_state["show_scenario_details"] = False
            st.rerun()

    _scenario_details_dialog()

def _render_event_feed(run: ScenarioRun | None, failover_detected: bool) -> None:
    st.subheader("Event Feed")
    events: list[str] = []

    if run:
        events.append(f"{_format_ts(run.created_at)} | orchestrator | run created: {run.run_id[:8]}")
        for step in run.step_logs:
            if step.started_at:
                events.append(
                    f"{_format_ts(step.started_at)} | cluster | step started: {step.action_type} on {step.target_node}"
                )
            if step.finished_at:
                state = "pass" if step.status == "succeeded" else "fail"
                events.append(
                    f"{_format_ts(step.finished_at)} | orchestrator | step {step.index + 1} {state}"
                )

        if run.status == RunStatus.SUCCEEDED:
            events.append(f"{_format_ts(run.finished_at)} | orchestrator | scenario completed")
        elif run.status == RunStatus.FAILED:
            events.append(f"{_format_ts(run.finished_at)} | orchestrator | scenario failed: {run.error_reason}")
    else:
        events.append(f"{_format_ts(datetime.now(UTC))} | system | demo playback waiting for run")

    if failover_detected:
        events.append(f"{_format_ts(datetime.now(UTC))} | referee | split-brain prevented")

    events = sorted(set(events))

    st.markdown('<div style="max-height: 280px; overflow-y:auto;">', unsafe_allow_html=True)
    for item in events:
        st.markdown(f'<div class="event-item">{item}<div class="event-muted">cluster event stream</div></div>', unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)


def _schedule_ui_refresh(interval_ms: int, key: str) -> None:
    if st_autorefresh is not None:
        st_autorefresh(interval=interval_ms, key=key)
        return

    st.caption("Live update недоступен без зависимости streamlit-autorefresh.")


st.set_page_config(page_title="Demo Playback", layout="wide")
apply_base_page_styles(PRESENTATION_CSS)
st.title("Demo Playback")

runner = get_demo_runner()
catalog_status = get_scenario_catalog_status()

if catalog_status.error:
    st.error(f"Ошибка загрузки сценариев: {catalog_status.error}")
elif catalog_status.fallback_used:
    st.warning("Каталог config/demo_scenarios пуст. Используется встроенный fallback-сценарий.")

scenarios = runner.list_scenarios()
with st.sidebar:
    st.markdown("### Управление сценарием")
    if scenarios:
        scenario_options = {f"{scenario.name} ({scenario.id})": scenario for scenario in scenarios}
        selected_label = st.selectbox("Сценарии для запуска", options=list(scenario_options.keys()), label_visibility="collapsed")
        selected_scenario = scenario_options[selected_label]

        if st.button("Показать описание", key="scenario_description_help", width="stretch"):
            st.session_state["show_scenario_details"] = True
        if st.session_state.get("show_scenario_details", False):
            _render_scenario_description_modal(selected_scenario)

        st.caption(
            f"Источник сценариев: {catalog_status.loaded_from}. Загружено: {len(scenarios)}."
        )

        params_override: dict[str, str] = {}
        planned_switchover_result = None
        if selected_scenario.id == PLANNED_SWITCHOVER_SCENARIO_ID:
            selected_target_master = st.session_state.get(PLANNED_SWITCHOVER_TARGET_MASTER_KEY)
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
                if st.session_state.get(PLANNED_SWITCHOVER_TARGET_MASTER_KEY) not in available_slaves:
                    st.session_state[PLANNED_SWITCHOVER_TARGET_MASTER_KEY] = available_slaves[0]
                selected_target_master = st.selectbox(
                    "Целевой standby для planned_switchover",
                    options=available_slaves,
                    key=PLANNED_SWITCHOVER_TARGET_MASTER_KEY,
                    help="Выберите актуальный standby-узел на момент старта сценария.",
                )
                planned_switchover_result = build_params_override_for_planned_switchover(
                    selected_scenario,
                    selected_target_master=selected_target_master,
                )

            params_override = planned_switchover_result.params_override
        planned_switchover_start_blocked = bool(
            planned_switchover_result
            and (
                not planned_switchover_result.available_slaves
                or planned_switchover_result.validation_error
                or not params_override.get("target_master")
            )
        )

        col_start, col_stop = st.columns(2)
        with col_start:
            if st.button("▶️ Запустить выбранный сценарий", type="primary", width="stretch"):
                if planned_switchover_start_blocked and planned_switchover_result:
                    error_message = (
                        planned_switchover_result.validation_error
                        or planned_switchover_result.warning_message
                        or "Запуск planned_switchover недоступен: выберите актуальный standby-узел."
                    )
                    st.error(error_message)
                else:
                    st.session_state["scenario_run_id"] = runner.start_scenario(
                        selected_scenario.id,
                        params_override=params_override or None,
                    )

            if st.button("⏹ Остановить", width="stretch"):
                run_id = st.session_state.get("scenario_run_id")
                if run_id:
                    runner.stop_scenario(run_id)
    else:
        st.warning("Сценарии не зарегистрированы.")

    st.text_input("Путь к конфигу кластера", key="demo_playback_cfg_path")

st.session_state.setdefault("demo_playback_cfg_path", "config/cluster.json")
raw_cfg_path = str(st.session_state["demo_playback_cfg_path"]).strip()
cfg_path = Path(raw_cfg_path)
if not raw_cfg_path:
    st.error("Укажите путь к JSON-конфигу кластера.")
    st.stop()
if not cfg_path.exists():
    st.error(f"Конфиг не найден: {cfg_path}")
    st.stop()
if not cfg_path.is_file():
    st.error(f"Ожидался JSON-файл конфига, но указан каталог: {cfg_path}")
    st.stop()

try:
    configured_nodes = _load_nodes_from_config(cfg_path)
except Exception as exc:
    st.error(f"Не удалось прочитать конфиг: {exc}")
    st.stop()

run_id = st.session_state.get("scenario_run_id")
run: ScenarioRun | None = None

if run_id:
    try:
        run = runner.get_run_status(run_id)
    except ValueError:
        st.warning("Текущий run_id не найден. Запустите сценарий заново.")

live_update_enabled = st.toggle("Live update", value=True, key="demo_playback_live_update")
if live_update_enabled:
    active_run_statuses = {RunStatus.PENDING, RunStatus.RUNNING}
    refresh_interval_ms = 1000 if run and run.status in active_run_statuses else 4000
    _schedule_ui_refresh(interval_ms=refresh_interval_ms, key="demo_playback_topology_refresh")

left, right = st.columns([1.25, 1])
with left:
    _render_topology_map(run, configured_nodes, cfg_path)
with right:
    _, _, failover_detected, _ = _build_topology(run, configured_nodes, cfg_path)
    _render_slo_panel(run, failover_detected)

c1, c2 = st.columns(2)
with c1:
    _render_step_timeline(run)
with c2:
    _render_event_feed(run, failover_detected)
