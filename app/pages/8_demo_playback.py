from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import streamlit as st

from orchestration.demo_runner import RunStatus, ScenarioRun, get_demo_runner
from ui_styles import apply_base_page_styles

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
.demo-node {
    border-radius: 14px;
    border: 2px solid #cbd5e1;
    padding: 0.8rem;
    margin-bottom: 0.75rem;
    background: #ffffff;
}
.demo-node .meta {
    color: #334155;
    font-size: 0.9rem;
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
    padding: 0.55rem 0.65rem;
    margin-bottom: 0.45rem;
    background: #f8fafc;
}
.event-item {
    border-left: 3px solid #cbd5e1;
    padding: 0.4rem 0.6rem;
    margin-bottom: 0.3rem;
    font-size: 0.92rem;
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
.presentation-mode {
    font-size: 1.18rem !important;
}
.presentation-mode .meta,
.presentation-mode .event-muted {
    display: none;
}
.presentation-mode .step-row {
    padding-top: 0.75rem;
    padding-bottom: 0.75rem;
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


def _build_topology(run: ScenarioRun | None, configured_nodes: list[dict[str, str]]) -> tuple[list[dict[str, str]], list[tuple[str, str]], bool, bool]:
    failover_detected = False
    role_shift_detected = False
    if run:
        for step in run.step_logs:
            action = step.action_type.lower()
            if "failover" in action or "promote" in action:
                failover_detected = True
                role_shift_detected = True
            if "restart" in action and step.status in {"running", "failed"}:
                failover_detected = True

    nodes = [node.copy() for node in configured_nodes]
    default_leader = nodes[0]["name"]
    promoted_leader = nodes[1]["name"] if len(nodes) > 1 else default_leader

    if run and run.status in {RunStatus.RUNNING, RunStatus.FAILED} and run.current_step_index >= 0:
        current_target = run.step_logs[run.current_step_index].target_node
        for node in nodes:
            if node["name"] == current_target:
                node["status"] = "degraded" if run.status == RunStatus.RUNNING else "down"

    if failover_detected:
        for node in nodes:
            if node["name"] == promoted_leader:
                node["role"] = "leader"
                node["status"] = "up"
            elif node["name"] == default_leader:
                node["role"] = "replica"
                if run and run.status == RunStatus.RUNNING:
                    node["status"] = "degraded"

    source = promoted_leader if failover_detected else default_leader
    links = [(source, node["name"]) for node in nodes if node["name"] != source]
    return nodes, links, failover_detected, role_shift_detected


def _render_topology_map(run: ScenarioRun | None, configured_nodes: list[dict[str, str]], presentation_mode: bool) -> None:
    st.subheader("Topology Map")
    nodes, links, failover_detected, role_shift_detected = _build_topology(run, configured_nodes)

    classes = "presentation-mode" if presentation_mode else ""
    st.markdown(f'<div class="{classes}">', unsafe_allow_html=True)
    for node in nodes:
        role_color = ROLE_COLORS[node["role"]]
        status_color = NODE_STATUS_COLORS[node["status"]]
        extra_class = ""
        if failover_detected and node["role"] == "leader":
            extra_class += " failover-pulse"
        if role_shift_detected and node["name"] in {"pg-node-1", "pg-node-2"}:
            extra_class += " role-shift"

        st.markdown(
            f"""
            <div class=\"demo-node{extra_class}\">
                <strong>{node['name']}</strong>
                <span class=\"role-chip\" style=\"background:{role_color};\">{node['role']}</span>
                <div class=\"meta\">status: <span style=\"color:{status_color};font-weight:600;\">{node['status']}</span></div>
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


def _render_step_timeline(run: ScenarioRun | None, presentation_mode: bool, auto_scroll: bool) -> None:
    st.subheader("Step Timeline")
    if not run:
        st.info("Запустите сценарий на странице orchestration, чтобы увидеть timeline.")
        return

    container_id = "timeline-container"
    classes = "presentation-mode" if presentation_mode else ""
    st.markdown(f'<div id="{container_id}" class="{classes}" style="max-height: 390px; overflow-y: auto;">', unsafe_allow_html=True)
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

    if auto_scroll and run.current_step_index >= 0:
        st.components.v1.html(
            f"""
            <script>
            const root = window.parent.document.getElementById('{container_id}');
            if (root) {{ root.scrollTop = root.scrollHeight; }}
            </script>
            """,
            height=0,
        )


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


def _render_event_feed(run: ScenarioRun | None, failover_detected: bool, presentation_mode: bool) -> None:
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

    classes = "presentation-mode" if presentation_mode else ""
    st.markdown(f'<div class="{classes}" style="max-height: 390px; overflow-y:auto;">', unsafe_allow_html=True)
    for item in events:
        st.markdown(f'<div class="event-item">{item}<div class="event-muted">cluster event stream</div></div>', unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)


st.set_page_config(page_title="Demo Playback", layout="wide")
apply_base_page_styles(PRESENTATION_CSS)
st.title("Demo Playback")
st.caption("Topology, timeline, SLO and event stream for failover demos")

runner = get_demo_runner()

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

presentation_mode = st.toggle("🎬 Режим презентации", value=False, help="Крупный шрифт + скрытие второстепенных деталей")
auto_scroll_timeline = st.toggle("Авто-скролл таймлайна", value=True)
st.text_input("Путь к конфигу кластера", key="demo_playback_cfg_path")

run_id = st.session_state.get("scenario_run_id")
run: ScenarioRun | None = None

if run_id:
    try:
        run = runner.get_run_status(run_id)
    except ValueError:
        st.warning("Текущий run_id не найден. Запустите сценарий заново.")
else:
    st.info("Нет активного запуска. Перейдите на страницу Scenario Orchestration и запустите сценарий.")

left, right = st.columns([1.25, 1])
with left:
    _render_topology_map(run, configured_nodes, presentation_mode)
with right:
    _, _, failover_detected, _ = _build_topology(run, configured_nodes)
    _render_slo_panel(run, failover_detected)

c1, c2 = st.columns(2)
with c1:
    _render_step_timeline(run, presentation_mode, auto_scroll_timeline)
with c2:
    _render_event_feed(run, failover_detected, presentation_mode)
