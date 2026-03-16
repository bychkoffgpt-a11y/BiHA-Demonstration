from __future__ import annotations

import json
import random
import shlex
import subprocess
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import psycopg
import streamlit as st

APP_TITLE = "BiHA PostgreSQL Cluster Demo"
DEFAULT_HISTORY = 120


@dataclass
class NodeConfig:
    name: str
    dsn: str
    role_hint: str = "unknown"
    control_via_ssh: bool = False
    ssh_host: str | None = None
    ssh_user: str | None = None
    service_name: str = "postgrespro"


@dataclass
class ClusterConfig:
    nodes: list[NodeConfig]
    poll_interval_sec: int = 2


class WorkloadGenerator:
    """Background thread that generates read/write load according to selected mode."""

    def __init__(self) -> None:
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()
        self._stats: dict[str, int] = {"read_tx": 0, "write_tx": 0, "errors": 0}

    @property
    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def stats_snapshot(self) -> dict[str, int]:
        with self._lock:
            return dict(self._stats)

    def reset_stats(self) -> None:
        with self._lock:
            for key in self._stats:
                self._stats[key] = 0

    def start(self, cluster: ClusterConfig, mode: str, tps: int, read_ratio: float) -> None:
        if self.running:
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            args=(cluster, mode, tps, read_ratio),
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=2)

    def _run(self, cluster: ClusterConfig, mode: str, tps: int, read_ratio: float) -> None:
        interval = max(0.05, 1.0 / max(tps, 1))
        while not self._stop_event.is_set():
            try:
                write_tx = random.random() > read_ratio
                node = select_node_for_workload(cluster.nodes, mode, write_tx)
                if not node:
                    raise RuntimeError("No available node for selected mode")
                execute_workload_tx(node, write_tx)
                with self._lock:
                    self._stats["write_tx" if write_tx else "read_tx"] += 1
            except Exception:
                with self._lock:
                    self._stats["errors"] += 1
            time.sleep(interval)


def select_node_for_workload(nodes: list[NodeConfig], mode: str, write_tx: bool) -> NodeConfig | None:
    masters = [n for n in nodes if n.role_hint == "master"]
    slaves = [n for n in nodes if n.role_hint == "slave"]

    if mode == "single-node":
        return masters[0] if masters else (nodes[0] if nodes else None)

    if mode == "dual-read":
        if write_tx:
            return masters[0] if masters else None
        return random.choice(nodes) if nodes else None

    if mode == "master-rw-slave-r":
        if write_tx:
            return masters[0] if masters else None
        if slaves:
            return random.choice(slaves)
        return masters[0] if masters else None

    return nodes[0] if nodes else None


def execute_workload_tx(node: NodeConfig, write_tx: bool) -> None:
    with psycopg.connect(node.dsn, connect_timeout=2, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_sleep(0.01), 1")
            if write_tx:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS biha_demo_load (
                        id bigserial PRIMARY KEY,
                        created_at timestamptz NOT NULL DEFAULT now(),
                        payload text NOT NULL
                    )
                    """
                )
                cur.execute(
                    "INSERT INTO biha_demo_load(payload) VALUES (%s)",
                    (f"demo-{time.time()}",),
                )


def load_cluster_config(path: Path) -> ClusterConfig:
    cfg = json.loads(path.read_text(encoding="utf-8"))
    nodes = [NodeConfig(**item) for item in cfg.get("nodes", [])]
    return ClusterConfig(nodes=nodes, poll_interval_sec=cfg.get("poll_interval_sec", 2))


def fetch_node_metrics(node: NodeConfig) -> dict[str, Any]:
    result: dict[str, Any] = {
        "node": node.name,
        "status": "down",
        "role": node.role_hint,
        "replay_delay_sec": None,
        "active_locks": None,
        "xact_commit": None,
        "xact_rollback": None,
        "blks_read": None,
        "blks_hit": None,
        "tup_returned": None,
        "tup_fetched": None,
        "error": None,
    }

    try:
        with psycopg.connect(node.dsn, connect_timeout=2, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        CASE WHEN pg_is_in_recovery() THEN 'slave' ELSE 'master' END,
                        COALESCE(EXTRACT(EPOCH FROM now() - pg_last_xact_replay_timestamp())::bigint, 0),
                        (SELECT count(*) FROM pg_locks WHERE granted),
                        (SELECT xact_commit FROM pg_stat_database WHERE datname = current_database()),
                        (SELECT xact_rollback FROM pg_stat_database WHERE datname = current_database()),
                        (SELECT blks_read FROM pg_stat_database WHERE datname = current_database()),
                        (SELECT blks_hit FROM pg_stat_database WHERE datname = current_database()),
                        (SELECT tup_returned FROM pg_stat_database WHERE datname = current_database()),
                        (SELECT tup_fetched FROM pg_stat_database WHERE datname = current_database())
                    """
                )
                row = cur.fetchone()
                if row:
                    result.update(
                        {
                            "status": "up",
                            "role": row[0],
                            "replay_delay_sec": row[1],
                            "active_locks": row[2],
                            "xact_commit": row[3],
                            "xact_rollback": row[4],
                            "blks_read": row[5],
                            "blks_hit": row[6],
                            "tup_returned": row[7],
                            "tup_fetched": row[8],
                        }
                    )
    except Exception as exc:
        result["error"] = str(exc)

    return result


def run_node_action(node: NodeConfig, action: str) -> tuple[bool, str]:
    if not node.control_via_ssh or not node.ssh_host:
        return False, "SSH control disabled for this node in config"

    user_prefix = f"{node.ssh_user}@" if node.ssh_user else ""
    if action == "stop":
        remote_cmd = f"sudo systemctl stop {shlex.quote(node.service_name)}"
    elif action == "start":
        remote_cmd = f"sudo systemctl start {shlex.quote(node.service_name)}"
    elif action == "restart":
        remote_cmd = f"sudo systemctl restart {shlex.quote(node.service_name)}"
    else:
        return False, f"Unknown action: {action}"

    cmd = ["ssh", f"{user_prefix}{node.ssh_host}", remote_cmd]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
    ok = proc.returncode == 0
    output = (proc.stdout + "\n" + proc.stderr).strip()
    return ok, output or "OK"


def render_sidebar() -> dict[str, Any]:
    st.sidebar.header("Load profile")
    mode = st.sidebar.selectbox(
        "Mode",
        options=["single-node", "dual-read", "master-rw-slave-r"],
        help="single-node: вся нагрузка на master; dual-read: чтение с обеих; master-rw-slave-r: запись на master, чтение с slave",
    )
    tps = st.sidebar.slider("TPS", min_value=1, max_value=200, value=20)
    read_ratio = st.sidebar.slider("Read ratio", 0.0, 1.0, 0.7, 0.05)
    auto_refresh = st.sidebar.checkbox("Auto-refresh", value=True)
    return {"mode": mode, "tps": tps, "read_ratio": read_ratio, "auto_refresh": auto_refresh}


def render_controls(cluster: ClusterConfig, wg: WorkloadGenerator, profile: dict[str, Any]) -> None:
    st.subheader("Scenario controls")
    col1, col2, col3, col4 = st.columns(4)

    if col1.button("Start load", type="primary", use_container_width=True):
        wg.start(cluster, profile["mode"], int(profile["tps"]), float(profile["read_ratio"]))
    if col2.button("Stop load", use_container_width=True):
        wg.stop()
    if col3.button("Reset counters", use_container_width=True):
        wg.reset_stats()
    if col4.button("Refresh now", use_container_width=True):
        st.rerun()

    st.caption(f"Generator state: {'RUNNING' if wg.running else 'STOPPED'}")
    st.markdown("#### Failure simulation")

    for node in cluster.nodes:
        c1, c2, c3, c4 = st.columns([2, 1, 1, 1])
        c1.write(f"**{node.name}** ({node.role_hint})")
        if c2.button(f"Stop {node.name}", key=f"stop-{node.name}"):
            ok, msg = run_node_action(node, "stop")
            st.toast(f"{node.name} stop: {'OK' if ok else 'ERR'} | {msg}")
        if c3.button(f"Start {node.name}", key=f"start-{node.name}"):
            ok, msg = run_node_action(node, "start")
            st.toast(f"{node.name} start: {'OK' if ok else 'ERR'} | {msg}")
        if c4.button(f"Restart {node.name}", key=f"restart-{node.name}"):
            ok, msg = run_node_action(node, "restart")
            st.toast(f"{node.name} restart: {'OK' if ok else 'ERR'} | {msg}")


def render_metrics(cluster: ClusterConfig, wg: WorkloadGenerator) -> None:
    st.subheader("Cluster state")
    rows = [fetch_node_metrics(node) for node in cluster.nodes]
    df = pd.DataFrame(rows)
    st.dataframe(df, use_container_width=True)

    summary_cols = st.columns(4)
    up_nodes = int((df["status"] == "up").sum()) if not df.empty else 0
    summary_cols[0].metric("Nodes UP", up_nodes)
    summary_cols[1].metric("Master", ", ".join(df[df["role"] == "master"]["node"].tolist()) or "-")
    summary_cols[2].metric("Slave", ", ".join(df[df["role"] == "slave"]["node"].tolist()) or "-")

    stats = wg.stats_snapshot()
    summary_cols[3].metric("Generator errors", stats["errors"])

    st.subheader("Load stats")
    st.json(stats)

    if "history" not in st.session_state:
        st.session_state.history = []

    history = st.session_state.history
    history.append(
        {
            "ts": pd.Timestamp.utcnow(),
            "read_tx": stats["read_tx"],
            "write_tx": stats["write_tx"],
            "errors": stats["errors"],
            "active_locks": int(df["active_locks"].fillna(0).sum()) if not df.empty else 0,
            "blks_read": int(df["blks_read"].fillna(0).sum()) if not df.empty else 0,
        }
    )

    if len(history) > DEFAULT_HISTORY:
        del history[:-DEFAULT_HISTORY]

    hist_df = pd.DataFrame(history)
    if not hist_df.empty:
        st.line_chart(hist_df.set_index("ts")[["read_tx", "write_tx", "errors"]])
        st.line_chart(hist_df.set_index("ts")[["active_locks", "blks_read"]])


def main() -> None:
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)
    st.caption("Demo GUI for BiHA PostgreSQL Pro cluster validation")

    cfg_path = Path(st.text_input("Path to config", "config/cluster.example.json"))
    if not cfg_path.exists():
        st.error(f"Config not found: {cfg_path}")
        st.stop()

    try:
        cluster = load_cluster_config(cfg_path)
    except Exception as exc:
        st.error(f"Cannot parse config: {exc}")
        st.stop()

    if "workload_generator" not in st.session_state:
        st.session_state.workload_generator = WorkloadGenerator()

    wg: WorkloadGenerator = st.session_state.workload_generator
    profile = render_sidebar()
    render_controls(cluster, wg, profile)
    render_metrics(cluster, wg)

    if profile["auto_refresh"]:
        time.sleep(cluster.poll_interval_sec)
        st.rerun()


if __name__ == "__main__":
    main()
