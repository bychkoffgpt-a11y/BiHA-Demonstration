from __future__ import annotations

import json
import random
import shlex
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from zoneinfo import ZoneInfo

import pandas as pd
import psycopg
import streamlit as st

from logging_utils import setup_file_logger

APP_TITLE = "BiHA PostgreSQL Cluster Demo"
DEFAULT_HISTORY = 120
MAX_WORKERS = 32
METRICS_FETCH_WORKERS = 8
MOSCOW_TZ = ZoneInfo("Europe/Moscow")
LOGGER = setup_file_logger()


@dataclass
class NodeConfig:
    name: str
    dsn: str
    role_hint: str = "unknown"
    control_via_ssh: bool = False
    ssh_host: str | None = None
    ssh_user: str | None = None
    ssh_port: int = 22
    ssh_identity_file: str | None = None
    ssh_extra_options: list[str] | None = None
    ssh_legacy_algorithms: bool = False
    service_name: str = "postgrespro"


@dataclass
class ClusterConfig:
    nodes: list[NodeConfig]
    poll_interval_sec: int = 2


class WorkloadGenerator:
    """Background thread that generates read/write load according to selected mode."""

    def __init__(self) -> None:
        self._threads: list[threading.Thread] = []
        self._stop_event = threading.Event()
        self._lock = threading.Lock()
        self._stats: dict[str, int] = {"read_tx": 0, "write_tx": 0, "errors": 0}

    @property
    def running(self) -> bool:
        return any(thread.is_alive() for thread in self._threads)

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
        workers = max(1, min(MAX_WORKERS, (tps // 1000) + 1))
        per_worker_tps = tps / workers
        self._threads = []
        for _ in range(workers):
            thread = threading.Thread(
                target=self._run_worker,
                args=(cluster, mode, per_worker_tps, read_ratio),
                daemon=True,
            )
            self._threads.append(thread)
            thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        for thread in self._threads:
            thread.join(timeout=2)
        self._threads = []

    def _run_worker(self, cluster: ClusterConfig, mode: str, tps: float, read_ratio: float) -> None:
        interval = 1.0 / tps if tps > 0 else 0.5
        next_tick = time.perf_counter()
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
                LOGGER.exception("Workload worker transaction failed")
                with self._lock:
                    self._stats["errors"] += 1
            next_tick += interval
            sleep_for = next_tick - time.perf_counter()
            if sleep_for > 0:
                time.sleep(sleep_for)
            else:
                next_tick = time.perf_counter()


def select_node_for_workload(nodes: list[NodeConfig], mode: str, write_tx: bool) -> NodeConfig | None:
    master_hints = {"master", "primary", "leader"}
    slave_hints = {"slave", "replica", "standby"}

    masters = [n for n in nodes if n.role_hint.strip().lower() in master_hints]
    slaves = [n for n in nodes if n.role_hint.strip().lower() in slave_hints]

    fallback_node = nodes[0] if nodes else None

    if mode == "single-node":
        return masters[0] if masters else fallback_node

    if mode == "dual-read":
        if write_tx:
            return masters[0] if masters else fallback_node
        return random.choice(nodes) if nodes else fallback_node

    if mode == "master-rw-slave-r":
        if write_tx:
            return masters[0] if masters else fallback_node
        if slaves:
            return random.choice(slaves)
        if masters:
            return masters[0]
        return fallback_node

    return fallback_node


def execute_workload_tx(node: NodeConfig, write_tx: bool) -> None:
    with psycopg.connect(node.dsn, connect_timeout=2, autocommit=False) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS biha_demo_load (
                    id bigserial PRIMARY KEY,
                    created_at timestamptz NOT NULL DEFAULT now(),
                    payload text NOT NULL
                )
                """
            )
            if write_tx:
                payload = f"demo-{time.time()}"
                cur.execute("INSERT INTO biha_demo_load(payload) VALUES (%s) RETURNING id", (payload,))
                row_id = cur.fetchone()[0]
                cur.execute("SELECT id, payload, created_at FROM biha_demo_load WHERE id = %s", (row_id,))
                cur.execute(
                    "UPDATE biha_demo_load SET payload = payload || '-updated' WHERE id = %s",
                    (row_id,),
                )
            else:
                cur.execute(
                    """
                    SELECT id, payload, created_at
                    FROM biha_demo_load
                    ORDER BY id DESC
                    LIMIT 100
                    """
                )
        conn.commit()


def extract_dbname_from_dsn(dsn: str) -> str:
    parsed = urlparse(dsn)
    db_from_path = parsed.path.lstrip("/")
    if db_from_path:
        return db_from_path
    query = parse_qs(parsed.query)
    return query.get("dbname", ["postgres"])[0]


def get_target_database(cluster: ClusterConfig, mode: str) -> str:
    preferred_node = select_node_for_workload(cluster.nodes, mode, write_tx=True)
    if preferred_node:
        return extract_dbname_from_dsn(preferred_node.dsn)
    return extract_dbname_from_dsn(cluster.nodes[0].dsn) if cluster.nodes else "postgres"


def load_cluster_config(path: Path) -> ClusterConfig:
    cfg = json.loads(path.read_text(encoding="utf-8"))
    nodes = [NodeConfig(**item) for item in cfg.get("nodes", [])]
    return ClusterConfig(nodes=nodes, poll_interval_sec=cfg.get("poll_interval_sec", 2))


def mask_dsn(dsn: str) -> str:
    parsed = urlparse(dsn)
    if not parsed.netloc or "@" not in parsed.netloc:
        return dsn
    creds, host_part = parsed.netloc.rsplit("@", 1)
    username = creds.split(":", 1)[0] if creds else "user"
    masked_netloc = f"{username}:***@{host_part}"
    return parsed._replace(netloc=masked_netloc).geturl()


def fetch_node_metrics(node: NodeConfig, target_db: str) -> dict[str, Any]:
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
        "active_queries": None,
        "blk_read_time_ms": None,
        "blk_write_time_ms": None,
        "disk_io_queue": None,
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
                        (
                            SELECT count(*)
                            FROM pg_locks l
                            JOIN pg_stat_activity a ON a.pid = l.pid
                            WHERE l.granted AND a.datname = %s
                        ),
                        (SELECT xact_commit FROM pg_stat_database WHERE datname = %s),
                        (SELECT xact_rollback FROM pg_stat_database WHERE datname = %s),
                        (SELECT blks_read FROM pg_stat_database WHERE datname = %s),
                        (SELECT blks_hit FROM pg_stat_database WHERE datname = %s),
                        (SELECT tup_returned FROM pg_stat_database WHERE datname = %s),
                        (SELECT tup_fetched FROM pg_stat_database WHERE datname = %s),
                        (SELECT count(*) FROM pg_stat_activity WHERE datname = %s AND state = 'active'),
                        (SELECT COALESCE(blk_read_time, 0) FROM pg_stat_database WHERE datname = %s),
                        (SELECT COALESCE(blk_write_time, 0) FROM pg_stat_database WHERE datname = %s),
                        (
                            SELECT count(*)
                            FROM pg_stat_activity
                            WHERE datname = %s AND wait_event_type = 'IO'
                        )
                    """,
                    (target_db, target_db, target_db, target_db, target_db, target_db, target_db, target_db, target_db, target_db, target_db),
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
                            "active_queries": row[9],
                            "blk_read_time_ms": row[10],
                            "blk_write_time_ms": row[11],
                            "disk_io_queue": row[12],
                        }
                    )
    except Exception as exc:
        result["error"] = str(exc)
        LOGGER.exception(
            "DB metrics fetch failed for node=%s target_db=%s dsn=%s",
            node.name,
            target_db,
            mask_dsn(node.dsn),
        )

    return result


def fetch_all_node_metrics(nodes: list[NodeConfig], target_db: str) -> list[dict[str, Any]]:
    if not nodes:
        return []

    worker_count = max(1, min(METRICS_FETCH_WORKERS, len(nodes)))
    rows_by_name: dict[str, dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_to_node = {executor.submit(fetch_node_metrics, node, target_db): node for node in nodes}
        for future in as_completed(future_to_node):
            node = future_to_node[future]
            try:
                rows_by_name[node.name] = future.result()
            except Exception as exc:
                rows_by_name[node.name] = {
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
                    "active_queries": None,
                    "blk_read_time_ms": None,
                    "blk_write_time_ms": None,
                    "disk_io_queue": None,
                    "error": str(exc),
                }

    return [rows_by_name[node.name] for node in nodes if node.name in rows_by_name]


def run_node_action(node: NodeConfig, action: str) -> tuple[bool, str]:
    if not node.control_via_ssh or not node.ssh_host:
        return False, "SSH control disabled for this node in config"

    user_prefix = f"{node.ssh_user}@" if node.ssh_user else ""
    if action == "stop":
        remote_cmd = f"sudo -n systemctl stop {shlex.quote(node.service_name)}"
    elif action == "start":
        remote_cmd = f"sudo -n systemctl start {shlex.quote(node.service_name)}"
    elif action == "restart":
        remote_cmd = f"sudo -n systemctl restart {shlex.quote(node.service_name)}"
    else:
        return False, f"Unknown action: {action}"

    cmd = [
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        "StrictHostKeyChecking=accept-new",
        "-o",
        "ConnectTimeout=5",
        "-p",
        str(node.ssh_port),
    ]

    if node.ssh_identity_file:
        cmd.extend(["-i", node.ssh_identity_file])

    if node.ssh_legacy_algorithms:
        cmd.extend(
            [
                "-o",
                "HostKeyAlgorithms=+ssh-rsa",
                "-o",
                "PubkeyAcceptedAlgorithms=+ssh-rsa",
                "-o",
                "KexAlgorithms=+diffie-hellman-group14-sha1",
            ]
        )

    if node.ssh_extra_options:
        for opt in node.ssh_extra_options:
            cmd.extend(["-o", opt])

    cmd.extend(
        [
            f"{user_prefix}{node.ssh_host}",
            remote_cmd,
        ]
    )
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
    except subprocess.TimeoutExpired:
        LOGGER.error("SSH command timeout for node=%s action=%s host=%s", node.name, action, node.ssh_host)
        return False, "SSH command timed out after 15 seconds"
    ok = proc.returncode == 0
    output = (proc.stdout + "\n" + proc.stderr).strip()
    if ok:
        LOGGER.info("SSH action succeeded for node=%s action=%s host=%s", node.name, action, node.ssh_host)
    else:
        LOGGER.error(
            "SSH action failed for node=%s action=%s host=%s returncode=%s output=%s",
            node.name,
            action,
            node.ssh_host,
            proc.returncode,
            output or "<empty>",
        )
    return ok, output or "OK"


def render_sidebar() -> dict[str, Any]:
    st.sidebar.header("Профиль нагрузки (Load profile)")
    mode = st.sidebar.selectbox(
        "Режим (Mode)",
        options=["single-node", "dual-read", "master-rw-slave-r"],
        help="single-node: вся нагрузка на master; dual-read: чтение с обеих; master-rw-slave-r: запись на master, чтение с slave",
    )
    tps = st.sidebar.slider("TPS", min_value=0, max_value=50000, value=500, step=100)
    read_ratio = st.sidebar.slider("Доля чтения (Read ratio)", 0.0, 1.0, 0.7, 0.05)
    auto_refresh = st.sidebar.checkbox("Автообновление (Auto-refresh)", value=True)
    st.session_state.load_mode = mode
    return {"mode": mode, "tps": tps, "read_ratio": read_ratio, "auto_refresh": auto_refresh}


def apply_compact_top_styles() -> None:
    st.markdown(
        """
        <style>
            div.block-container {
                padding-top: 1.2rem;
            }

            h1 {
                font-size: 2.2rem !important;
                margin-bottom: 0.1rem !important;
            }

            h3 {
                font-size: 2rem !important;
                margin-top: 0.8rem !important;
                margin-bottom: 0.4rem !important;
            }

            .stCaption {
                font-size: 0.9rem !important;
                margin-bottom: 0.2rem !important;
            }

            div[data-testid="stTextInput"] label p,
            div[data-testid="stSlider"] label p,
            div[data-testid="stSelectbox"] label p,
            div[data-testid="stCheckbox"] label p {
                font-size: 0.92rem !important;
            }

            div[data-testid="stTextInput"] input {
                font-size: 0.95rem !important;
                padding-top: 0.35rem !important;
                padding-bottom: 0.35rem !important;
            }

            div[data-testid="stButton"] button {
                font-size: 0.92rem !important;
                min-height: 2.2rem !important;
                padding: 0.3rem 0.6rem !important;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_controls(cluster: ClusterConfig, wg: WorkloadGenerator, profile: dict[str, Any]) -> None:
    st.subheader("Управление сценарием (Scenario controls)")
    col1, col2, col3, col4, col5 = st.columns(5)

    if col1.button("Запустить нагрузку (Start load)", type="primary", width="stretch"):
        wg.start(cluster, profile["mode"], int(profile["tps"]), float(profile["read_ratio"]))
    if col2.button("Остановить нагрузку (Stop load)", width="stretch"):
        wg.stop()
    if col3.button("Сбросить счётчики (Reset counters)", width="stretch"):
        wg.reset_stats()
    if col4.button("Сбросить серверную статистику (Reset server stats)", width="stretch"):
        reset_server_stats(cluster)
    if col5.button("Обновить сейчас (Refresh now)", width="stretch"):
        st.rerun()

    st.caption(f"Состояние генератора (Generator state): {'RUNNING' if wg.running else 'STOPPED'}")
    st.markdown("#### Симуляция отказа (Failure simulation)")

    for node in cluster.nodes:
        c1, c2, c3, c4 = st.columns([2, 1, 1, 1])
        c1.write(f"**{node.name}** ({node.role_hint})")
        if c2.button(f"Остановить (Stop) {node.name}", key=f"stop-{node.name}"):
            ok, msg = run_node_action(node, "stop")
            st.toast(f"{node.name} stop: {'OK' if ok else 'ERR'} | {msg}")
        if c3.button(f"Запустить (Start) {node.name}", key=f"start-{node.name}"):
            ok, msg = run_node_action(node, "start")
            st.toast(f"{node.name} start: {'OK' if ok else 'ERR'} | {msg}")
        if c4.button(f"Перезапустить (Restart) {node.name}", key=f"restart-{node.name}"):
            ok, msg = run_node_action(node, "restart")
            st.toast(f"{node.name} restart: {'OK' if ok else 'ERR'} | {msg}")


def reset_server_stats(cluster: ClusterConfig) -> None:
    failures: list[str] = []
    for node in cluster.nodes:
        try:
            with psycopg.connect(node.dsn, connect_timeout=2, autocommit=True) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT pg_stat_reset()")
        except Exception as exc:
            failures.append(f"{node.name}: {exc}")

    if failures:
        st.warning("Не удалось сбросить статистику (Failed to reset stats): " + "; ".join(failures))
    else:
        st.success("Серверная статистика сброшена (Server statistics reset).")


def render_metrics(cluster: ClusterConfig, wg: WorkloadGenerator) -> None:
    st.subheader("Состояние кластера (Cluster state)")
    mode = st.session_state.get("load_mode", "single-node")
    target_db = get_target_database(cluster, mode)
    rows = fetch_all_node_metrics(cluster.nodes, target_db)
    df = pd.DataFrame(rows)
    if not df.empty:
        df.insert(
            0,
            "status_icon",
            df["status"].map({"up": "🟢", "down": "🔴"}).fillna("⚪"),
        )
    if not df.empty:
        df["status"] = df["status"].map({"up": "РАБОТАЕТ (UP)", "down": "НЕ РАБОТАЕТ (DOWN)"}).fillna(df["status"])
    localized_df = df.rename(
        columns={
            "status_icon": "Сост. (State)",
            "node": "Узел (Node)",
            "status": "Статус (Status)",
            "role": "Роль (Role)",
            "replay_delay_sec": "Задержка реплея, с (Replay delay, sec)",
            "active_locks": "Активные блокировки (Active locks)",
            "active_queries": "Активные запросы (Active queries)",
            "xact_commit": "Подтверждённые транзакции (Committed tx)",
            "xact_rollback": "Откаты транзакций (Rolled back tx)",
            "blks_read": "Блоков с диска (Blocks read)",
            "blks_hit": "Попаданий в кэш (Cache hits)",
            "tup_returned": "Возвращено строк (Rows returned)",
            "tup_fetched": "Извлечено строк (Rows fetched)",
            "blk_read_time_ms": "Задержка чтения диска, мс (Disk read latency, ms)",
            "blk_write_time_ms": "Задержка записи диска, мс (Disk write latency, ms)",
            "disk_io_queue": "Очередь к диску (Disk queue)",
            "error": "Ошибка (Error)",
        }
    )
    st.caption(f"Целевая БД нагрузки (Target DB): {target_db}")
    st.dataframe(localized_df, width="stretch")
    if rows and all(row.get("status") != "up" for row in rows):
        LOGGER.warning(
            "No DB connections to any node. Errors: %s",
            {row.get("node"): row.get("error") for row in rows},
        )
        st.warning("Нет подключений к узлам БД. Проверьте доступность PostgreSQL и параметры DSN/SSH в конфиге.")
    stats = wg.stats_snapshot()
    st.info(f"Статус генератора нагрузки (Load generator status): {'🟢 РАБОТАЕТ' if wg.running else '🔴 ОСТАНОВЛЕН'}")

    st.subheader("Статистика нагрузки (Load stats)")
    stat_cols = st.columns(3)
    stat_cols[0].metric("Транзакции чтения", f"{stats['read_tx']:,}".replace(",", " "))
    stat_cols[1].metric("Транзакции записи", f"{stats['write_tx']:,}".replace(",", " "))
    stat_cols[2].metric("Ошибки", f"{stats['errors']:,}".replace(",", " "))

    if "history" not in st.session_state:
        st.session_state.history = []

    history = st.session_state.history
    history.append(
        {
            "ts": pd.Timestamp.now(tz=MOSCOW_TZ),
            "read_tx": stats["read_tx"],
            "write_tx": stats["write_tx"],
            "errors": stats["errors"],
            "active_locks": int(pd.to_numeric(df["active_locks"], errors="coerce").fillna(0).sum()) if not df.empty else 0,
            "active_queries": int(pd.to_numeric(df["active_queries"], errors="coerce").fillna(0).sum()) if not df.empty else 0,
            **{f"blks_read_{row['node']}": int(row.get("blks_read") or 0) for row in rows},
            **{f"disk_read_latency_{row['node']}": float(row.get("blk_read_time_ms") or 0.0) for row in rows},
            **{f"disk_write_latency_{row['node']}": float(row.get("blk_write_time_ms") or 0.0) for row in rows},
            **{f"disk_queue_{row['node']}": int(row.get("disk_io_queue") or 0) for row in rows},
        }
    )

    if len(history) > DEFAULT_HISTORY:
        del history[:-DEFAULT_HISTORY]

    hist_df = pd.DataFrame(history)
    if not hist_df.empty:
        st.line_chart(hist_df.set_index("ts")[["read_tx", "write_tx", "errors"]])
        st.line_chart(hist_df.set_index("ts")[["active_locks", "active_queries"]])

        st.markdown("##### Нагрузка на диски по узлам (Disk load per node)")
        disk_cols: list[str] = []
        for node in cluster.nodes:
            disk_cols.extend(
                [
                    f"disk_read_latency_{node.name}",
                    f"disk_write_latency_{node.name}",
                    f"disk_queue_{node.name}",
                ]
            )
        disk_cols = [col for col in disk_cols if col in hist_df.columns]
        if disk_cols:
            st.line_chart(hist_df.set_index("ts")[disk_cols])


def main() -> None:
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    apply_compact_top_styles()
    st.title(APP_TITLE)
    st.caption("Демо-интерфейс для проверки кластера BiHA PostgreSQL Pro (Demo GUI for BiHA PostgreSQL Pro cluster validation)")

    cfg_path = Path(st.text_input("Путь к конфигу (Path to config)", "config/cluster.example.json"))
    if not cfg_path.exists():
        st.error(f"Конфиг не найден (Config not found): {cfg_path}")
        st.stop()

    try:
        cluster = load_cluster_config(cfg_path)
    except Exception as exc:
        LOGGER.exception("Failed to parse config file: %s", cfg_path)
        st.error(f"Не удалось прочитать конфиг (Cannot parse config): {exc}")
        st.stop()

    LOGGER.info("App started with config_path=%s nodes=%s", cfg_path, len(cluster.nodes))

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
