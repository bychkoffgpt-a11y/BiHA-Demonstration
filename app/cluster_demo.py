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
from psycopg.conninfo import conninfo_to_dict
import streamlit as st

from logging_utils import setup_file_logger
from ui_styles import apply_base_page_styles
from workload_status_store import write_workload_status
from workload_profiles import PgLikeSizing, estimate_pg_like_sizing, run_pg_like_tx

APP_TITLE = "BiHA PostgreSQL Cluster Demo"
DEFAULT_HISTORY = 120
DEFAULT_WORKLOAD_DB_SIZE_GB = 1.0
MAX_WORKERS = 64
METRICS_FETCH_WORKERS = 8
MOSCOW_TZ = ZoneInfo("Europe/Moscow")
LOGGER = setup_file_logger()
WORKLOAD_LOCK_TIMEOUT_MS = 1_500
WORKLOAD_STATEMENT_TIMEOUT_MS = 8_000
PG_QUERY_CANCELED_SQLSTATE = "57014"


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
    collect_disk_metrics_via_ssh: bool = True
    disk_device: str | None = None


@dataclass
class ClusterConfig:
    nodes: list[NodeConfig]
    poll_interval_sec: int = 2


class WorkloadGenerator:
    """Фоновый генератор pgbench-подобной нагрузки с динамическим масштабированием."""

    @dataclass
    class _WorkerSlot:
        thread: threading.Thread
        stop_event: threading.Event

    def __init__(self) -> None:
        self._workers: list[WorkloadGenerator._WorkerSlot] = []
        self._stop_event = threading.Event()
        self._manager_thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._settings_lock = threading.Lock()
        self._stats: dict[str, int] = {"read_tx": 0, "write_tx": 0, "errors": 0}
        self._recent_errors: list[dict[str, Any]] = []
        self._cluster: ClusterConfig | None = None
        self._mode: str = "rw-master"
        self._read_ratio: float = 0.7
        self._desired_workers: int = 1
        self._sizing: PgLikeSizing = estimate_pg_like_sizing(DEFAULT_WORKLOAD_DB_SIZE_GB)
        self._last_status_flush_monotonic: float = 0.0

    @property
    def running(self) -> bool:
        return self._manager_thread is not None and self._manager_thread.is_alive()

    def stats_snapshot(self) -> dict[str, int]:
        with self._lock:
            return dict(self._stats)

    def reset_stats(self) -> None:
        with self._lock:
            for key in self._stats:
                self._stats[key] = 0
            self._recent_errors = []

    def recent_errors_snapshot(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._recent_errors)

    def start(self, cluster: ClusterConfig, mode: str, clients: int, threads_per_client: int, read_ratio: float) -> None:
        self.update_settings(cluster, mode, clients, threads_per_client, read_ratio)
        if self.running:
            return
        self._stop_event.clear()
        self._write_status(is_running=True)

        self._manager_thread = threading.Thread(target=self._manage_workers, daemon=True, name="workload-manager")
        self._manager_thread.start()

    def update_settings(
        self, cluster: ClusterConfig, mode: str, clients: int, threads_per_client: int, read_ratio: float
    ) -> None:
        target_size_gb = float(st.session_state.get("target_size_gb", DEFAULT_WORKLOAD_DB_SIZE_GB))
        desired_workers = max(1, min(MAX_WORKERS, clients * threads_per_client))
        with self._settings_lock:
            self._cluster = cluster
            self._mode = mode
            self._read_ratio = read_ratio
            self._desired_workers = desired_workers
            self._sizing = estimate_pg_like_sizing(target_size_gb)
        self._write_status(
            is_running=self.running,
            mode=mode,
            clients=clients,
            threads_per_client=threads_per_client,
            read_ratio=read_ratio,
        )

    def stop(self) -> None:
        self._stop_event.set()
        if self._manager_thread:
            self._manager_thread.join(timeout=2)
        self._manager_thread = None
        self._stop_all_workers()
        self._write_status(is_running=False)

    def _manage_workers(self) -> None:
        while not self._stop_event.is_set():
            self._sync_workers()
            now = time.monotonic()
            if now - self._last_status_flush_monotonic >= 1.0:
                self._write_status(is_running=True)
                self._last_status_flush_monotonic = now
            self._stop_event.wait(0.2)
        self._stop_all_workers()

    def _stop_all_workers(self) -> None:
        workers = list(self._workers)
        self._workers = []
        for worker in workers:
            worker.stop_event.set()
        for worker in workers:
            worker.thread.join(timeout=2)

    def _sync_workers(self) -> None:
        alive_workers: list[WorkloadGenerator._WorkerSlot] = []
        for worker in self._workers:
            if worker.thread.is_alive():
                alive_workers.append(worker)
        self._workers = alive_workers

        with self._settings_lock:
            desired_workers = self._desired_workers

        while len(self._workers) > desired_workers:
            worker = self._workers.pop()
            worker.stop_event.set()
            worker.thread.join(timeout=2)

        while len(self._workers) < desired_workers and not self._stop_event.is_set():
            worker_stop_event = threading.Event()
            thread = threading.Thread(target=self._run_worker, args=(worker_stop_event,), daemon=True)
            slot = WorkloadGenerator._WorkerSlot(thread=thread, stop_event=worker_stop_event)
            self._workers.append(slot)
            thread.start()

    def _run_worker(self, worker_stop_event: threading.Event) -> None:
        while not self._stop_event.is_set() and not worker_stop_event.is_set():
            node: NodeConfig | None = None
            write_tx = False
            mode = "rw-master"
            try:
                with self._settings_lock:
                    cluster = self._cluster
                    mode = self._mode
                    read_ratio = self._read_ratio
                    sizing = self._sizing
                if cluster is None:
                    raise RuntimeError("Cluster config is not initialized")

                read_only_modes = {"r-master", "r-master-r-slave"}
                write_tx = False if mode in read_only_modes else random.random() > read_ratio
                node = select_node_for_workload(cluster.nodes, mode, write_tx)
                if not node:
                    raise RuntimeError("No available node for selected mode")
                try:
                    execute_workload_tx(node, write_tx, sizing)
                except Exception as exc:
                    fallback_node = pick_master_node(cluster.nodes)
                    should_retry_on_master = (
                        not write_tx
                        and fallback_node is not None
                        and fallback_node.name != node.name
                        and is_recovery_conflict_error(exc)
                    )
                    if not should_retry_on_master:
                        raise

                    LOGGER.info(
                        "Retrying read transaction on master after replica recovery conflict | "
                        "mode=%s failed_node=%s fallback_node=%s",
                        mode,
                        node.name,
                        fallback_node.name,
                    )
                    node = fallback_node
                    execute_workload_tx(node, write_tx, sizing)
                with self._lock:
                    self._stats["write_tx" if write_tx else "read_tx"] += 1
            except Exception as exc:
                error_message = format_workload_error(exc)
                node_name = node.name if node else "unknown"
                tx_type = "write" if write_tx else "read"
                LOGGER.error(
                    "Workload transaction failed | mode=%s node=%s role_hint=%s tx_type=%s error=%s",
                    mode,
                    node_name,
                    node.role_hint if node else "unknown",
                    tx_type,
                    error_message,
                )
                with self._lock:
                    self._stats["errors"] += 1
                    self._recent_errors.append(
                        {
                            "ts": pd.Timestamp.now(tz=MOSCOW_TZ),
                            "node": node_name,
                            "tx_type": tx_type,
                            "error": error_message,
                        }
                    )
                    if len(self._recent_errors) > 20:
                        del self._recent_errors[:-20]
            time.sleep(0.001)

    def _write_status(
        self,
        *,
        is_running: bool,
        mode: str | None = None,
        clients: int | None = None,
        threads_per_client: int | None = None,
        read_ratio: float | None = None,
    ) -> None:
        with self._settings_lock:
            current_mode = self._mode if mode is None else mode
            desired_workers = self._desired_workers
            current_read_ratio = self._read_ratio if read_ratio is None else read_ratio

        payload: dict[str, Any] = {
            "is_running": is_running,
            "mode": current_mode,
            "clients": clients,
            "threads_per_client": threads_per_client,
            "read_ratio": current_read_ratio,
            "requested_threads": desired_workers,
            "updated_at": pd.Timestamp.now(tz=MOSCOW_TZ).isoformat(),
        }
        if payload["clients"] is None:
            payload.pop("clients")
        if payload["threads_per_client"] is None:
            payload.pop("threads_per_client")
        write_workload_status(payload)


class BackgroundMetricsCollector:
    """Фоновый сборщик метрик кластера."""

    def __init__(self) -> None:
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()
        self._mode: str = "rw-master"
        self._cluster_signature: tuple[Any, ...] | None = None
        self._latest_rows: list[dict[str, Any]] = []
        self._latest_target_db: str = "postgres"
        self._last_update_ts: pd.Timestamp | None = None
        self._last_error: str | None = None
        self._history: list[dict[str, Any]] = []

    @property
    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    @property
    def cluster_signature(self) -> tuple[Any, ...] | None:
        with self._lock:
            return self._cluster_signature

    def start(self, cluster: ClusterConfig, wg: WorkloadGenerator, mode: str) -> None:
        self.stop()
        with self._lock:
            self._mode = mode
            self._cluster_signature = build_cluster_signature(cluster)
            self._latest_rows = []
            self._latest_target_db = get_target_database(cluster, mode)
            self._last_update_ts = None
            self._last_error = None
            self._history = []

        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, args=(cluster, wg), daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=2)
        self._thread = None

    def set_mode(self, mode: str) -> None:
        with self._lock:
            self._mode = mode

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "rows": list(self._latest_rows),
                "target_db": self._latest_target_db,
                "updated_at": self._last_update_ts,
                "error": self._last_error,
            }

    def history_snapshot(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._history)

    def _run(self, cluster: ClusterConfig, wg: WorkloadGenerator) -> None:
        while not self._stop_event.is_set():
            try:
                with self._lock:
                    mode = self._mode

                target_db = get_target_database(cluster, mode)
                rows = fetch_all_node_metrics(cluster.nodes, target_db)
                stats = wg.stats_snapshot()

                history_entry = {
                    "ts": pd.Timestamp.now(tz=MOSCOW_TZ),
                    "read_tx": stats["read_tx"],
                    "write_tx": stats["write_tx"],
                    "errors": stats["errors"],
                    "active_locks": int(sum(int(row.get("active_locks") or 0) for row in rows)),
                    "active_queries": int(sum(int(row.get("active_queries") or 0) for row in rows)),
                    **{f"blks_read_{row['node']}": int(row.get("blks_read") or 0) for row in rows},
                    **{f"disk_read_latency_{row['node']}": float(row.get("blk_read_time_ms") or 0.0) for row in rows},
                    **{f"disk_write_latency_{row['node']}": float(row.get("blk_write_time_ms") or 0.0) for row in rows},
                    **{f"disk_queue_{row['node']}": int(row.get("disk_io_queue") or 0) for row in rows},
                    **{f"disk_read_kb_s_{row['node']}": float(row.get("disk_read_kb_s_os") or 0.0) for row in rows},
                    **{f"disk_write_kb_s_{row['node']}": float(row.get("disk_write_kb_s_os") or 0.0) for row in rows},
                    **{f"disk_util_pct_{row['node']}": float(row.get("disk_util_pct_os") or 0.0) for row in rows},
                }

                with self._lock:
                    self._latest_rows = rows
                    self._latest_target_db = target_db
                    self._last_update_ts = history_entry["ts"]
                    self._last_error = None
                    self._history.append(history_entry)
                    if len(self._history) > DEFAULT_HISTORY:
                        del self._history[:-DEFAULT_HISTORY]
            except Exception as exc:
                LOGGER.exception("Ошибка фонового сбора метрик")
                with self._lock:
                    self._last_error = str(exc)

            self._stop_event.wait(cluster.poll_interval_sec)


def build_cluster_signature(cluster: ClusterConfig) -> tuple[Any, ...]:
    return (
        cluster.poll_interval_sec,
        tuple((node.name, node.dsn, node.role_hint, node.control_via_ssh, node.ssh_host) for node in cluster.nodes),
    )


def select_node_for_workload(nodes: list[NodeConfig], mode: str, write_tx: bool) -> NodeConfig | None:
    master_hints = {"master", "primary", "leader"}
    slave_hints = {"slave", "replica", "standby"}

    masters = [n for n in nodes if n.role_hint.strip().lower() in master_hints]
    slaves = [n for n in nodes if n.role_hint.strip().lower() in slave_hints]

    fallback_node = nodes[0] if nodes else None

    if mode in {"single-node", "master-rw", "rw-master", "r-master"}:
        return masters[0] if masters else fallback_node

    if mode in {"dual-read", "r-master-r-slave"}:
        readable_nodes = masters + slaves
        if readable_nodes:
            return random.choice(readable_nodes)
        return random.choice(nodes) if nodes else fallback_node

    if mode in {"master-rw-slave-r", "rw-master-r-slave"}:
        if write_tx:
            return masters[0] if masters else fallback_node
        readable_nodes = masters + slaves
        if readable_nodes:
            return random.choice(readable_nodes)
        if masters:
            return masters[0]
        return fallback_node

    return fallback_node


def pick_master_node(nodes: list[NodeConfig]) -> NodeConfig | None:
    master_hints = {"master", "primary", "leader"}
    masters = [n for n in nodes if n.role_hint.strip().lower() in master_hints]
    if masters:
        return masters[0]
    return nodes[0] if nodes else None


def execute_workload_tx(node: NodeConfig, write_tx: bool, sizing: PgLikeSizing) -> None:
    with psycopg.connect(node.dsn, connect_timeout=2, autocommit=False) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SET LOCAL lock_timeout = '{WORKLOAD_LOCK_TIMEOUT_MS}ms'")
            cur.execute(f"SET LOCAL statement_timeout = '{WORKLOAD_STATEMENT_TIMEOUT_MS}ms'")
            cur.execute("SET LOCAL idle_in_transaction_session_timeout = '10s'")
        run_pg_like_tx(conn, write_tx, sizing)
        conn.commit()


def format_workload_error(exc: Exception) -> str:
    sqlstate = getattr(exc, "sqlstate", None)
    detail = getattr(getattr(exc, "diag", None), "message_detail", None)
    hint = getattr(getattr(exc, "diag", None), "message_hint", None)

    parts = [str(exc)]
    if sqlstate:
        parts.append(f"sqlstate={sqlstate}")
    if detail:
        parts.append(f"detail={detail}")
    if hint:
        parts.append(f"hint={hint}")
    return " | ".join(parts)


def is_recovery_conflict_error(exc: Exception) -> bool:
    sqlstate = getattr(exc, "sqlstate", None)
    if sqlstate != PG_QUERY_CANCELED_SQLSTATE:
        return False

    error_text = str(exc).lower()
    detail = str(getattr(getattr(exc, "diag", None), "message_detail", "") or "").lower()
    combined = f"{error_text} {detail}"
    keywords = ("conflict with recovery", "конфликт", "восстановлен")
    return any(keyword in combined for keyword in keywords)


def extract_dbname_from_dsn(dsn: str) -> str:
    try:
        conninfo = conninfo_to_dict(dsn)
    except Exception:
        conninfo = {}

    dbname = conninfo.get("dbname")
    if dbname:
        return dbname

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
    if not path.exists():
        raise FileNotFoundError(f"Config not found: {path}")
    if not path.is_file():
        raise ValueError(f"Expected a JSON config file, got a directory: {path}")
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
        "disk_read_kb_s_os": None,
        "disk_write_kb_s_os": None,
        "disk_util_pct_os": None,
        "tx_read_only": None,
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
                        ),
                        current_setting('transaction_read_only')
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
                            "tx_read_only": row[13],
                        }
                    )

        if node.collect_disk_metrics_via_ssh:
            result.update(fetch_disk_metrics_via_ssh(node))
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
                    "disk_read_kb_s_os": None,
                    "disk_write_kb_s_os": None,
                    "disk_util_pct_os": None,
                    "tx_read_only": None,
                    "error": str(exc),
                }

    return [rows_by_name[node.name] for node in nodes if node.name in rows_by_name]


def build_ssh_command(node: NodeConfig, remote_cmd: str) -> list[str]:
    user_prefix = f"{node.ssh_user}@" if node.ssh_user else ""
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

    cmd.extend([f"{user_prefix}{node.ssh_host}", remote_cmd])
    return cmd


def fetch_disk_metrics_via_ssh(node: NodeConfig) -> dict[str, float | int | None]:
    default_metrics = {
        "disk_io_queue": None,
        "disk_read_kb_s_os": None,
        "disk_write_kb_s_os": None,
        "disk_util_pct_os": None,
    }
    if not node.control_via_ssh or not node.ssh_host:
        return default_metrics

    remote_cmd = 'bash -lc "LC_ALL=C iostat -dx 1 2"'
    cmd = build_ssh_command(node, remote_cmd)
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=8)
    except Exception:
        return default_metrics

    if proc.returncode != 0:
        return default_metrics

    raw_output = proc.stdout.strip()
    if not raw_output:
        return default_metrics
    sections = [section.strip() for section in raw_output.split("\n\n") if section.strip() and "Device" in section]
    if not sections:
        return default_metrics

    lines = [line.strip() for line in sections[-1].splitlines() if line.strip()]
    header_idx = next((idx for idx, line in enumerate(lines) if line.startswith("Device")), None)
    if header_idx is None or header_idx + 1 >= len(lines):
        return default_metrics

    headers = lines[header_idx].split()
    device_rows = [line.split() for line in lines[header_idx + 1 :] if not line.startswith("avg-cpu")]
    if not device_rows:
        return default_metrics

    if node.disk_device:
        requested_device = node.disk_device.strip()
        requested_aliases = {requested_device, requested_device.removeprefix("/dev/")}
        filtered_rows = [row for row in device_rows if row and row[0] in requested_aliases]
        if not filtered_rows:
            return default_metrics
        device_rows = filtered_rows

    totals = {
        "disk_read_kb_s_os": 0.0,
        "disk_write_kb_s_os": 0.0,
        "disk_io_queue": 0.0,
        "disk_util_pct_os": 0.0,
    }
    found = {key: False for key in totals}

    def parse_metric(values_by_header: dict[str, str], candidates: list[str]) -> tuple[float | None, str | None]:
        for candidate in candidates:
            value = values_by_header.get(candidate)
            if value is None:
                continue
            try:
                return float(value), candidate
            except ValueError:
                continue
        return None, None

    for row in device_rows:
        if len(row) < len(headers):
            continue
        values_by_header = dict(zip(headers[1:], row[1:]))

        read_kb, read_key = parse_metric(values_by_header, ["rkB/s", "rKB/s", "kB_read/s", "rMB/s"])
        write_kb, write_key = parse_metric(values_by_header, ["wkB/s", "wKB/s", "kB_wrtn/s", "wMB/s"])
        queue, _ = parse_metric(values_by_header, ["aqu-sz", "avgqu-sz"])
        util, _ = parse_metric(values_by_header, ["%util", "util"])

        if read_kb is not None:
            if read_key == "rMB/s":
                read_kb *= 1024
            totals["disk_read_kb_s_os"] += read_kb
            found["disk_read_kb_s_os"] = True
        if write_kb is not None:
            if write_key == "wMB/s":
                write_kb *= 1024
            totals["disk_write_kb_s_os"] += write_kb
            found["disk_write_kb_s_os"] = True
        if queue is not None:
            totals["disk_io_queue"] += queue
            found["disk_io_queue"] = True
        if util is not None:
            totals["disk_util_pct_os"] += util
            found["disk_util_pct_os"] = True

    read_kb = totals["disk_read_kb_s_os"] if found["disk_read_kb_s_os"] else None
    write_kb = totals["disk_write_kb_s_os"] if found["disk_write_kb_s_os"] else None
    queue = totals["disk_io_queue"] if found["disk_io_queue"] else None
    util = totals["disk_util_pct_os"] if found["disk_util_pct_os"] else None

    if read_kb is None and write_kb is None and queue is None and util is None:
        return default_metrics

    return {
        "disk_read_kb_s_os": read_kb,
        "disk_write_kb_s_os": write_kb,
        "disk_io_queue": queue,
        "disk_util_pct_os": util,
    }


def format_cluster_table_value(value: Any, decimals: int = 2) -> Any:
    if value is None:
        return ""
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return f"{value:,}".replace(",", " ")
    if isinstance(value, float):
        return f"{value:,.{decimals}f}".replace(",", " ")
    return value


def run_node_action(node: NodeConfig, action: str) -> tuple[bool, str]:
    if not node.control_via_ssh or not node.ssh_host:
        return False, "SSH control disabled for this node in config"

    if action == "stop":
        remote_cmd = f"sudo -n systemctl stop {shlex.quote(node.service_name)}"
    elif action == "start":
        remote_cmd = f"sudo -n systemctl start {shlex.quote(node.service_name)}"
    elif action == "restart":
        remote_cmd = f"sudo -n systemctl restart {shlex.quote(node.service_name)}"
    else:
        return False, f"Unknown action: {action}"

    cmd = build_ssh_command(node, remote_cmd)
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
    except subprocess.TimeoutExpired:
        LOGGER.error("SSH action timeout for node=%s action=%s host=%s", node.name, action, node.ssh_host)
        return False, "SSH command timeout"

    output = (proc.stdout + "\n" + proc.stderr).strip()
    ok = proc.returncode == 0
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


def render_pending_service_operation_messages() -> None:
    pending_messages = st.session_state.pop("pending_service_operation_messages", [])
    for level, message in pending_messages:
        notifier = getattr(st, level, None)
        if callable(notifier):
            notifier(message)
        else:
            st.info(message)


def request_reset_caches() -> None:
    st.session_state["confirm_reset_caches_inline"] = False
    st.session_state["pending_reset_caches_action"] = True
    st.rerun()


def run_pending_service_operations(cluster: ClusterConfig) -> None:
    if st.session_state.pop("pending_reset_caches_action", False):
        with st.spinner("Очищаем page cache/dentries/inodes на нодах кластера..."):
            messages = reset_all_node_caches(cluster)
        if not messages:
            messages = [("info", "Очистка кэшей завершена, но узлы для обработки не найдены.")]
        st.session_state["pending_service_operation_messages"] = messages


def open_reset_counters_dialog(cluster: ClusterConfig, wg: WorkloadGenerator) -> None:
    if not st.session_state.get("confirm_reset_counters_inline", False):
        return

    if not hasattr(st, "dialog"):
        with st.sidebar.container(border=True):
            st.warning(
                "Подтвердите сброс серверной статистики на всех узлах кластера. "
                "Будут предприняты попытки очистки счётчиков pg_stat_database и связанных shared статистик."
            )
            confirm_cols = st.columns(2)
            if confirm_cols[0].button("✅ Да, сбросить", key="confirm_reset_yes", use_container_width=True):
                wg.reset_stats()
                st.session_state["pending_service_operation_messages"] = reset_server_stats(cluster)
                st.session_state["confirm_reset_counters_inline"] = False
                st.rerun()
            if confirm_cols[1].button("❌ Отмена", key="confirm_reset_no", use_container_width=True):
                st.session_state["confirm_reset_counters_inline"] = False
                st.rerun()
        return

    @st.dialog("Сбросить счётчики?")
    def _dialog() -> None:
        st.warning(
            "Будут сброшены серверные счётчики на всех узлах кластера, "
            "включая статистику pg_stat_database и shared statistics."
        )
        confirm_cols = st.columns(2)
        if confirm_cols[0].button("✅ Да, сбросить", key="confirm_reset_yes_dialog", use_container_width=True):
            wg.reset_stats()
            st.session_state["pending_service_operation_messages"] = reset_server_stats(cluster)
            st.session_state["confirm_reset_counters_inline"] = False
            st.rerun()
        if confirm_cols[1].button("❌ Отмена", key="confirm_reset_no_dialog", use_container_width=True):
            st.session_state["confirm_reset_counters_inline"] = False
            st.rerun()

    _dialog()


def open_reset_caches_dialog(cluster: ClusterConfig) -> None:
    if not st.session_state.get("confirm_reset_caches_inline", False):
        return

    if not hasattr(st, "dialog"):
        with st.sidebar.container(border=True):
            st.warning(
                "Подтвердите очистку page cache/dentries/inodes на всех нодах кластера. "
                "Операция требует sudo без пароля и может временно ухудшить производительность."
            )
            confirm_cols = st.columns(2)
            if confirm_cols[0].button("✅ Да, очистить кэши", key="confirm_reset_caches_yes", use_container_width=True):
                request_reset_caches()
            if confirm_cols[1].button("❌ Отмена", key="confirm_reset_caches_no", use_container_width=True):
                st.session_state["confirm_reset_caches_inline"] = False
                st.rerun()
        return

    @st.dialog("Очистить кэши?")
    def _dialog() -> None:
        st.warning(
            "Будет выполнена очистка page cache/dentries/inodes на всех нодах кластера. "
            "Операция требует sudo без пароля и может временно ухудшить производительность."
        )
        confirm_cols = st.columns(2)
        if confirm_cols[0].button("✅ Да, очистить кэши", key="confirm_reset_caches_yes_dialog", use_container_width=True):
            request_reset_caches()
        if confirm_cols[1].button("❌ Отмена", key="confirm_reset_caches_no_dialog", use_container_width=True):
            st.session_state["confirm_reset_caches_inline"] = False
            st.rerun()

    _dialog()


def render_sidebar(cluster: ClusterConfig, wg: WorkloadGenerator) -> dict[str, Any]:
    st.sidebar.header("Управление нагрузкой")
    toggle_label = "⏹ Остановить нагрузку" if wg.running else "▶️ Запустить нагрузку"
    toggle_type = "primary" if wg.running else "secondary"
    if st.sidebar.button(toggle_label, key="sidebar_toggle_load", use_container_width=True, type=toggle_type):
        if wg.running:
            wg.stop()
        else:
            wg.start(
                cluster,
                st.session_state.get("load_mode", "rw-master"),
                int(st.session_state.get("load_clients", 1)),
                int(st.session_state.get("load_threads_per_client", 1)),
                float(st.session_state.get("load_read_ratio", 0.7)),
            )
        st.rerun()

    st.sidebar.divider()
    st.sidebar.header("Профиль нагрузки")

    legacy_mode_mapping = {
        "single-node": "rw-master",
        "master-rw": "rw-master",
        "dual-read": "r-master-r-slave",
        "master-rw-slave-r": "rw-master-r-slave",
    }

    defaults = {
        "load_mode": "rw-master",
        "load_clients": 10,
        "load_threads_per_client": 1,
        "load_read_ratio": 0.7,
        "load_auto_refresh": True,
    }

    # Widget-bound keys can be cleared by Streamlit when switching between pages.
    # Keep persistent values in dedicated keys and restore widget defaults from them.
    for key, default in defaults.items():
        persistent_key = f"persist_{key}"
        if persistent_key not in st.session_state:
            st.session_state[persistent_key] = default
        if key == "load_mode":
            st.session_state[persistent_key] = legacy_mode_mapping.get(st.session_state[persistent_key], st.session_state[persistent_key])
        if key not in st.session_state:
            st.session_state[key] = st.session_state[persistent_key]
        elif key == "load_mode":
            st.session_state[key] = legacy_mode_mapping.get(st.session_state[key], st.session_state[key])

    # Sync exact number inputs to slider-backed values before widgets are instantiated.
    # Streamlit does not allow changing widget state after the widget with the same key is created.
    for key in ("load_clients", "load_threads_per_client"):
        input_key = f"{key}_input"
        if input_key not in st.session_state:
            st.session_state[input_key] = int(st.session_state[key])
        elif int(st.session_state[input_key]) != int(st.session_state[key]):
            st.session_state[key] = int(st.session_state[input_key])

    mode = st.sidebar.selectbox(
        "Режим",
        options=["r-master", "rw-master", "r-master-r-slave", "rw-master-r-slave"],
        help=(
            "r-master: только чтение с master; "
            "rw-master: чтение и запись на master; "
            "r-master-r-slave: чтение распределяется между master и slave; "
            "rw-master-r-slave: запись идёт на master, чтение распределяется между master и slave"
        ),
        key="load_mode",
    )
    clients = st.sidebar.slider("Количество клиентов", min_value=1, max_value=64, step=1, key="load_clients")
    clients = st.sidebar.number_input("Клиенты (точное значение)", min_value=1, max_value=64, step=1, key="load_clients_input", value=int(clients))

    threads_per_client = st.sidebar.slider(
        "Потоков на клиента", min_value=1, max_value=8, step=1, key="load_threads_per_client"
    )
    threads_per_client = st.sidebar.number_input(
        "Потоки на клиента (точное значение)",
        min_value=1,
        max_value=8,
        step=1,
        key="load_threads_per_client_input",
        value=int(threads_per_client),
    )

    requested_workers = int(clients) * int(threads_per_client)
    total_workers = min(MAX_WORKERS, requested_workers)
    if requested_workers > MAX_WORKERS:
        st.sidebar.caption(
            f"Итого рабочих потоков генератора: {total_workers} из {requested_workers} (лимит: {MAX_WORKERS})"
        )
    else:
        st.sidebar.caption(f"Итого рабочих потоков генератора: {total_workers}")
    read_ratio = st.sidebar.slider("Доля чтения", 0.0, 1.0, step=0.05, key="load_read_ratio")
    if mode in {"r-master", "r-master-r-slave"}:
        st.sidebar.caption("Для профилей только на чтение параметр `Доля чтения` не используется: генератор выполняет только чтение.")
    auto_refresh = st.sidebar.checkbox("Автообновление", key="load_auto_refresh")

    st.session_state.persist_load_mode = mode
    st.session_state.persist_load_clients = int(clients)
    st.session_state.persist_load_threads_per_client = int(threads_per_client)
    st.session_state.persist_load_read_ratio = read_ratio
    st.session_state.persist_load_auto_refresh = auto_refresh

    st.sidebar.divider()
    st.sidebar.markdown("#### Сервисные операции")
    if st.sidebar.button("🔄 Сбросить счётчики", use_container_width=True, key="sidebar_reset_counters"):
        st.session_state["confirm_reset_counters_inline"] = True
        st.rerun()
    if st.sidebar.button("🧹 Сбросить кэши", use_container_width=True, key="sidebar_reset_caches"):
        st.session_state["confirm_reset_caches_inline"] = True
        st.rerun()

    open_reset_counters_dialog(cluster, wg)
    open_reset_caches_dialog(cluster)
    run_pending_service_operations(cluster)
    render_pending_service_operation_messages()

    return {
        "mode": mode,
        "clients": int(clients),
        "threads_per_client": int(threads_per_client),
        "total_workers": total_workers,
        "read_ratio": read_ratio,
        "auto_refresh": auto_refresh,
    }


def apply_compact_top_styles() -> None:
    apply_base_page_styles(
        """
        h1 {
            font-size: 2.05rem !important;
            line-height: 1.2 !important;
            margin-bottom: 0.2rem !important;
        }

        h3 {
            font-size: 1.35rem !important;
            margin-top: 0.45rem !important;
            margin-bottom: 0.2rem !important;
        }

        .stCaption {
            font-size: 0.84rem !important;
            margin-bottom: 0.1rem !important;
        }

        div[data-testid="stHorizontalBlock"] {
            gap: 0.5rem !important;
        }

        div[data-testid="stMetricValue"] {
            font-size: 1.25rem !important;
        }

        div[data-testid="stMetricLabel"] {
            font-size: 0.8rem !important;
        }

        div[data-testid="stMetricDelta"] {
            font-size: 0.75rem !important;
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
            font-size: 0.76rem !important;
            min-height: 1.35rem !important;
            padding: 0.05rem 0.35rem !important;
            line-height: 1.15 !important;
        }

        section[data-testid="stSidebar"] div[data-testid="stSidebarUserContent"] {
            padding-top: 0.75rem !important;
        }

        section[data-testid="stSidebar"] h2 {
            font-size: 1.85rem !important;
        }

        section[data-testid="stSidebar"] h3 {
            font-size: 1.16rem !important;
            margin-top: 0.35rem !important;
            margin-bottom: 0.15rem !important;
        }

        section[data-testid="stSidebar"] hr {
            margin-top: 0.75rem !important;
            margin-bottom: 0.75rem !important;
        }

        section[data-testid="stSidebar"] div[data-testid="stSelectbox"] label p,
        section[data-testid="stSidebar"] div[data-testid="stSlider"] label p,
        section[data-testid="stSidebar"] div[data-testid="stNumberInput"] label p,
        section[data-testid="stSidebar"] div[data-testid="stCheckbox"] label p {
            font-size: 0.86rem !important;
            line-height: 1.1 !important;
        }

        section[data-testid="stSidebar"] div[data-testid="stSelectbox"] div[data-baseweb="select"] > div,
        section[data-testid="stSidebar"] div[data-testid="stNumberInput"] input,
        section[data-testid="stSidebar"] div[data-testid="stTextInput"] input {
            min-height: 2.15rem !important;
            padding-top: 0.2rem !important;
            padding-bottom: 0.2rem !important;
        }

        section[data-testid="stSidebar"] div[data-testid="stButton"] button {
            min-height: 1.85rem !important;
            padding-top: 0.1rem !important;
            padding-bottom: 0.1rem !important;
            font-size: 0.95rem !important;
        }

        section[data-testid="stSidebar"] div[data-testid="stButton"]:first-of-type button[kind="primary"] {
            background-color: #16a34a !important;
            border-color: #16a34a !important;
            color: #ffffff !important;
        }

        section[data-testid="stSidebar"] div[data-testid="stButton"]:first-of-type button[kind="secondary"] {
            background-color: #dc2626 !important;
            border-color: #dc2626 !important;
            color: #ffffff !important;
        }

        section[data-testid="stSidebar"] div[data-testid="stButton"]:first-of-type button:hover {
            filter: brightness(0.95) !important;
        }

        section[data-testid="stSidebar"] div[data-testid="stSlider"] {
            margin-bottom: 0.15rem !important;
        }
        """,
    )


def render_controls(cluster: ClusterConfig, collector: BackgroundMetricsCollector) -> None:
    snapshot_rows = collector.snapshot().get("rows", [])
    node_statuses = {row.get("node"): row.get("status") for row in snapshot_rows}
    host_cols = st.columns(len(cluster.nodes))

    for idx, node in enumerate(cluster.nodes):
        with host_cols[idx]:
            is_running = node_statuses.get(node.name) == "up"
            button_label = (
                f"⏹ Остановить postgres на {node.name}"
                if is_running
                else f"▶️ Запустить postgres на {node.name}"
            )
            action = "stop" if is_running else "start"
            action_ru = "остановка" if is_running else "запуск"

            st.write(f"**{node.name}** ({node.role_hint})")
            if st.button(button_label, key=f"failure-toggle-{node.name}", use_container_width=True):
                ok, msg = run_node_action(node, action)
                st.toast(f"{node.name} {action_ru}: {'OK' if ok else 'ERR'} | {msg}")
                st.rerun()


def reset_server_stats(cluster: ClusterConfig) -> list[tuple[str, str]]:
    failures: list[str] = []
    notes: list[str] = []
    reset_statements = [
        "SELECT pg_stat_reset()",
        "SELECT pg_stat_reset_shared('bgwriter')",
        "SELECT pg_stat_reset_shared('wal')",
    ]

    for node in cluster.nodes:
        try:
            with psycopg.connect(node.dsn, connect_timeout=2, autocommit=True) as conn:
                with conn.cursor() as cur:
                    for stmt in reset_statements:
                        try:
                            cur.execute(stmt)
                        except Exception as stmt_exc:
                            notes.append(f"{node.name}: {stmt} -> {stmt_exc}")
        except Exception as exc:
            failures.append(f"{node.name}: {exc}")

    notes.append(
        "pg_stat_activity, pg_locks и pg_last_xact_replay_timestamp не имеют прямого reset API; "
        "их значения изменяются только состоянием кластера и активными сессиями."
    )

    messages: list[tuple[str, str]] = []
    if failures:
        messages.append(("warning", "Не удалось сбросить статистику (Failed to reset stats): " + "; ".join(failures)))
    else:
        messages.append(("success", "Серверная статистика сброшена (Server statistics reset)."))
    if notes:
        messages.append(("info", "Примечания по сбросу (Reset notes): " + " | ".join(notes)))
    return messages


def reset_all_node_caches(cluster: ClusterConfig) -> list[tuple[str, str]]:
    failures: list[str] = []
    successes: list[str] = []

    for node in cluster.nodes:
        ok, msg = run_node_action_via_ssh(node, 'sudo -n sh -c "sync; echo 3 > /proc/sys/vm/drop_caches"', timeout=20)
        if ok:
            successes.append(node.name)
        else:
            failures.append(f"{node.name}: {msg}")

    messages: list[tuple[str, str]] = []
    if successes:
        messages.append(("success", "Кэши очищены на нодах: " + ", ".join(successes)))
    if failures:
        messages.append(("warning", "Не удалось очистить кэши: " + "; ".join(failures)))
    return messages


def run_node_action_via_ssh(node: NodeConfig, remote_cmd: str, timeout: int = 15) -> tuple[bool, str]:
    if not node.control_via_ssh or not node.ssh_host:
        return False, "SSH control disabled for this node in config"

    cmd = build_ssh_command(node, remote_cmd)
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        LOGGER.error("SSH action timeout for node=%s cmd=%s", node.name, remote_cmd)
        return False, "SSH command timeout"

    output = (proc.stdout + "\n" + proc.stderr).strip()
    ok = proc.returncode == 0
    if not ok:
        LOGGER.error(
            "SSH action failed for node=%s cmd=%s returncode=%s output=%s",
            node.name,
            remote_cmd,
            proc.returncode,
            output or "<empty>",
        )
    return ok, output or "OK"


def render_metrics(cluster: ClusterConfig, wg: WorkloadGenerator, collector: BackgroundMetricsCollector) -> None:
    st.subheader("Состояние кластера (Cluster state)")
    snap = collector.snapshot()
    target_db = snap["target_db"]
    rows = snap["rows"]
    df = pd.DataFrame(rows)
    if not df.empty:
        df.insert(
            0,
            "status_icon",
            df["status"].map({"up": "🟢", "down": "🔴"}).fillna("⚪"),
        )
    if not df.empty:
        df["status"] = df["status"].map({"up": "UP", "down": "DOWN"}).fillna(df["status"])
    localized_df = df.rename(
        columns={
            "status_icon": "State",
            "node": "Node",
            "status": "Status",
            "role": "Role",
            "replay_delay_sec": "Replay delay, sec",
            "active_locks": "Active locks",
            "active_queries": "Active queries",
            "xact_commit": "Committed tx",
            "xact_rollback": "Rolled back tx",
            "blks_read": "Blocks read",
            "blks_hit": "Cache hits",
            "tup_returned": "Rows returned",
            "tup_fetched": "Rows fetched",
            "blk_read_time_ms": "Disk read latency, ms",
            "blk_write_time_ms": "Disk write latency, ms",
            "disk_io_queue": "Disk queue",
            "disk_read_kb_s_os": "OS disk read, KB/s",
            "disk_write_kb_s_os": "OS disk write, KB/s",
            "disk_util_pct_os": "OS disk util, %",
            "tx_read_only": "Tx read-only",
            "error": "Error",
        }
    )
    if not localized_df.empty:
        if {"Role", "Replay delay, sec"}.issubset(localized_df.columns):
            master_mask = localized_df["Role"].astype(str).str.lower().isin({"master", "primary", "leader"})
            localized_df["Replay delay, sec"] = localized_df["Replay delay, sec"].astype("object")
            localized_df.loc[master_mask, "Replay delay, sec"] = "—"

        numeric_columns = [
            "Replay delay, sec",
            "Active locks",
            "Active queries",
            "Committed tx",
            "Rolled back tx",
            "Blocks read",
            "Cache hits",
            "Rows returned",
            "Rows fetched",
            "Disk read latency, ms",
            "Disk write latency, ms",
            "Disk queue",
            "OS disk read, KB/s",
            "OS disk write, KB/s",
            "OS disk util, %",
        ]
        for column in numeric_columns:
            if column in localized_df.columns:
                localized_df[column] = localized_df[column].map(format_cluster_table_value)
        if "Error" in localized_df.columns:
            localized_df["Error"] = localized_df["Error"].fillna("")
    metrics_meta = f"Целевая БД нагрузки (Target DB): {target_db}"
    if snap["updated_at"] is not None:
        metrics_meta += f" | Последнее обновление метрик: {snap['updated_at'].strftime('%H:%M:%S')}"
    st.caption(metrics_meta)
    row_height_px = 35
    header_height_px = 38
    table_height = max(105, min(190, header_height_px + len(localized_df) * row_height_px))
    st.dataframe(localized_df, width="stretch", height=table_height, hide_index=True)
    st.caption(
        "Data source: PostgreSQL system views (pg_stat_database, pg_stat_activity, pg_locks, pg_last_xact_replay_timestamp) and SSH iostat -dx."
    )
    if snap["error"]:
        st.warning(f"Ошибка фонового сборщика метрик: {snap['error']}")

    if rows and all(row.get("status") != "up" for row in rows):
        LOGGER.warning(
            "No DB connections to any node. Errors: %s",
            {row.get("node"): row.get("error") for row in rows},
        )
        st.warning("Нет подключений к узлам БД. Проверьте доступность PostgreSQL и параметры DSN/SSH в конфиге.")
    st.info(f"Статус генератора нагрузки (Load generator status): {'🟢 РАБОТАЕТ' if wg.running else '🔴 ОСТАНОВЛЕН'}")
    clients = int(st.session_state.get("load_clients", 1))
    threads_per_client = int(st.session_state.get("load_threads_per_client", 1))
    requested_workers = clients * threads_per_client
    actual_workers = min(MAX_WORKERS, requested_workers)
    workers_caption = f"рабочих потоков={actual_workers}"
    if requested_workers > MAX_WORKERS:
        workers_caption += f" из {requested_workers} (лимит: {MAX_WORKERS})"

    st.caption(
        f"Параметры нагрузки: клиентов={clients}, "
        f"потоков на клиента={threads_per_client}, "
        f"{workers_caption}"
    )

    st.subheader("Статистика нагрузки (Load stats)")

    recent_errors = wg.recent_errors_snapshot()
    if recent_errors:
        st.caption("Подробные логи SQL-транзакций перенесены на страницу «SQL логи транзакций».")

    history = collector.history_snapshot()
    hist_df = pd.DataFrame(history)
    if not hist_df.empty:
        chart_df = hist_df.set_index("ts")
        query_error_cols = ["read_tx", "write_tx", "errors"]
        query_error_moment_df = chart_df[query_error_cols].diff().clip(lower=0).fillna(0)
        chart_cols = st.columns(3)

        with chart_cols[0]:
            st.caption("Запросы и ошибки (Queries and errors)")
            st.line_chart(query_error_moment_df, height=320)
            st.caption("Источник данных: генератор нагрузки (счётчики read_tx, write_tx, errors; на графике показана разница между соседними срезами).")

        with chart_cols[1]:
            st.caption("Блокировки и активные запросы (Locks and active queries)")
            st.line_chart(chart_df[["active_locks", "active_queries"]], height=320)
            st.caption("Источник данных: PostgreSQL system views pg_locks и pg_stat_activity.")

        with chart_cols[2]:
            st.caption("Нагрузка на диски по узлам (Disk load per node)")
            role_aliases = {
                "master": {"master", "primary", "leader"},
                "slave": {"slave", "replica", "standby"},
            }

            role_nodes: dict[str, NodeConfig | None] = {"master": None, "slave": None}
            for node in cluster.nodes:
                normalized_role = node.role_hint.strip().lower()
                for role, aliases in role_aliases.items():
                    if role_nodes[role] is None and normalized_role in aliases:
                        role_nodes[role] = node

            disk_chart_data: dict[str, pd.Series] = {}
            for role, node in role_nodes.items():
                if not node:
                    continue

                read_col = f"disk_read_kb_s_{node.name}"
                write_col = f"disk_write_kb_s_{node.name}"

                if read_col in chart_df.columns:
                    disk_chart_data[f"{role}_read_kb_s"] = chart_df[read_col]
                if write_col in chart_df.columns:
                    disk_chart_data[f"{role}_write_kb_s"] = chart_df[write_col]

            if disk_chart_data:
                disk_chart_df = pd.DataFrame(disk_chart_data, index=chart_df.index)
                st.line_chart(disk_chart_df, height=320)
                st.caption(
                    "Источник данных: SSH iostat -dx. "
                    "Ось Y: скорость чтения/записи диска в KB/s для master и slave узлов."
                )
            else:
                st.info("Пока нет данных по дискам")


def main() -> None:
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    apply_compact_top_styles()
    title_cols = st.columns([8.4, 1.6], gap="small")
    with title_cols[0]:
        st.title(APP_TITLE)
    with title_cols[1]:
        if st.button("🔄 Refresh", key="scenario_refresh_now", help="Refresh now", use_container_width=True):
            st.rerun()

    st.session_state.setdefault("cfg_path", "config/cluster.json")
    raw_cfg_path = str(st.session_state["cfg_path"]).strip()
    if not raw_cfg_path:
        st.error("Укажите путь к конфигу (Provide a path to the config file).")
        st.stop()

    cfg_path = Path(raw_cfg_path)
    if not cfg_path.exists():
        st.error(f"Конфиг не найден (Config not found): {cfg_path}")
        st.stop()
    if not cfg_path.is_file():
        st.error(f"Ожидался JSON-файл конфига, но указан каталог (Expected a JSON config file, got a directory): {cfg_path}")
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
    if "metrics_collector" not in st.session_state:
        st.session_state.metrics_collector = BackgroundMetricsCollector()

    wg: WorkloadGenerator = st.session_state.workload_generator
    collector: BackgroundMetricsCollector = st.session_state.metrics_collector
    profile = render_sidebar(cluster, wg)

    wg.update_settings(
        cluster,
        profile["mode"],
        int(profile["clients"]),
        int(profile["threads_per_client"]),
        float(profile["read_ratio"]),
    )

    current_signature = build_cluster_signature(cluster)
    if collector.cluster_signature != current_signature or not collector.running:
        collector.start(cluster, wg, profile["mode"])
    else:
        collector.set_mode(profile["mode"])

    render_controls(cluster, collector)
    render_metrics(cluster, wg, collector)

    st.divider()
    st.text_input("Путь к конфигу (Path to config)", key="cfg_path")

    if profile["auto_refresh"]:
        time.sleep(cluster.poll_interval_sec)
        st.rerun()


if __name__ == "__main__":
    main()
