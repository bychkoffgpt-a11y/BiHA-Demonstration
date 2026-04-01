from __future__ import annotations

import json
import random
import shlex
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from zoneinfo import ZoneInfo

import pandas as pd
import psycopg
from psycopg.conninfo import conninfo_to_dict
import streamlit as st

from orchestration.command_audit import record_command
from logging_utils import setup_file_logger
from ui_styles import apply_base_page_styles
from workload_status_store import (
    normalize_workload_status,
    read_workload_status,
    update_workload_desired_state,
    write_runtime_workload_status,
    WorkloadStatusConflictError,
)
from workload_profiles import PgLikeSizing, estimate_pg_like_sizing, run_pg_like_tx

APP_TITLE = "Демонстрация кластера PostgreSQL BiHA"
DEFAULT_HISTORY = 120
DEFAULT_WORKLOAD_DB_SIZE_GB = 1.0
MAX_WORKERS = 64
METRICS_FETCH_WORKERS = 8
MOSCOW_TZ = ZoneInfo("Europe/Moscow")
LOGGER = setup_file_logger()
WORKLOAD_LOCK_TIMEOUT_MS = 1_500
WORKLOAD_STATEMENT_TIMEOUT_MS = 8_000
PG_QUERY_CANCELED_SQLSTATE = "57014"
WORKLOAD_STATUS_SYNC_KEYS = ("mode", "clients", "threads_per_client", "read_ratio")
SHARED_WORKLOAD_RUNTIME_ID = "shared-workload-runtime"
_SHARED_WORKLOAD_GENERATOR: WorkloadGenerator | None = None
_SHARED_WORKLOAD_GENERATOR_LOCK = threading.RLock()
MASTER_ROLE_ALIASES = {"master", "primary", "leader", "rw", "writer", "readwrite", "read-write"}
SLAVE_ROLE_ALIASES = {"slave", "replica", "standby", "ro", "reader", "readonly", "read-only"}
CLUSTER_METRICS_SQL = """
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
                        current_setting('transaction_read_only'),
                        current_setting('track_io_timing', true)
                    """


@dataclass
class NodeConfig:
    name: str
    dsn: str
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
    vip_dsn: str
    poll_interval_sec: int = 2


def classify_node_role(role_value: Any, tx_read_only: Any) -> str:
    normalized_role = str(role_value or "").strip().lower()
    if normalized_role in MASTER_ROLE_ALIASES:
        return "master"
    if normalized_role in SLAVE_ROLE_ALIASES:
        return "slave"

    normalized_tx_read_only = str(tx_read_only or "").strip().lower()
    if normalized_tx_read_only in {"off", "false", "0", "no"}:
        return "master"
    if normalized_tx_read_only in {"on", "true", "1", "yes"}:
        return "slave"
    return "unknown"


class WorkloadGenerator:
    """Фоновый генератор pgbench-подобной нагрузки с динамическим масштабированием."""

    @dataclass
    class _WorkerSlot:
        thread: threading.Thread
        stop_event: threading.Event

    def __init__(self, runtime_id: str) -> None:
        self._runtime_id = runtime_id
        self._workers: list[WorkloadGenerator._WorkerSlot] = []
        self._workers_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._manager_thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._settings_lock = threading.Lock()
        self._stats: dict[str, int] = {"read_tx": 0, "write_tx": 0, "errors": 0}
        self._recent_errors: list[dict[str, Any]] = []
        self._cluster: ClusterConfig | None = None
        self._mode: str = "rw"
        self._read_ratio: float = 0.7
        self._desired_workers: int = 1
        self._sizing: PgLikeSizing = estimate_pg_like_sizing(DEFAULT_WORKLOAD_DB_SIZE_GB)
        self._last_status_flush_monotonic: float = 0.0

    @property
    def running(self) -> bool:
        return self.manager_running or self.has_alive_workers()

    @property
    def manager_running(self) -> bool:
        return self._manager_thread is not None and self._manager_thread.is_alive()

    def has_alive_workers(self) -> bool:
        with self._workers_lock:
            return any(worker.thread.is_alive() for worker in self._workers)

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

    def start(
        self,
        cluster: ClusterConfig,
        mode: str,
        clients: int,
        threads_per_client: int,
        read_ratio: float,
        *,
        target_size_gb: float | None = None,
    ) -> None:
        self.update_settings(
            cluster,
            mode,
            clients,
            threads_per_client,
            read_ratio,
            target_size_gb=target_size_gb,
        )
        if self.manager_running:
            return
        self._stop_event.clear()
        self._write_status(is_running=True)

        self._manager_thread = threading.Thread(target=self._manage_workers, daemon=True, name="workload-manager")
        self._manager_thread.start()

    def update_settings(
        self,
        cluster: ClusterConfig,
        mode: str,
        clients: int,
        threads_per_client: int,
        read_ratio: float,
        *,
        target_size_gb: float | None = None,
    ) -> None:
        resolved_target_size_gb = (
            DEFAULT_WORKLOAD_DB_SIZE_GB if target_size_gb is None else float(target_size_gb)
        )
        desired_workers = max(1, min(MAX_WORKERS, clients * threads_per_client))
        with self._settings_lock:
            self._cluster = cluster
            self._mode = mode
            self._read_ratio = read_ratio
            self._desired_workers = desired_workers
            self._sizing = estimate_pg_like_sizing(resolved_target_size_gb)
        self._write_status(
            is_running=self.running,
        )

    def stop(self) -> None:
        self._stop_event.set()
        if self._manager_thread:
            self._manager_thread.join(timeout=2)
        self._manager_thread = None
        self._stop_all_workers()
        self._write_status(is_running=False)

    def _manage_workers(self) -> None:
        try:
            while not self._stop_event.is_set():
                self._sync_workers()
                now = time.monotonic()
                if now - self._last_status_flush_monotonic >= 1.0:
                    try:
                        self._write_status(is_running=True)
                    except Exception:
                        LOGGER.exception("Failed to flush runtime workload status from manager thread")
                    self._last_status_flush_monotonic = now
                self._stop_event.wait(0.2)
        except Exception:
            LOGGER.exception("Workload manager thread crashed")
        finally:
            self._stop_all_workers()
            try:
                self._write_status(is_running=False)
            except Exception:
                LOGGER.exception("Failed to flush stopped runtime workload status after manager shutdown")

    def _stop_all_workers(self) -> None:
        with self._workers_lock:
            workers = list(self._workers)
            self._workers = []
        for worker in workers:
            worker.stop_event.set()
        for worker in workers:
            worker.thread.join(timeout=2)

    def _sync_workers(self) -> None:
        with self._workers_lock:
            workers_snapshot = list(self._workers)
        alive_workers: list[WorkloadGenerator._WorkerSlot] = []
        for worker in workers_snapshot:
            if worker.thread.is_alive():
                alive_workers.append(worker)
        with self._workers_lock:
            self._workers = alive_workers

        with self._settings_lock:
            desired_workers = self._desired_workers

        while True:
            with self._workers_lock:
                if len(self._workers) <= desired_workers:
                    break
                worker = self._workers.pop()
            worker.stop_event.set()
            worker.thread.join(timeout=2)

        while not self._stop_event.is_set():
            with self._workers_lock:
                if len(self._workers) >= desired_workers:
                    break
            worker_stop_event = threading.Event()
            thread = threading.Thread(target=self._run_worker, args=(worker_stop_event,), daemon=True)
            slot = WorkloadGenerator._WorkerSlot(thread=thread, stop_event=worker_stop_event)
            with self._workers_lock:
                self._workers.append(slot)
            thread.start()

    def _run_worker(self, worker_stop_event: threading.Event) -> None:
        while not self._stop_event.is_set() and not worker_stop_event.is_set():
            node: NodeConfig | None = None
            write_tx = False
            mode = "rw"
            try:
                with self._settings_lock:
                    cluster = self._cluster
                    mode = self._mode
                    read_ratio = self._read_ratio
                    sizing = self._sizing
                if cluster is None:
                    raise RuntimeError("Cluster config is not initialized")

                read_only_modes = {"r"}
                write_tx = False if mode in read_only_modes else random.random() > read_ratio
                node = select_node_for_workload(cluster.nodes)
                if not node:
                    raise RuntimeError("No available node for selected mode")
                try:
                    execute_workload_tx(cluster.vip_dsn, write_tx, sizing)
                except Exception as exc:
                    should_retry = not write_tx and is_recovery_conflict_error(exc)
                    if not should_retry:
                        raise

                    LOGGER.info(
                        "Retrying read transaction via VIP after replica recovery conflict | "
                        "mode=%s failed_node=%s",
                        mode,
                        node.name,
                    )
                    execute_workload_tx(cluster.vip_dsn, write_tx, sizing)
                with self._lock:
                    self._stats["write_tx" if write_tx else "read_tx"] += 1
            except Exception as exc:
                error_message = format_workload_error(exc)
                node_name = node.name if node else "unknown"
                tx_type = "write" if write_tx else "read"
                LOGGER.error(
                    "Workload transaction failed | mode=%s node=%s tx_type=%s error=%s",
                    mode,
                    node_name,
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
    ) -> None:
        with self._settings_lock:
            desired_workers = self._desired_workers

        payload: dict[str, Any] = {
            "is_running": is_running,
            "requested_threads": desired_workers,
            "updated_at": pd.Timestamp.now(tz=MOSCOW_TZ).isoformat(),
            "last_error": None,
        }
        write_runtime_workload_status(payload)


def get_shared_workload_generator() -> WorkloadGenerator:
    global _SHARED_WORKLOAD_GENERATOR
    with _SHARED_WORKLOAD_GENERATOR_LOCK:
        if _SHARED_WORKLOAD_GENERATOR is None:
            _SHARED_WORKLOAD_GENERATOR = WorkloadGenerator(SHARED_WORKLOAD_RUNTIME_ID)
        return _SHARED_WORKLOAD_GENERATOR


def start_shared_workload(
    wg: WorkloadGenerator,
    cluster: ClusterConfig,
    profile: dict[str, Any],
    target_size_gb: float,
) -> None:
    with _SHARED_WORKLOAD_GENERATOR_LOCK:
        wg.start(
            cluster,
            str(profile["mode"]),
            int(profile["clients"]),
            int(profile["threads_per_client"]),
            float(profile["read_ratio"]),
            target_size_gb=target_size_gb,
        )


def update_shared_workload(
    wg: WorkloadGenerator,
    cluster: ClusterConfig,
    profile: dict[str, Any],
    target_size_gb: float,
) -> None:
    with _SHARED_WORKLOAD_GENERATOR_LOCK:
        wg.update_settings(
            cluster,
            str(profile["mode"]),
            int(profile["clients"]),
            int(profile["threads_per_client"]),
            float(profile["read_ratio"]),
            target_size_gb=target_size_gb,
        )


def stop_shared_workload(wg: WorkloadGenerator) -> None:
    with _SHARED_WORKLOAD_GENERATOR_LOCK:
        wg.stop()


def queue_workload_ui_message(message: str, *, kind: str = "warning") -> None:
    pending = list(st.session_state.get("pending_workload_ui_messages", []))
    pending.append({"kind": kind, "message": message})
    st.session_state["pending_workload_ui_messages"] = pending


def render_pending_workload_ui_messages() -> None:
    pending = st.session_state.pop("pending_workload_ui_messages", [])
    for item in pending:
        message = str(item.get("message", "")).strip()
        if not message:
            continue
        kind = str(item.get("kind", "info"))
        toast_fn = getattr(st, "toast", None)
        if callable(toast_fn):
            toast_fn(message)
        elif kind == "error":
            st.error(message)
        elif kind == "warning":
            st.warning(message)
        else:
            st.info(message)


def request_shared_workload_profile_update(
    profile: dict[str, Any],
    *,
    desired_is_running: bool | None = None,
    expected_revision: int | None = None,
) -> dict[str, Any] | None:
    desired_patch = {
        "mode": str(profile["mode"]),
        "clients": int(profile["clients"]),
        "threads_per_client": int(profile["threads_per_client"]),
        "read_ratio": float(profile["read_ratio"]),
    }
    if desired_is_running is not None:
        desired_patch["is_running"] = bool(desired_is_running)
    try:
        return update_workload_desired_state(desired_patch, expected_revision=expected_revision)
    except WorkloadStatusConflictError:
        queue_workload_ui_message(
            "Общее состояние нагрузки изменилось в другой вкладке. Параметры обновлены из файла.",
            kind="warning",
        )
        return None
    except Exception as exc:
        queue_workload_ui_message(f"Не удалось обновить общее состояние нагрузки: {exc}", kind="error")
        return None


def reconcile_shared_workload_runtime(cluster: ClusterConfig, wg: WorkloadGenerator, target_size_gb: float) -> dict[str, Any]:
    status_document = read_workload_status()
    normalized_state = normalize_workload_status(status_document)
    desired_is_running = bool(normalized_state.get("desired_is_running"))
    profile = {
        "mode": normalized_state["mode"],
        "clients": int(normalized_state["clients"]),
        "threads_per_client": int(normalized_state["threads_per_client"]),
        "read_ratio": float(normalized_state["read_ratio"]),
    }
    requested_threads = int(profile["clients"]) * int(profile["threads_per_client"])
    previous_applied_revision = int(normalized_state.get("applied_revision", 0) or 0)

    runtime_patch: dict[str, Any] = {
        "applied_revision": previous_applied_revision,
        "requested_threads": min(MAX_WORKERS, requested_threads),
        "updated_at": pd.Timestamp.now(tz=MOSCOW_TZ).isoformat(),
        "last_error": None,
    }

    try:
        if desired_is_running:
            if wg.running:
                update_shared_workload(wg, cluster, profile, target_size_gb)
            else:
                start_shared_workload(wg, cluster, profile, target_size_gb)
        elif wg.running:
            stop_shared_workload(wg)
        runtime_patch["applied_revision"] = int(normalized_state["revision"])
        runtime_patch["is_running"] = wg.running
    except Exception as exc:
        LOGGER.exception("Failed to reconcile shared workload runtime")
        runtime_patch["is_running"] = wg.running
        runtime_patch["last_error"] = str(exc)
    finally:
        write_runtime_workload_status(runtime_patch)

    return normalize_workload_status(read_workload_status())


class BackgroundMetricsCollector:
    """Фоновый сборщик метрик кластера."""

    def __init__(self) -> None:
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()
        self._mode: str = "rw"
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
        thread = self._thread
        if thread:
            thread.join(timeout=2)
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
                    **{f"role_{row['node']}": str(row.get("role") or "unknown") for row in rows},
                    **{f"disk_read_latency_{row['node']}": float(row.get("blk_read_time_ms") or 0.0) for row in rows},
                    **{f"disk_write_latency_{row['node']}": float(row.get("blk_write_time_ms") or 0.0) for row in rows},
                    **{f"disk_queue_{row['node']}": float(row.get("disk_io_queue") or 0.0) for row in rows},
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
                if isinstance(exc, RuntimeError) and (
                    str(exc) == "cannot schedule new futures after interpreter shutdown"
                ):
                    LOGGER.info(
                        "Фоновый сбор метрик остановлен во время shutdown интерпретатора: %s",
                        exc,
                    )
                    with self._lock:
                        self._last_error = None
                    break
                LOGGER.exception("Ошибка фонового сбора метрик")
                with self._lock:
                    self._last_error = str(exc)

            self._stop_event.wait(cluster.poll_interval_sec)


def build_cluster_signature(cluster: ClusterConfig) -> tuple[Any, ...]:
    return (
        cluster.poll_interval_sec,
        cluster.vip_dsn,
        tuple((node.name, node.dsn, node.control_via_ssh, node.ssh_host) for node in cluster.nodes),
    )


def select_node_for_workload(nodes: list[NodeConfig]) -> NodeConfig | None:
    if not nodes:
        return None
    return random.choice(nodes)


def execute_workload_tx(vip_dsn: str, write_tx: bool, sizing: PgLikeSizing) -> None:
    with psycopg.connect(vip_dsn, connect_timeout=2, autocommit=False) as conn:
        with conn.cursor() as cur:
            lock_timeout_sql = f"SET LOCAL lock_timeout = '{WORKLOAD_LOCK_TIMEOUT_MS}ms'"
            statement_timeout_sql = f"SET LOCAL statement_timeout = '{WORKLOAD_STATEMENT_TIMEOUT_MS}ms'"
            idle_timeout_sql = "SET LOCAL idle_in_transaction_session_timeout = '10s'"
            for stmt in (lock_timeout_sql, statement_timeout_sql, idle_timeout_sql):
                record_command("sql_workload", stmt, dsn=vip_dsn, write_tx=write_tx)
                cur.execute(stmt)
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
    return extract_dbname_from_dsn(cluster.vip_dsn)


def load_cluster_config(path: Path | str) -> ClusterConfig:
    cfg_path = Path(path)
    if not cfg_path.exists():
        raise FileNotFoundError(f"Config not found: {path}")
    if not cfg_path.is_file():
        raise ValueError(f"Expected a JSON config file, got a directory: {path}")
    cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
    allowed_keys = {field.name for field in fields(NodeConfig)}
    nodes = [NodeConfig(**{k: v for k, v in item.items() if k in allowed_keys}) for item in cfg.get("nodes", [])]
    vip_dsn = cfg.get("vip_dsn")
    if not vip_dsn:
        raise ValueError("Config must contain non-empty 'vip_dsn'")
    return ClusterConfig(nodes=nodes, vip_dsn=str(vip_dsn), poll_interval_sec=cfg.get("poll_interval_sec", 2))


def mask_dsn(dsn: str) -> str:
    parsed = urlparse(dsn)
    if not parsed.netloc or "@" not in parsed.netloc:
        return dsn
    creds, host_part = parsed.netloc.rsplit("@", 1)
    username = creds.split(":", 1)[0] if creds else "user"
    masked_netloc = f"{username}:***@{host_part}"
    return parsed._replace(netloc=masked_netloc).geturl()


def extract_host_from_dsn(dsn: str) -> str:
    try:
        conninfo = conninfo_to_dict(dsn)
    except Exception:
        conninfo = {}
    host = str(conninfo.get("host") or "").strip()
    if host:
        return host
    parsed = urlparse(dsn)
    return parsed.hostname or "-"


def fetch_node_metrics(node: NodeConfig, target_db: str) -> dict[str, Any]:
    result: dict[str, Any] = {
        "node": node.name,
        "host": extract_host_from_dsn(node.dsn),
        "status": "down",
        "role": "unknown",
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
        track_io_timing: str | None = None
        query_params = (
            target_db,
            target_db,
            target_db,
            target_db,
            target_db,
            target_db,
            target_db,
            target_db,
            target_db,
            target_db,
            target_db,
        )
        LOGGER.info(
            "Executing PostgreSQL metrics query | node=%s host=%s db=%s sql=%s params=%s",
            node.name,
            extract_host_from_dsn(node.dsn),
            target_db,
            " ".join(CLUSTER_METRICS_SQL.split()),
            query_params,
        )
        record_command(
            "sql_observation",
            " ".join(CLUSTER_METRICS_SQL.split()),
            node=node.name,
            host=extract_host_from_dsn(node.dsn),
            database=target_db,
            params=repr(query_params),
        )
        with psycopg.connect(node.dsn, connect_timeout=2, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(CLUSTER_METRICS_SQL, query_params)
                LOGGER.info("PostgreSQL metrics query completed | node=%s db=%s", node.name, target_db)
                row = cur.fetchone()
                if row:
                    track_io_timing = row[14]
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

        LOGGER.info(
            "Метрики узла для таблицы состояния кластера | %s",
            json.dumps(
                {
                    "node": node.name,
                    "target_db": target_db,
                    "status": result["status"],
                    "role": result["role"],
                    "track_io_timing": track_io_timing,
                    "tx_read_only": result["tx_read_only"],
                    "disk_metrics_source": "ssh_iostat" if node.collect_disk_metrics_via_ssh else "postgresql",
                    "db_metrics": {
                        "blk_read_time_ms": result["blk_read_time_ms"],
                        "blk_write_time_ms": result["blk_write_time_ms"],
                        "disk_io_queue": result["disk_io_queue"],
                        "active_queries": result["active_queries"],
                        "active_locks": result["active_locks"],
                    },
                    "os_disk_metrics": {
                        "collect_via_ssh": node.collect_disk_metrics_via_ssh,
                        "disk_device": node.disk_device,
                        "disk_read_kb_s_os": result["disk_read_kb_s_os"],
                        "disk_write_kb_s_os": result["disk_write_kb_s_os"],
                        "disk_util_pct_os": result["disk_util_pct_os"],
                    },
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
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
                    "host": extract_host_from_dsn(node.dsn),
                    "status": "down",
                    "role": "unknown",
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
        "blk_read_time_ms": None,
        "blk_write_time_ms": None,
        "disk_io_queue": None,
        "disk_read_kb_s_os": None,
        "disk_write_kb_s_os": None,
        "disk_util_pct_os": None,
    }

    def log_and_return_default(reason: str, **details: Any) -> dict[str, float | int | None]:
        LOGGER.info(
            "SSH-метрики диска недоступны для таблицы состояния кластера | %s",
            json.dumps(
                {
                    "node": node.name,
                    "disk_device": node.disk_device,
                    "reason": reason,
                    **details,
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
        )
        return default_metrics

    if not node.control_via_ssh or not node.ssh_host:
        return log_and_return_default("ssh_disabled_or_host_missing")

    remote_cmd = 'bash -lc "LC_ALL=C iostat -dx 1 2"'
    cmd = build_ssh_command(node, remote_cmd)
    command_rendered = shlex.join(cmd)
    record_command("ssh_observation", command_rendered, node=node.name, host=node.ssh_host, probe="disk_iostat")
    LOGGER.info(
        "Executing SSH disk metrics command | node=%s host=%s command=%s",
        node.name,
        node.ssh_host,
        command_rendered,
    )
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=8)
    except Exception as exc:
        return log_and_return_default("ssh_command_exception", error=str(exc))
    LOGGER.info(
        "SSH disk metrics command finished | node=%s host=%s returncode=%s",
        node.name,
        node.ssh_host,
        proc.returncode,
    )

    if proc.returncode != 0:
        return log_and_return_default(
            "ssh_command_failed",
            returncode=proc.returncode,
            stderr=proc.stderr.strip(),
            stdout=proc.stdout.strip(),
        )

    raw_output = proc.stdout.strip()
    if not raw_output:
        return log_and_return_default("empty_output")
    sections = [section.strip() for section in raw_output.split("\n\n") if section.strip() and "Device" in section]
    if not sections:
        return log_and_return_default("device_section_not_found", stdout=raw_output)

    lines = [line.strip() for line in sections[-1].splitlines() if line.strip()]
    header_idx = next((idx for idx, line in enumerate(lines) if line.startswith("Device")), None)
    if header_idx is None or header_idx + 1 >= len(lines):
        return log_and_return_default("header_or_device_rows_missing", stdout=sections[-1])

    headers = lines[header_idx].split()
    device_rows = [line.split() for line in lines[header_idx + 1 :] if not line.startswith("avg-cpu")]
    if not device_rows:
        return log_and_return_default("parsed_device_rows_empty", headers=headers)

    if node.disk_device:
        requested_device = node.disk_device.strip()
        requested_aliases = {requested_device, requested_device.removeprefix("/dev/")}
        filtered_rows = [row for row in device_rows if row and row[0] in requested_aliases]
        if not filtered_rows:
            return log_and_return_default(
                "configured_device_not_found",
                requested_device=requested_device,
                available_devices=[row[0] for row in device_rows if row],
            )
        device_rows = filtered_rows

    totals = {
        "blk_read_time_ms": 0.0,
        "blk_write_time_ms": 0.0,
        "disk_read_kb_s_os": 0.0,
        "disk_write_kb_s_os": 0.0,
        "disk_io_queue": 0.0,
        "disk_util_pct_os": 0.0,
    }
    found = {key: False for key in totals}
    counts = {"blk_read_time_ms": 0, "blk_write_time_ms": 0}

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
        read_latency_ms, _ = parse_metric(values_by_header, ["r_await", "await"])
        write_latency_ms, _ = parse_metric(values_by_header, ["w_await", "await"])
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
        if read_latency_ms is not None:
            totals["blk_read_time_ms"] += read_latency_ms
            found["blk_read_time_ms"] = True
            counts["blk_read_time_ms"] += 1
        if write_latency_ms is not None:
            totals["blk_write_time_ms"] += write_latency_ms
            found["blk_write_time_ms"] = True
            counts["blk_write_time_ms"] += 1
        if queue is not None:
            totals["disk_io_queue"] += queue
            found["disk_io_queue"] = True
        if util is not None:
            totals["disk_util_pct_os"] += util
            found["disk_util_pct_os"] = True

    read_latency_ms = (
        totals["blk_read_time_ms"] / counts["blk_read_time_ms"]
        if counts["blk_read_time_ms"]
        else None
    )
    write_latency_ms = (
        totals["blk_write_time_ms"] / counts["blk_write_time_ms"]
        if counts["blk_write_time_ms"]
        else None
    )
    read_kb = totals["disk_read_kb_s_os"] if found["disk_read_kb_s_os"] else None
    write_kb = totals["disk_write_kb_s_os"] if found["disk_write_kb_s_os"] else None
    queue = totals["disk_io_queue"] if found["disk_io_queue"] else None
    util = totals["disk_util_pct_os"] if found["disk_util_pct_os"] else None

    if read_latency_ms is None and write_latency_ms is None and read_kb is None and write_kb is None and queue is None and util is None:
        return log_and_return_default("metrics_not_parsed", headers=headers, rows_used=[row[0] for row in device_rows if row])

    parsed_metrics = {
        "blk_read_time_ms": read_latency_ms,
        "blk_write_time_ms": write_latency_ms,
        "disk_read_kb_s_os": read_kb,
        "disk_write_kb_s_os": write_kb,
        "disk_io_queue": queue,
        "disk_util_pct_os": util,
    }
    LOGGER.info(
        "SSH-метрики диска для таблицы состояния кластера | %s",
        json.dumps(
            {
                "node": node.name,
                "disk_device": node.disk_device,
                "rows_used": [row[0] for row in device_rows if row],
                "headers": headers,
                "metrics": parsed_metrics,
            },
            ensure_ascii=False,
            sort_keys=True,
        ),
    )

    return parsed_metrics


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
    command_rendered = shlex.join(cmd)
    record_command("ssh_action", command_rendered, node=node.name, host=node.ssh_host, action=action)
    LOGGER.info(
        "Executing SSH node action | node=%s host=%s action=%s command=%s",
        node.name,
        node.ssh_host,
        action,
        command_rendered,
    )
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
    except subprocess.TimeoutExpired:
        LOGGER.error("SSH action timeout for node=%s action=%s host=%s", node.name, action, node.ssh_host)
        return False, f"SSH command timeout\nCommand: {command_rendered}"

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
    command_output = output or "OK"
    return ok, f"Command: {command_rendered}\nResponse: {command_output}"


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


def request_master_switchover() -> None:
    st.session_state["confirm_master_switchover_inline"] = False
    st.session_state["pending_master_switchover_action"] = True
    st.rerun()


def switchover_master_role(cluster: ClusterConfig, target_master: str | None = None) -> dict[str, Any]:
    operation_started_monotonic = time.monotonic()
    target_db = get_target_database(cluster, "rw")
    rows = fetch_all_node_metrics(cluster.nodes, target_db)
    result: dict[str, Any] = {
        "messages": [],
        "success": False,
        "roles": {},
        "diagnostics": [],
        "downtime_sec": None,
        "orchestration_duration_sec": None,
    }
    normalized_target_master = str(target_master or "").strip() or None

    def finalize(payload: dict[str, Any]) -> dict[str, Any]:
        payload["orchestration_duration_sec"] = round(time.monotonic() - operation_started_monotonic, 3)
        return payload

    if not rows:
        result["messages"] = [("error", "Не удалось получить состояние узлов перед переключением master.")]
        return finalize(result)

    nodes_by_name = {node.name: node for node in cluster.nodes}

    masters = [
        row
        for row in rows
        if str(row.get("status")).lower() == "up"
        and classify_node_role(row.get("role"), row.get("tx_read_only")) == "master"
    ]
    slaves = [
        row
        for row in rows
        if str(row.get("status")).lower() == "up"
        and classify_node_role(row.get("role"), row.get("tx_read_only")) == "slave"
    ]

    if len(masters) != 1:
        result["messages"] = [
            (
                "error",
                "Для безопасного переключения требуется ровно один активный master. "
                f"Сейчас обнаружено: {len(masters)}.",
            )
        ]
        return finalize(result)
    if not slaves:
        result["messages"] = [("error", "Не найден активный slave, на который можно безопасно перевести роль master.")]
        return finalize(result)

    old_master_row = masters[0]
    if normalized_target_master is None:
        new_master_row = slaves[0]
    else:
        slave_by_name = {str(row.get("node")): row for row in slaves}
        new_master_row = slave_by_name.get(normalized_target_master)
        if new_master_row is None:
            result["messages"] = [
                (
                    "error",
                    "Целевой узел для switchover не найден среди доступных slave. "
                    f"target_master='{normalized_target_master}', available_slaves={sorted(slave_by_name)}.",
                )
            ]
            result["diagnostics"] = [
                {
                    "step": "Выбор целевого slave",
                    "target_master": normalized_target_master,
                    "available_slaves": ", ".join(sorted(slave_by_name)) or "<none>",
                }
            ]
            return finalize(result)
    old_master = nodes_by_name.get(str(old_master_row["node"]))
    new_master = nodes_by_name.get(str(new_master_row["node"]))

    if old_master is None or new_master is None:
        result["messages"] = [("error", "Не удалось сопоставить роли узлов с конфигурацией кластера.")]
        return finalize(result)

    if not old_master.control_via_ssh or not old_master.ssh_host:
        result["messages"] = [("error", f"Для узла master '{old_master.name}' не настроено SSH-управление.")]
        return finalize(result)
    if not new_master.control_via_ssh or not new_master.ssh_host:
        result["messages"] = [("error", f"Для узла slave '{new_master.name}' не настроено SSH-управление.")]
        return finalize(result)

    messages: list[tuple[str, str]] = []
    diagnostics: list[dict[str, str]] = []
    if len(slaves) > 1:
        if normalized_target_master:
            messages.append(
                (
                    "info",
                    f"Обнаружено несколько slave-узлов ({len(slaves)}). "
                    f"Для переключения использован target_master='{new_master.name}'.",
                )
            )
        else:
            messages.append(
                (
                    "warning",
                    f"Обнаружено несколько slave-узлов ({len(slaves)}). "
                    f"Для переключения выбран '{new_master.name}'.",
                )
            )

    downtime_started_monotonic = time.monotonic()
    ok, output = run_node_action(old_master, "stop")
    diagnostics.append(
        {
            "step": "Остановка текущего master",
            "node": old_master.name,
            "action": "stop",
            "output": output,
        }
    )
    if not ok:
        messages.append(
            (
                "error",
                f"Не удалось остановить текущий master '{old_master.name}': {output or 'unknown error'}.",
            )
        )
        result["messages"] = messages
        result["diagnostics"] = diagnostics
        return finalize(result)

    promoted = False
    downtime_sec: float | None = None
    for _ in range(20):
        time.sleep(2)
        candidate_metrics = fetch_node_metrics(new_master, target_db)
        candidate_status = str(candidate_metrics.get("status", "")).lower()
        candidate_class = classify_node_role(
            candidate_metrics.get("role", ""),
            candidate_metrics.get("tx_read_only"),
        )
        if candidate_status == "up" and candidate_class == "master":
            promoted = True
            downtime_sec = round(max(time.monotonic() - downtime_started_monotonic, 0.0), 3)
            break

    if not promoted:
        messages.append(
            (
                "error",
                f"Узел '{new_master.name}' не принял роль master за отведённое время. "
                "Выполняем попытку вернуть сервис на прежнем master.",
            )
        )
        rollback_ok, rollback_output = run_node_action(old_master, "start")
        diagnostics.append(
            {
                "step": "Rollback: запуск прежнего master",
                "node": old_master.name,
                "action": "start",
                "output": rollback_output,
            }
        )
        if rollback_ok:
            messages.append(("warning", f"Сервис на '{old_master.name}' запущен обратно после неуспешного переключения."))
        else:
            messages.append(
                (
                    "error",
                    f"Не удалось вернуть сервис на '{old_master.name}': {rollback_output or 'unknown error'}.",
                )
        )
        result["messages"] = messages
        result["diagnostics"] = diagnostics
        result["downtime_sec"] = round(max(time.monotonic() - downtime_started_monotonic, 0.0), 3)
        return finalize(result)

    start_ok, start_output = run_node_action(old_master, "start")
    diagnostics.append(
        {
            "step": "Запуск бывшего master после промоушена standby",
            "node": old_master.name,
            "action": "start",
            "output": start_output,
        }
    )
    if not start_ok:
        messages.append(
            (
                "warning",
                f"Новый master: '{new_master.name}'. Не удалось запустить бывший master '{old_master.name}': "
                f"{start_output or 'unknown error'}.",
            )
        )
        result["messages"] = messages
        result["diagnostics"] = diagnostics
        result["downtime_sec"] = downtime_sec
        return finalize(result)

    for _ in range(20):
        time.sleep(2)
        old_master_metrics = fetch_node_metrics(old_master, target_db)
        old_status = str(old_master_metrics.get("status", "")).lower()
        old_class = classify_node_role(
            old_master_metrics.get("role", ""),
            old_master_metrics.get("tx_read_only"),
        )
        if old_status == "up" and old_class == "slave":
            messages.append(
                (
                    "success",
                    f"Переключение роли master выполнено: '{new_master.name}' стал master, "
                    f"'{old_master.name}' вернулся как slave.",
                )
            )
            result["messages"] = messages
            result["success"] = True
            result["roles"] = {"master": new_master.name, "slave": old_master.name}
            result["diagnostics"] = diagnostics
            result["downtime_sec"] = downtime_sec
            return finalize(result)

    messages.append(
        (
            "warning",
            f"Роль master переключена на '{new_master.name}', но узел '{old_master.name}' "
            "не подтвердил роль slave за отведённое время.",
        )
    )
    result["messages"] = messages
    result["diagnostics"] = diagnostics
    result["downtime_sec"] = downtime_sec
    return finalize(result)


def run_pending_service_operations(cluster: ClusterConfig) -> None:
    if st.session_state.pop("pending_reset_caches_action", False):
        with st.spinner("Очищаем page cache/dentries/inodes на нодах кластера..."):
            messages = reset_all_node_caches(cluster)
        if not messages:
            messages = [("info", "Очистка кэшей завершена, но узлы для обработки не найдены.")]
        st.session_state["pending_service_operation_messages"] = messages
    elif st.session_state.pop("pending_master_switchover_action", False):
        with st.spinner("Переключаем роль master между PostgreSQL-нодами..."):
            switchover_result = switchover_master_role(cluster)
            messages = switchover_result.get("messages", [])
        if not messages:
            switchover_result["messages"] = [("warning", "Операция переключения завершена без диагностических сообщений.")]
        st.session_state["master_switchover_result_payload"] = switchover_result
        st.session_state["show_master_switchover_result_dialog"] = True
        st.rerun()


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
            if confirm_cols[0].button("✅ Да, сбросить", key="confirm_reset_yes", width="stretch"):
                wg.reset_stats()
                st.session_state["pending_service_operation_messages"] = reset_server_stats(cluster)
                st.session_state["confirm_reset_counters_inline"] = False
                st.rerun()
            if confirm_cols[1].button("❌ Отмена", key="confirm_reset_no", width="stretch"):
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
        if confirm_cols[0].button("✅ Да, сбросить", key="confirm_reset_yes_dialog", width="stretch"):
            wg.reset_stats()
            st.session_state["pending_service_operation_messages"] = reset_server_stats(cluster)
            st.session_state["confirm_reset_counters_inline"] = False
            st.rerun()
        if confirm_cols[1].button("❌ Отмена", key="confirm_reset_no_dialog", width="stretch"):
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
            if confirm_cols[0].button("✅ Да, очистить кэши", key="confirm_reset_caches_yes", width="stretch"):
                request_reset_caches()
            if confirm_cols[1].button("❌ Отмена", key="confirm_reset_caches_no", width="stretch"):
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
        if confirm_cols[0].button("✅ Да, очистить кэши", key="confirm_reset_caches_yes_dialog", width="stretch"):
            request_reset_caches()
        if confirm_cols[1].button("❌ Отмена", key="confirm_reset_caches_no_dialog", width="stretch"):
            st.session_state["confirm_reset_caches_inline"] = False
            st.rerun()

    _dialog()


def open_master_switchover_dialog(cluster: ClusterConfig) -> None:
    if not st.session_state.get("confirm_master_switchover_inline", False):
        return

    if not hasattr(st, "dialog"):
        with st.sidebar.container(border=True):
            st.warning(
                "Подтвердите безопасное переключение роли master между двумя PostgreSQL-нодами. "
                "Сначала будет остановлен текущий master, затем ожидается автоматическое повышение standby."
            )
            confirm_cols = st.columns(2)
            if confirm_cols[0].button(
                "✅ Да, переключить master",
                key="confirm_master_switchover_yes",
                width="stretch",
            ):
                request_master_switchover()
            if confirm_cols[1].button("❌ Отмена", key="confirm_master_switchover_no", width="stretch"):
                st.session_state["confirm_master_switchover_inline"] = False
                st.rerun()
        return

    @st.dialog("Переключить роль master?")
    def _dialog() -> None:
        st.warning(
            "Будет выполнено контролируемое переключение роли master между двумя узлами кластера. "
            "Операция требует корректной настройки авто-failover и SSH-доступа."
        )
        confirm_cols = st.columns(2)
        if confirm_cols[0].button(
            "✅ Да, переключить master",
            key="confirm_master_switchover_yes_dialog",
            width="stretch",
        ):
            request_master_switchover()
        if confirm_cols[1].button("❌ Отмена", key="confirm_master_switchover_no_dialog", width="stretch"):
            st.session_state["confirm_master_switchover_inline"] = False
            st.rerun()

    _dialog()


def open_master_switchover_result_dialog() -> None:
    if not st.session_state.get("show_master_switchover_result_dialog", False):
        return

    payload = st.session_state.get("master_switchover_result_payload", {})
    is_success = bool(payload.get("success"))
    roles = payload.get("roles", {}) or {}
    diagnostics = payload.get("diagnostics", []) or []
    messages = payload.get("messages", []) or []

    @st.dialog("Результат переключения master")
    def _dialog() -> None:
        if is_success:
            st.success("Переключение выполнено штатно.")
            st.markdown(
                f"- **Текущий master:** `{roles.get('master', 'unknown')}`\n"
                f"- **Текущий slave:** `{roles.get('slave', 'unknown')}`"
            )
        else:
            st.error("Переключение завершилось с ошибкой. Ниже показаны команды и ответы.")
            for idx, item in enumerate(diagnostics, start=1):
                step = str(item.get("step", f"Шаг {idx}"))
                node_name = str(item.get("node", "unknown"))
                action = str(item.get("action", "unknown"))
                st.markdown(f"**{idx}. {step}** (`{node_name}` / `{action}`)")
                st.code(str(item.get("output", "")), language="bash")
            if not diagnostics:
                st.info("Команды не выполнялись: операция остановилась на этапе предварительных проверок.")

        for level, message in messages:
            notifier = getattr(st, level, None)
            if callable(notifier):
                notifier(message)
            else:
                st.info(message)

        if st.button("Закрыть", type="primary", width="stretch", key="close_master_switchover_result_dialog"):
            st.session_state["show_master_switchover_result_dialog"] = False
            st.session_state.pop("master_switchover_result_payload", None)
            st.rerun()

    _dialog()


def get_shared_workload_state(wg: WorkloadGenerator | None = None) -> dict[str, Any]:
    persisted = read_workload_status()
    return normalize_workload_status(persisted)


def sync_workload_settings_from_shared_state() -> None:
    persisted = normalize_workload_status(read_workload_status())
    shared_signature = {
        "revision": persisted.get("revision"),
        "mode": persisted.get("mode"),
        "clients": persisted.get("clients"),
        "threads_per_client": persisted.get("threads_per_client"),
        "read_ratio": persisted.get("read_ratio"),
        "desired_is_running": persisted.get("desired_is_running"),
        "is_running": persisted.get("is_running"),
        "applied_revision": persisted.get("applied_revision"),
    }
    if st.session_state.get("last_synced_workload_settings_signature") == shared_signature:
        return

    legacy_mode_mapping = {
        "single-node": "rw",
        "master-rw": "rw",
        "dual-read": "r",
        "master-rw-slave-r": "rw",
    }

    if "mode" in persisted:
        mode = legacy_mode_mapping.get(str(persisted["mode"]), str(persisted["mode"]))
        st.session_state["persist_load_mode"] = mode
        st.session_state["load_mode"] = mode

    if "clients" in persisted:
        clients = int(persisted["clients"])
        st.session_state["persist_load_clients"] = clients
        st.session_state["load_clients"] = clients
        st.session_state["load_clients_input"] = clients

    if "threads_per_client" in persisted:
        threads_per_client = int(persisted["threads_per_client"])
        st.session_state["persist_load_threads_per_client"] = threads_per_client
        st.session_state["load_threads_per_client"] = threads_per_client
        st.session_state["load_threads_per_client_input"] = threads_per_client

    if "read_ratio" in persisted:
        read_ratio = float(persisted["read_ratio"])
        st.session_state["persist_load_read_ratio"] = read_ratio
        st.session_state["load_read_ratio"] = read_ratio

    st.session_state["last_synced_workload_settings_signature"] = shared_signature


def sync_workload_integer_control(source_key: str, target_key: str) -> None:
    value = int(st.session_state[source_key])
    st.session_state[target_key] = value
    st.session_state[f"persist_{target_key}"] = value


def render_sidebar(cluster: ClusterConfig, wg: WorkloadGenerator) -> dict[str, Any]:
    shared_state = get_shared_workload_state(wg)
    globally_running = bool(shared_state.get("is_running"))
    desired_is_running = bool(shared_state.get("desired_is_running"))

    st.sidebar.header("Управление нагрузкой")
    toggle_label = "⏹ Остановить нагрузку" if desired_is_running else "▶️ Запустить нагрузку"
    toggle_type = "primary" if desired_is_running else "secondary"
    if st.sidebar.button(toggle_label, key="sidebar_toggle_load", width="stretch", type=toggle_type):
        start_profile = {
            "mode": st.session_state.get("load_mode", "rw"),
            "clients": int(st.session_state.get("load_clients", 1)),
            "threads_per_client": int(st.session_state.get("load_threads_per_client", 1)),
            "read_ratio": float(st.session_state.get("load_read_ratio", 0.7)),
        }
        request_shared_workload_profile_update(
            start_profile,
            desired_is_running=not desired_is_running,
            expected_revision=int(shared_state.get("revision", 0)),
        )
        st.rerun()

    st.sidebar.divider()
    st.sidebar.header("Профиль нагрузки")

    legacy_mode_mapping = {
        "single-node": "rw",
        "master-rw": "rw",
        "dual-read": "r",
        "master-rw-slave-r": "rw",
    }

    defaults = {
        "load_mode": "rw",
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

    for key in ("load_clients", "load_threads_per_client"):
        input_key = f"{key}_input"
        if input_key not in st.session_state:
            st.session_state[input_key] = int(st.session_state[key])

    mode = st.sidebar.selectbox(
        "Режим",
        options=["r", "rw"],
        help=(
            "r: только чтение через VIP кластера; "
            "rw: смешанная нагрузка чтение/запись через VIP кластера"
        ),
        key="load_mode",
    )
    clients = st.sidebar.slider(
        "Количество клиентов",
        min_value=1,
        max_value=64,
        step=1,
        key="load_clients",
        on_change=sync_workload_integer_control,
        args=("load_clients", "load_clients_input"),
    )
    clients = st.sidebar.number_input(
        "Клиенты (точное значение)",
        min_value=1,
        max_value=64,
        step=1,
        key="load_clients_input",
        on_change=sync_workload_integer_control,
        args=("load_clients_input", "load_clients"),
    )

    threads_per_client = st.sidebar.slider(
        "Потоков на клиента",
        min_value=1,
        max_value=8,
        step=1,
        key="load_threads_per_client",
        on_change=sync_workload_integer_control,
        args=("load_threads_per_client", "load_threads_per_client_input"),
    )
    threads_per_client = st.sidebar.number_input(
        "Потоки на клиента (точное значение)",
        min_value=1,
        max_value=8,
        step=1,
        key="load_threads_per_client_input",
        on_change=sync_workload_integer_control,
        args=("load_threads_per_client_input", "load_threads_per_client"),
    )

    requested_workers = int(clients) * int(threads_per_client)
    total_workers = min(MAX_WORKERS, requested_workers)
    if requested_workers > MAX_WORKERS:
        st.sidebar.caption(
            "Рабочие потоки генератора: "
            f"реально={total_workers}, настроено={requested_workers} (лимит: {MAX_WORKERS})"
        )
    else:
        st.sidebar.caption(f"Рабочие потоки генератора: реально={total_workers}, настроено={requested_workers}")
    read_ratio = st.sidebar.slider("Доля чтения", 0.0, 1.0, step=0.05, key="load_read_ratio")
    if mode == "r":
        st.sidebar.caption("Для профилей только на чтение параметр `Доля чтения` не используется: генератор выполняет только чтение.")
    auto_refresh = st.sidebar.checkbox("Автообновление", key="load_auto_refresh")

    st.session_state.persist_load_mode = mode
    st.session_state.persist_load_clients = int(clients)
    st.session_state.persist_load_threads_per_client = int(threads_per_client)
    st.session_state.persist_load_read_ratio = read_ratio
    st.session_state.persist_load_auto_refresh = auto_refresh

    current_profile = {
        "mode": mode,
        "clients": int(clients),
        "threads_per_client": int(threads_per_client),
        "read_ratio": float(read_ratio),
    }
    if any(shared_state.get(key) != current_profile[key] for key in WORKLOAD_STATUS_SYNC_KEYS):
        request_shared_workload_profile_update(
            current_profile,
            expected_revision=int(shared_state.get("revision", 0)),
        )

    st.sidebar.divider()
    st.sidebar.markdown("#### Сервисные операции")
    if st.sidebar.button("🔄 Сбросить счётчики", width="stretch", key="sidebar_reset_counters"):
        st.session_state["confirm_reset_counters_inline"] = True
        st.rerun()
    if st.sidebar.button("🧹 Сбросить кэши", width="stretch", key="sidebar_reset_caches"):
        st.session_state["confirm_reset_caches_inline"] = True
        st.rerun()
    if st.sidebar.button("🔁 Переключить master", width="stretch", key="sidebar_master_switchover"):
        st.session_state["confirm_master_switchover_inline"] = True
        st.rerun()

    open_reset_counters_dialog(cluster, wg)
    open_reset_caches_dialog(cluster)
    open_master_switchover_dialog(cluster)
    open_master_switchover_result_dialog()
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

            st.write(f"**{node.name}**")
            if st.button(button_label, key=f"failure-toggle-{node.name}", width="stretch"):
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
                            record_command("sql_maintenance", stmt, node=node.name, dsn=node.dsn)
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
    command_rendered = shlex.join(cmd)
    record_command("ssh_action", command_rendered, node=node.name, host=node.ssh_host, action=remote_cmd)
    LOGGER.info(
        "Executing SSH command | node=%s host=%s timeout_sec=%s command=%s",
        node.name,
        node.ssh_host,
        timeout,
        command_rendered,
    )
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        LOGGER.error("SSH action timeout for node=%s cmd=%s", node.name, remote_cmd)
        return False, "SSH command timeout"
    LOGGER.info(
        "SSH command finished | node=%s host=%s returncode=%s command=%s",
        node.name,
        node.ssh_host,
        proc.returncode,
        command_rendered,
    )

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


def render_metrics(
    cluster: ClusterConfig,
    wg: WorkloadGenerator,
    collector: BackgroundMetricsCollector,
    shared_state: dict[str, Any] | None = None,
) -> None:
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
            "host": "Host",
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
    metrics_meta = f"Целевая база данных для нагрузки: {target_db}"
    if snap["updated_at"] is not None:
        metrics_meta += f" | Последнее обновление метрик: {snap['updated_at'].strftime('%H:%M:%S')}"
    st.caption(metrics_meta)
    row_height_px = 35
    header_height_px = 38
    table_height = max(105, min(190, header_height_px + len(localized_df) * row_height_px))
    st.dataframe(localized_df, width="stretch", height=table_height, hide_index=True)
    st.caption(
        "Источник данных: системные представления PostgreSQL (pg_stat_database, pg_stat_activity, pg_locks, pg_last_xact_replay_timestamp); столбцы с задержками, очередью и нагрузкой на диск заполняются по данным SSH-команды iostat -dx, если этот сбор включён."
    )
    if snap["error"]:
        st.warning(f"Ошибка фонового сборщика метрик: {snap['error']}")

    if rows and all(row.get("status") != "up" for row in rows):
        LOGGER.warning(
            "No DB connections to any node. Errors: %s",
            {row.get("node"): row.get("error") for row in rows},
        )
        st.warning("Нет подключений к узлам БД. Проверьте доступность PostgreSQL и параметры DSN/SSH в конфиге.")
    if shared_state is None:
        shared_state = get_shared_workload_state(wg)
    is_running = bool(shared_state.get("is_running"))
    desired_is_running = bool(shared_state.get("desired_is_running"))
    desired_status = "🟢 ЗАПУСК ЗАПРОШЕН" if desired_is_running else "🔴 ОСТАНОВКА ЗАПРОШЕНА"
    runtime_status = "🟢 РАБОТАЕТ" if is_running else "🔴 ОСТАНОВЛЕН"
    st.info(
        "Статус генератора нагрузки: "
        f"{runtime_status} | Желаемое состояние: {desired_status}"
    )
    clients = int(shared_state.get("clients", st.session_state.get("load_clients", 1)))
    threads_per_client = int(shared_state.get("threads_per_client", st.session_state.get("load_threads_per_client", 1)))
    requested_workers = int(shared_state.get("requested_threads", clients * threads_per_client))
    actual_workers = min(MAX_WORKERS, requested_workers)
    workers_caption = f"рабочих потоков: реально={actual_workers}, настроено={requested_workers}"
    if requested_workers > MAX_WORKERS:
        workers_caption += f" (лимит: {MAX_WORKERS})"

    st.caption(
        f"Параметры нагрузки: клиентов настроено={clients}, "
        f"потоков на клиента настроено={threads_per_client}, "
        f"{workers_caption}"
    )
    last_error = shared_state.get("last_error")
    if last_error:
        st.warning(f"Последняя ошибка применения общего состояния нагрузки: {last_error}")

    st.subheader("Статистика нагрузки")

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
            st.caption("Запросы и ошибки")
            st.line_chart(query_error_moment_df, height=320)
            st.caption("Источник данных: генератор нагрузки (счётчики read_tx, write_tx, errors; на графике показана разница между соседними срезами).")

        with chart_cols[1]:
            st.caption("Блокировки и активные запросы")
            st.line_chart(chart_df[["active_locks", "active_queries"]], height=320)
            st.caption("Источник данных: системные представления PostgreSQL pg_locks и pg_stat_activity.")

        with chart_cols[2]:
            st.caption("Нагрузка на диски по узлам")
            role_aliases = {
                "master": {"master", "primary", "leader"},
                "slave": {"slave", "replica", "standby"},
            }

            master_read = pd.Series(0.0, index=chart_df.index)
            master_write = pd.Series(0.0, index=chart_df.index)
            slave_read = pd.Series(0.0, index=chart_df.index)
            slave_write = pd.Series(0.0, index=chart_df.index)

            for node in cluster.nodes:
                role_col = f"role_{node.name}"
                read_col = f"disk_read_kb_s_{node.name}"
                write_col = f"disk_write_kb_s_{node.name}"
                if role_col not in chart_df.columns:
                    continue
                role_series = chart_df[role_col].astype(str).str.lower()
                read_series = chart_df.get(read_col, pd.Series(0.0, index=chart_df.index)).astype(float)
                write_series = chart_df.get(write_col, pd.Series(0.0, index=chart_df.index)).astype(float)

                master_mask = role_series.isin(role_aliases["master"])
                slave_mask = role_series.isin(role_aliases["slave"])
                master_read = master_read.add(read_series.where(master_mask, 0.0), fill_value=0.0)
                master_write = master_write.add(write_series.where(master_mask, 0.0), fill_value=0.0)
                slave_read = slave_read.add(read_series.where(slave_mask, 0.0), fill_value=0.0)
                slave_write = slave_write.add(write_series.where(slave_mask, 0.0), fill_value=0.0)

            disk_chart_data: dict[str, pd.Series] = {
                "master_read_kb_s": master_read,
                "master_write_kb_s": master_write,
                "slave_read_kb_s": slave_read,
                "slave_write_kb_s": slave_write,
            }

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
    st.title(APP_TITLE)

    st.session_state.setdefault("cfg_path", "config/cluster.json")
    raw_cfg_path = str(st.session_state["cfg_path"]).strip()
    if not raw_cfg_path:
        st.error("Укажите путь к конфигурационному файлу.")
        st.stop()

    cfg_path = Path(raw_cfg_path)
    if not cfg_path.exists():
        st.error(f"Конфигурационный файл не найден: {cfg_path}")
        st.stop()
    if not cfg_path.is_file():
        st.error(f"Ожидался JSON-файл конфигурации, но указан каталог: {cfg_path}")
        st.stop()

    try:
        cluster = load_cluster_config(cfg_path)
    except Exception as exc:
        LOGGER.exception("Failed to parse config file: %s", cfg_path)
        st.error(f"Не удалось прочитать конфигурационный файл: {exc}")
        st.stop()

    LOGGER.info("App started with config_path=%s nodes=%s", cfg_path, len(cluster.nodes))

    sync_workload_settings_from_shared_state()
    render_pending_workload_ui_messages()

    if "workload_generator" not in st.session_state:
        st.session_state.workload_generator = get_shared_workload_generator()
    if "metrics_collector" not in st.session_state:
        st.session_state.metrics_collector = BackgroundMetricsCollector()

    wg: WorkloadGenerator = st.session_state.workload_generator
    collector: BackgroundMetricsCollector = st.session_state.metrics_collector
    profile = render_sidebar(cluster, wg)
    shared_state = reconcile_shared_workload_runtime(
        cluster,
        wg,
        float(st.session_state.get("target_size_gb", DEFAULT_WORKLOAD_DB_SIZE_GB)),
    )
    effective_profile = {
        **profile,
        "mode": str(shared_state.get("mode", profile["mode"])),
        "clients": int(shared_state.get("clients", profile["clients"])),
        "threads_per_client": int(shared_state.get("threads_per_client", profile["threads_per_client"])),
        "read_ratio": float(shared_state.get("read_ratio", profile["read_ratio"])),
    }

    current_signature = build_cluster_signature(cluster)
    if collector.cluster_signature != current_signature or not collector.running:
        collector.start(cluster, wg, effective_profile["mode"])
    else:
        collector.set_mode(effective_profile["mode"])

    render_controls(cluster, collector)
    render_metrics(cluster, wg, collector, shared_state)

    st.divider()
    st.text_input("Путь к конфигурационному файлу", key="cfg_path")

    if st.button("🔄 Обновить", key="scenario_refresh_now", help="Немедленно обновить данные", width="stretch"):
        st.rerun()

    if profile["auto_refresh"]:
        time.sleep(cluster.poll_interval_sec)
        st.rerun()


if __name__ == "__main__":
    main()
