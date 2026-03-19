from __future__ import annotations

import json
import shlex
import subprocess
import threading
import time
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Any, Callable
from urllib.parse import parse_qs, urlparse

import altair as alt
import pandas as pd
import psycopg
from psycopg.conninfo import conninfo_to_dict
import streamlit as st

try:
    from streamlit_autorefresh import st_autorefresh
except ImportError:
    st_autorefresh = None

from logging_utils import setup_file_logger
from ui_styles import apply_base_page_styles
from workload_status_store import read_workload_status

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
    disk_device: str | None = None


@dataclass
class ClusterConfig:
    nodes: list[NodeConfig]


def load_cluster_config(path: Path) -> ClusterConfig:
    if not path.exists():
        raise FileNotFoundError(f"Конфиг не найден: {path}")
    if not path.is_file():
        raise ValueError(f"Ожидался JSON-файл конфига, но указан каталог: {path}")
    cfg = json.loads(path.read_text(encoding="utf-8"))
    allowed_keys = {field.name for field in fields(NodeConfig)}
    nodes = [NodeConfig(**{k: v for k, v in item.items() if k in allowed_keys}) for item in cfg.get("nodes", [])]
    return ClusterConfig(nodes=nodes)


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


def resolve_cluster_roles(cluster: ClusterConfig) -> tuple[NodeConfig, NodeConfig | None]:
    resolved_primary: NodeConfig | None = None
    resolved_standby: NodeConfig | None = None

    for node in cluster.nodes:
        try:
            with psycopg.connect(node.dsn, connect_timeout=2, autocommit=True) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT pg_is_in_recovery()")
                    row = cur.fetchone()
                    is_in_recovery = bool(row[0]) if row else False
        except Exception:
            LOGGER.warning("Не удалось определить фактическую роль узла=%s, используем role_hint", node.name)
            continue

        if is_in_recovery:
            resolved_standby = resolved_standby or node
        else:
            resolved_primary = node

    primary = resolved_primary or next(
        (n for n in cluster.nodes if n.role_hint.lower() in {"master", "primary", "leader"}),
        cluster.nodes[0],
    )
    standby = resolved_standby or next(
        (n for n in cluster.nodes if n is not primary and n.role_hint.lower() in {"slave", "replica", "standby"}),
        None,
    )
    return primary, standby


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


def run_ssh_metric(node: NodeConfig, remote_cmd: str, timeout_sec: int = 8) -> str | None:
    if not node.control_via_ssh or not node.ssh_host:
        return None
    cmd = build_ssh_command(node, remote_cmd)
    log_cmd = " ".join(shlex.quote(part) for part in cmd)
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_sec)
    except subprocess.TimeoutExpired:
        LOGGER.error("SSH metric timeout | node=%s cmd=%s", node.name, log_cmd)
        return None
    except Exception:
        LOGGER.exception("SSH metric failed to execute | node=%s cmd=%s", node.name, log_cmd)
        return None
    if proc.returncode != 0:
        LOGGER.error("SSH metric error | node=%s rc=%s stdout=%r stderr=%r cmd=%s", node.name, proc.returncode, proc.stdout.strip(), proc.stderr.strip(), log_cmd)
        return None
    output = proc.stdout.strip()
    return output or None


def fetch_snapshot(primary: NodeConfig, standby: NodeConfig | None, target_db: str) -> dict[str, Any]:
    snapshot: dict[str, Any] = {
        "timestamp": pd.Timestamp.now(tz="Europe/Moscow"),
        "xact_commit": None,
        "xact_rollback": None,
        "latency_p95_ms": 0.0,
        "active": 0,
        "idle": 0,
        "idle_xact": 0,
        "waiting": 0,
        "wal_bytes": None,
        "replay_lag_sec": None,
        "cpu_primary": None,
        "cpu_standby": None,
        "disk_read_kb_s": None,
        "disk_write_kb_s": None,
    }

    try:
        with psycopg.connect(primary.dsn, connect_timeout=3, autocommit=True) as conn:
            with conn.cursor() as cur:
                effective_target_db = resolve_metrics_target_db(cur, target_db)
                snapshot.update(fetch_transaction_counters(cur, effective_target_db))
                snapshot.update(fetch_session_metrics(cur))
                snapshot["wal_bytes"] = fetch_wal_bytes(cur)
                snapshot["replay_lag_sec"] = fetch_replication_lag(cur)
                snapshot["latency_p95_ms"] = fetch_latency_p95_ms(cur)
    except Exception:
        LOGGER.exception("Не удалось собрать метрики PostgreSQL для узла=%s", primary.name)

    snapshot["cpu_primary"] = fetch_cpu_pct(primary)
    if standby:
        snapshot["cpu_standby"] = fetch_cpu_pct(standby)

    disk_metrics = fetch_disk_io(primary)
    snapshot["disk_read_kb_s"] = disk_metrics["read_kb_s"]
    snapshot["disk_write_kb_s"] = disk_metrics["write_kb_s"]

    return snapshot


def resolve_metrics_target_db(cur: psycopg.Cursor[Any], target_db: str) -> str | None:
    requested_db = str(target_db).strip()
    if requested_db:
        try:
            cur.execute("SELECT 1 FROM pg_stat_database WHERE datname = %s", (requested_db,))
            if cur.fetchone():
                return requested_db
            LOGGER.warning(
                "База %s не найдена в pg_stat_database, используем агрегирование по всем пользовательским БД",
                requested_db,
            )
        except Exception:
            LOGGER.exception("Не удалось проверить наличие БД %s в pg_stat_database", requested_db)
    return None


def fetch_transaction_counters(cur: psycopg.Cursor[Any], target_db: str | None) -> dict[str, int | None]:
    try:
        if target_db:
            cur.execute(
                """
                SELECT xact_commit, xact_rollback
                FROM pg_stat_database
                WHERE datname = %s
                """,
                (target_db,),
            )
        else:
            cur.execute(
                """
                SELECT COALESCE(SUM(xact_commit), 0), COALESCE(SUM(xact_rollback), 0)
                FROM pg_stat_database
                WHERE datistemplate = false
                """
            )
        row = cur.fetchone()
        if row:
            return {
                "xact_commit": int(row[0] or 0),
                "xact_rollback": int(row[1] or 0),
            }
    except Exception:
        LOGGER.exception("Не удалось прочитать счётчики транзакций для target_db=%s", target_db)
    return {"xact_commit": None, "xact_rollback": None}


def fetch_session_metrics(cur: psycopg.Cursor[Any]) -> dict[str, int]:
    try:
        cur.execute(
            """
            SELECT
                COUNT(*) FILTER (WHERE state = 'active') AS active,
                COUNT(*) FILTER (WHERE state = 'idle') AS idle,
                COUNT(*) FILTER (WHERE state = 'idle in transaction') AS idle_xact,
                COUNT(*) FILTER (WHERE state = 'active' AND wait_event_type IS NOT NULL) AS waiting
            FROM pg_stat_activity
            WHERE backend_type = 'client backend'
            """
        )
        row = cur.fetchone()
        if row:
            return {
                "active": int(row[0] or 0),
                "idle": int(row[1] or 0),
                "idle_xact": int(row[2] or 0),
                "waiting": int(row[3] or 0),
            }
    except Exception:
        LOGGER.exception("Не удалось прочитать метрики сессий")
    return {"active": 0, "idle": 0, "idle_xact": 0, "waiting": 0}


def fetch_wal_bytes(cur: psycopg.Cursor[Any]) -> int | None:
    try:
        cur.execute("SELECT wal_bytes FROM pg_stat_wal")
        row = cur.fetchone()
        if row and row[0] is not None:
            return int(row[0])
    except Exception:
        LOGGER.warning("pg_stat_wal недоступен, пробуем fallback через pg_current_wal_lsn()")

    fallback_queries = [
        "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')",
        "SELECT pg_xlog_location_diff(pg_current_xlog_location(), '0/0')",
    ]
    for query in fallback_queries:
        try:
            cur.execute(query)
            row = cur.fetchone()
            if row and row[0] is not None:
                return int(float(row[0]))
        except Exception:
            continue

    LOGGER.error("Не удалось прочитать объём WAL ни через pg_stat_wal, ни через LSN fallback")
    return None


def fetch_replication_lag(cur: psycopg.Cursor[Any]) -> float | None:
    try:
        cur.execute(
            """
            SELECT COALESCE(MAX(EXTRACT(EPOCH FROM replay_lag)), 0)
            FROM pg_stat_replication
            """
        )
        row = cur.fetchone()
        return float(row[0] or 0) if row else 0.0
    except Exception:
        LOGGER.exception("Не удалось прочитать lag репликации")
        return None


def fetch_latency_p95_ms(cur: psycopg.Cursor[Any]) -> float:
    try:
        cur.execute(
            """
            SELECT
                percentile_cont(0.95) WITHIN GROUP (
                    ORDER BY EXTRACT(EPOCH FROM (clock_timestamp() - query_start)) * 1000.0
                )
            FROM pg_stat_activity
            WHERE backend_type = 'client backend'
              AND state = 'active'
              AND query_start IS NOT NULL
            """
        )
        row = cur.fetchone()
        if row and row[0] is not None:
            return float(row[0])
    except Exception:
        LOGGER.exception("Не удалось прочитать latency p95")
    return 0.0


def fetch_sessions_snapshot(primary: NodeConfig) -> dict[str, Any]:
    snapshot: dict[str, Any] = {
        "timestamp": pd.Timestamp.now(tz="Europe/Moscow"),
        "active": 0,
        "idle": 0,
        "idle_xact": 0,
        "waiting": 0,
    }
    try:
        with psycopg.connect(primary.dsn, connect_timeout=3, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        COUNT(*) FILTER (WHERE state = 'active') AS active,
                        COUNT(*) FILTER (WHERE state = 'idle') AS idle,
                        COUNT(*) FILTER (WHERE state = 'idle in transaction') AS idle_xact,
                        COUNT(*) FILTER (WHERE state = 'active' AND wait_event_type IS NOT NULL) AS waiting
                    FROM pg_stat_activity
                    WHERE backend_type = 'client backend'
                    """
                )
                row = cur.fetchone()
                if row:
                    snapshot["active"] = int(row[0] or 0)
                    snapshot["idle"] = int(row[1] or 0)
                    snapshot["idle_xact"] = int(row[2] or 0)
                    snapshot["waiting"] = int(row[3] or 0)
    except Exception:
        LOGGER.exception("Не удалось собрать метрики сессий PostgreSQL для узла=%s", primary.name)
    return snapshot


def fetch_replication_lag_snapshot(primary: NodeConfig) -> dict[str, Any]:
    snapshot: dict[str, Any] = {"timestamp": pd.Timestamp.now(tz="Europe/Moscow"), "replay_lag_sec": None}
    try:
        with psycopg.connect(primary.dsn, connect_timeout=3, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COALESCE(MAX(EXTRACT(EPOCH FROM replay_lag)), 0)
                    FROM pg_stat_replication
                    """
                )
                row = cur.fetchone()
                snapshot["replay_lag_sec"] = float(row[0] or 0) if row else 0.0
    except Exception:
        LOGGER.exception("Не удалось собрать lag репликации PostgreSQL для узла=%s", primary.name)
    return snapshot


def fetch_cpu_snapshot(primary: NodeConfig, standby: NodeConfig | None) -> dict[str, Any]:
    snapshot: dict[str, Any] = {
        "timestamp": pd.Timestamp.now(tz="Europe/Moscow"),
        "cpu_primary": fetch_cpu_pct(primary),
        "cpu_standby": fetch_cpu_pct(standby) if standby else None,
    }
    return snapshot


def fetch_cpu_pct(node: NodeConfig) -> float | None:
    output = run_ssh_metric(node, 'bash -lc "grep -m1 \'^cpu \' /proc/stat; sleep 1; grep -m1 \'^cpu \' /proc/stat"')
    if output is None:
        return None

    lines = [line.strip() for line in output.splitlines() if line.strip()]
    if len(lines) < 2:
        LOGGER.warning("CPU metric parse failed: expected 2 lines | node=%s output=%r", node.name, output)
        return None

    def parse_cpu_line(line: str) -> tuple[float, float] | None:
        parts = line.split()
        if len(parts) < 8 or parts[0] != "cpu":
            return None
        try:
            counters = [float(value) for value in parts[1:] if value]
        except ValueError:
            return None
        total = sum(counters)
        idle = counters[3] + counters[4] if len(counters) > 4 else counters[3]
        return total, idle

    first = parse_cpu_line(lines[0])
    second = parse_cpu_line(lines[1])
    if first is None or second is None:
        LOGGER.warning("CPU metric parse failed: malformed /proc/stat | node=%s output=%r", node.name, output)
        return None

    total_delta = second[0] - first[0]
    idle_delta = second[1] - first[1]
    if total_delta <= 0:
        return None

    usage = (total_delta - idle_delta) * 100.0 / total_delta
    return max(0.0, min(100.0, round(usage, 2)))


def fetch_disk_io(node: NodeConfig) -> dict[str, float | None]:
    metrics = {"read_kb_s": None, "write_kb_s": None}
    output = run_ssh_metric(node, 'bash -lc "LC_ALL=C iostat -dx 1 2"')
    if output is None:
        return metrics

    sections = [section.strip() for section in output.split("\n\n") if section.strip() and "Device" in section]
    if not sections:
        return metrics

    lines = [line.strip() for line in sections[-1].splitlines() if line.strip()]
    header_idx = next((idx for idx, line in enumerate(lines) if line.startswith("Device")), None)
    if header_idx is None or header_idx + 1 >= len(lines):
        return metrics

    headers = lines[header_idx].split()
    rows = [line.split() for line in lines[header_idx + 1 :] if not line.startswith("avg-cpu")]
    if node.disk_device:
        requested_device = node.disk_device.strip().removeprefix("/dev/")
        aliases = {requested_device, f"/dev/{requested_device}"}
        exact_rows = [row for row in rows if row and row[0] in aliases]
        prefix_rows = [
            row for row in rows if row and (row[0].startswith(requested_device) or row[0].startswith(f"/dev/{requested_device}"))
        ]
        physical_rows = [
            row
            for row in rows
            if row and not row[0].startswith(("loop", "ram", "sr", "fd", "md127"))
        ]
        if exact_rows:
            rows = exact_rows
        elif prefix_rows:
            rows = prefix_rows
        else:
            LOGGER.warning(
                "Не найдено устройство %s в выводе iostat для узла=%s, используем агрегирование по всем дискам",
                node.disk_device,
                node.name,
            )
            rows = physical_rows or rows
    if not rows:
        return metrics

    totals = {"read_kb_s": 0.0, "write_kb_s": 0.0}
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

    for row in rows:
        if len(row) < len(headers):
            continue
        values_by_header = dict(zip(headers[1:], row[1:]))
        read_value, read_key = parse_metric(values_by_header, ["rkB/s", "rKB/s", "kB_read/s", "rMB/s"])
        write_value, write_key = parse_metric(values_by_header, ["wkB/s", "wKB/s", "kB_wrtn/s", "wMB/s"])

        if read_value is not None:
            totals["read_kb_s"] += read_value * 1024 if read_key == "rMB/s" else read_value
            found["read_kb_s"] = True
        if write_value is not None:
            totals["write_kb_s"] += write_value * 1024 if write_key == "wMB/s" else write_value
            found["write_kb_s"] = True

    metrics["read_kb_s"] = totals["read_kb_s"] if found["read_kb_s"] else None
    metrics["write_kb_s"] = totals["write_kb_s"] if found["write_kb_s"] else None
    return metrics



def rate(curr: float | int | None, prev: float | int | None, dt_sec: float) -> float | None:
    if curr is None or prev is None or dt_sec <= 0:
        return None
    delta = float(curr) - float(prev)
    if delta < 0:
        return None
    return delta / dt_sec


def build_timeseries(history: list[dict[str, Any]], interval_minutes: int) -> dict[str, pd.DataFrame]:
    if len(history) < 2:
        return {}

    cutoff = pd.Timestamp.now(tz="Europe/Moscow") - pd.Timedelta(minutes=interval_minutes)
    points = [row for row in history if row["timestamp"] >= cutoff]
    if len(points) < 2:
        return {}

    tps_rows: list[dict[str, Any]] = []
    latency_rows: list[dict[str, Any]] = []
    sessions_rows: list[dict[str, Any]] = []
    cpu_rows: list[dict[str, Any]] = []
    disk_rows: list[dict[str, Any]] = []
    wal_rows: list[dict[str, Any]] = []

    for idx, (prev, curr) in enumerate(zip(points, points[1:])):
        ts = curr["timestamp"]
        dt_sec = (curr["timestamp"] - prev["timestamp"]).total_seconds()
        if dt_sec <= 0:
            continue

        commit_rate = rate(curr["xact_commit"], prev["xact_commit"], dt_sec)
        rollback_rate = rate(curr["xact_rollback"], prev["xact_rollback"], dt_sec)
        tps_rows.extend(
            [
                {"timestamp": ts, "metric": "COMMIT/s", "value": commit_rate},
                {"timestamp": ts, "metric": "ROLLBACK/s", "value": rollback_rate},
            ]
        )

        if curr["latency_p95_ms"] is not None:
            latency_rows.append({"timestamp": ts, "metric": "p95", "value": curr["latency_p95_ms"]})

        sessions_rows.extend(
            [
                {"timestamp": ts, "state": "active", "value": curr["active"]},
                {"timestamp": ts, "state": "waiting", "value": curr["waiting"]},
                {"timestamp": ts, "state": "idle in xact", "value": curr["idle_xact"]},
                {"timestamp": ts, "state": "idle", "value": curr["idle"]},
            ]
        )

        cpu_rows.extend(
            [
                {"timestamp": ts, "node": "Primary CPU", "value": curr["cpu_primary"]},
                {"timestamp": ts, "node": "Standby CPU", "value": curr["cpu_standby"]},
            ]
        )

        disk_rows.extend(
            [
                {"timestamp": ts, "metric": "Read", "value": curr["disk_read_kb_s"]},
                {"timestamp": ts, "metric": "Write", "value": curr["disk_write_kb_s"]},
            ]
        )

        wal_rate = rate(curr["wal_bytes"], prev["wal_bytes"], dt_sec)
        wal_rows.append({"timestamp": ts, "metric": "WAL MB/s", "value": wal_rate / 1024 / 1024 if wal_rate is not None else None})

    result = {
        "tps": pd.DataFrame(tps_rows),
        "latency": pd.DataFrame(latency_rows),
        "sessions": pd.DataFrame(sessions_rows),
        "cpu": pd.DataFrame(cpu_rows),
        "disk": pd.DataFrame(disk_rows),
        "wal": pd.DataFrame(wal_rows),
    }

    return result


class AsyncMetricsCollector:
    """Фоновый сбор метрик без блокировки интерфейса Streamlit."""

    def __init__(self, interval_sec: int, history_limit: int) -> None:
        self.interval_sec = interval_sec
        self.history_limit = history_limit
        self._history: list[dict[str, Any]] = []
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self, collect_fn: Callable[[], dict[str, Any]], start_delay_sec: float = 0.0) -> None:
        if self._thread and self._thread.is_alive():
            return

        self._stop_event.clear()

        def worker() -> None:
            if start_delay_sec > 0:
                self._stop_event.wait(start_delay_sec)
            while not self._stop_event.is_set():
                snapshot = collect_fn()
                with self._lock:
                    self._history.append(snapshot)
                    if len(self._history) > self.history_limit:
                        self._history = self._history[-self.history_limit:]
                self._stop_event.wait(self.interval_sec)

        self._thread = threading.Thread(target=worker, daemon=True, name="metrics-collector")
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)
        self._thread = None

    def update(self, interval_sec: int, history_limit: int) -> None:
        self.interval_sec = interval_sec
        self.history_limit = history_limit

    def collect_once(self, collect_fn: Callable[[], dict[str, Any]]) -> None:
        snapshot = collect_fn()
        with self._lock:
            self._history.append(snapshot)
            if len(self._history) > self.history_limit:
                self._history = self._history[-self.history_limit:]

    def history(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._history)


@st.cache_resource
def get_async_collector(session_key: str, interval_sec: int, history_limit: int) -> AsyncMetricsCollector:
    collector = AsyncMetricsCollector(interval_sec=interval_sec, history_limit=history_limit)
    return collector


def build_sessions_df(history: list[dict[str, Any]], interval_minutes: int) -> pd.DataFrame:
    cutoff = pd.Timestamp.now(tz="Europe/Moscow") - pd.Timedelta(minutes=interval_minutes)
    rows: list[dict[str, Any]] = []
    for item in history:
        if item["timestamp"] < cutoff:
            continue
        rows.extend(
            [
                {"timestamp": item["timestamp"], "state": "active", "value": item.get("active")},
                {"timestamp": item["timestamp"], "state": "waiting", "value": item.get("waiting")},
                {"timestamp": item["timestamp"], "state": "idle in xact", "value": item.get("idle_xact")},
                {"timestamp": item["timestamp"], "state": "idle", "value": item.get("idle")},
            ]
        )
    return pd.DataFrame(rows)


def build_cpu_df(history: list[dict[str, Any]], interval_minutes: int) -> pd.DataFrame:
    cutoff = pd.Timestamp.now(tz="Europe/Moscow") - pd.Timedelta(minutes=interval_minutes)
    rows: list[dict[str, Any]] = []
    for item in history:
        if item["timestamp"] < cutoff:
            continue
        rows.extend(
            [
                {"timestamp": item["timestamp"], "node": "Primary CPU", "value": item.get("cpu_primary")},
                {"timestamp": item["timestamp"], "node": "Standby CPU", "value": item.get("cpu_standby")},
            ]
        )
    return pd.DataFrame(rows)



def theme_chart(chart: alt.Chart) -> alt.Chart:
    return (
        chart.configure_view(strokeOpacity=0)
        .configure_axis(labelColor="#334155", titleColor="#0f172a", gridColor="#cbd5e1")
        .configure_legend(labelColor="#334155", titleColor="#0f172a", orient="bottom")
        .configure_title(color="#0f172a", fontSize=18)
    )


CHART_HEIGHT = 347
MAX_WORKLOAD_THREADS = 64
LOAD_MODE_LABELS = {
    "r-master": "Чтение с master",
    "rw-master": "Чтение/запись на master",
    "r-master-r-slave": "Чтение с master и standby",
    "rw-master-r-slave": "Запись на master, чтение с master и standby",
}


CHART_EXPLANATIONS = {
    "tps": """
**Источник данных**
- PostgreSQL view: `pg_stat_database`
- Поля: `xact_commit`, `xact_rollback`
- Фильтр: только выбранная рабочая БД (`Рабочая БД для TPS`)

**Как считаются точки**
1. При каждом срезе приложение читает накопительные счётчики commit/rollback.
2. Для соседних срезов считается разница счётчиков.
3. Разница делится на фактический интервал между срезами в секундах.

**Что показывает график**
- `COMMIT/s` = `(curr.xact_commit - prev.xact_commit) / dt`
- `ROLLBACK/s` = `(curr.xact_rollback - prev.xact_rollback) / dt`

Если счётчик сбросился или данных недостаточно, точка не рисуется.
""".strip(),
    "latency": """
**Источник данных**
- PostgreSQL view: `pg_stat_activity`
- Используются только активные client backend-сессии с ненулевым `query_start`

**Как считается значение**
1. Для каждого активного запроса берётся текущее время выполнения:
   `clock_timestamp() - query_start`
2. Значение переводится в миллисекунды.
3. На SQL-стороне считается 95-й процентиль через `percentile_cont(0.95)`.

**Что важно понимать**
- Это не историческая latency из трассировки запросов.
- Это моментальный p95 по запросам, которые активны в момент снятия среза.
- Если активных запросов нет, точка отсутствует.
""".strip(),
    "sessions": """
**Источник данных**
- PostgreSQL view: `pg_stat_activity`
- Фильтр: `backend_type = 'client backend'`

**Как считаются серии**
- `active` — сессии со state = `active`
- `waiting` — активные сессии, у которых `wait_event_type IS NOT NULL`
- `idle in xact` — state = `idle in transaction`
- `idle` — state = `idle`

**Что показывает график**
- Это stacked area-график моментальных значений по числу клиентских сессий.
- Каждая точка — снимок состояния в момент очередного опроса.
""".strip(),
    "cpu": """
**Источник данных**
- Метрика берётся по SSH с primary и standby-узлов.
- На хосте читается `/proc/stat` два раза с паузой 1 секунда.

**Как считается загрузка CPU**
1. Из первой и второй строки `cpu ...` берутся суммарные счётчики времени CPU.
2. Считается прирост total и idle.
3. Используется формула:
   `CPU% = (total_delta - idle_delta) / total_delta * 100`

**Что показывает график**
- `Primary CPU` — загрузка primary-узла.
- `Standby CPU` — загрузка standby-узла.

Если SSH недоступен или ответ не разобрался, точка пропускается.
""".strip(),
    "disk": """
**Источник данных**
- Метрика берётся по SSH только с primary-узла.
- Используется команда: `iostat -dx 1 2`

**Как считаются точки**
- Берётся второй срез `iostat`, чтобы исключить накопленные значения с момента старта ОС.
- Из таблицы устройств читаются поля:
  - `rkB/s` / `rKB/s` / `kB_read/s` → скорость чтения
  - `wkB/s` / `wKB/s` / `kB_wrtn/s` → скорость записи
- Если `iostat` возвращает `rMB/s` или `wMB/s`, приложение переводит их в КБ/с.

**Что показывает график**
- `Read` — чтение с диска primary в КБ/с
- `Write` — запись на диск primary в КБ/с

Если в конфиге задан `disk_device`, график строится именно по этому устройству.
""".strip(),
    "wal": """
**Источник данных**
- PostgreSQL view: `pg_stat_wal`
- Поле: `wal_bytes`

**Как считаются точки**
1. При каждом срезе читается накопительный объём WAL в байтах.
2. Для соседних срезов считается дельта `wal_bytes`.
3. Дельта делится на интервал между срезами.
4. Затем значение переводится из байт/с в МБ/с.

**Формула**
- `WAL MB/s = (curr.wal_bytes - prev.wal_bytes) / dt / 1024 / 1024`

Если счётчик обнулился или данных недостаточно, точка не рисуется.
""".strip(),
}


def line_chart(
    df: pd.DataFrame,
    color_field: str,
    y_title: str,
    y_scale: alt.Scale | None = None,
    color_scale: alt.Scale | None = None,
    x_title: str = "Время",
) -> None:
    if df.empty:
        st.info("Недостаточно данных")
        return
    clean_df = df.dropna(subset=["value"])
    if clean_df.empty:
        st.info("Недостаточно валидных данных")
        return
    chart = (
        alt.Chart(clean_df)
        .mark_line(strokeWidth=4, point=alt.OverlayMarkDef(size=70, filled=True))
        .encode(
            x=alt.X("timestamp:T", title=x_title),
            y=alt.Y("value:Q", title=y_title, scale=y_scale),
            color=alt.Color(f"{color_field}:N", legend=alt.Legend(title=None), scale=color_scale),
            tooltip=["timestamp:T", color_field, alt.Tooltip("value:Q", format=".2f")],
        )
        .properties(height=CHART_HEIGHT)
    )
    st.altair_chart(theme_chart(chart), width="stretch")


def cpu_y_scale(df: pd.DataFrame) -> alt.Scale:
    clean_df = df.dropna(subset=["value"])
    if clean_df.empty:
        return alt.Scale(domain=[0, 105])

    max_value = float(clean_df["value"].max())
    padded_upper_bound = max(105.0, min(120.0, round(max_value + 5.0, 2)))
    return alt.Scale(domain=[0, padded_upper_bound])


def render_chart_help(chart_key: str, chart_title: str) -> None:
    help_text = CHART_EXPLANATIONS[chart_key]
    with st.popover(chart_title, help="Нажмите, чтобы посмотреть описание графика", use_container_width=True):
        st.markdown(help_text)


def schedule_ui_refresh(interval_ms: int, key: str) -> None:
    if st_autorefresh is not None:
        st_autorefresh(interval=interval_ms, key=key)
        return

    time.sleep(interval_ms / 1000)
    st.rerun()


def get_workload_status_snapshot() -> dict[str, Any]:
    persisted_status = read_workload_status()
    workload_generator = st.session_state.get("workload_generator")
    is_running = bool(persisted_status.get("is_running"))
    if not is_running and workload_generator is not None:
        is_running = bool(getattr(workload_generator, "running", False))

    mode = str(
        persisted_status.get(
            "mode",
            st.session_state.get(
                "load_mode",
                st.session_state.get("persist_load_mode", "rw-master"),
            ),
        )
    )
    clients = int(
        persisted_status.get(
            "clients",
            st.session_state.get(
                "load_clients",
                st.session_state.get("persist_load_clients", 10),
            ),
        )
    )
    threads_per_client = int(
        persisted_status.get(
            "threads_per_client",
            st.session_state.get(
                "load_threads_per_client",
                st.session_state.get("persist_load_threads_per_client", 1),
            ),
        )
    )
    requested_threads = int(persisted_status.get("requested_threads", clients * threads_per_client))
    total_threads = min(MAX_WORKLOAD_THREADS, requested_threads)
    read_ratio = float(
        persisted_status.get(
            "read_ratio",
            st.session_state.get(
                "load_read_ratio",
                st.session_state.get("persist_load_read_ratio", 0.7),
            ),
        )
    )

    if mode in {"r-master", "r-master-r-slave"}:
        mode_details = "100% read"
    else:
        mode_details = f"read {read_ratio:.0%} / write {(1 - read_ratio):.0%}"

    return {
        "is_running": is_running,
        "status_label": "РАБОТАЕТ" if is_running else "ОСТАНОВЛЕНА",
        "status_class": "running" if is_running else "stopped",
        "mode_label": LOAD_MODE_LABELS.get(mode, mode),
        "mode_details": mode_details,
        "threads_label": str(total_threads),
        "threads_details": (
            f"из запрошенных {requested_threads}" if requested_threads > MAX_WORKLOAD_THREADS else "активный профиль"
        ),
    }


def render_workload_status_banner() -> None:
    snapshot = get_workload_status_snapshot()
    st.markdown(
        f"""
        <div class="workload-status-banner workload-status-banner--{snapshot["status_class"]}">
            <div class="workload-status-banner__item">
                <span class="workload-status-banner__label">Статус нагрузки</span>
                <span class="workload-status-banner__value">{snapshot["status_label"]}</span>
            </div>
            <div class="workload-status-banner__item">
                <span class="workload-status-banner__label">Потоков</span>
                <span class="workload-status-banner__value">{snapshot["threads_label"]}</span>
                <span class="workload-status-banner__hint">{snapshot["threads_details"]}</span>
            </div>
            <div class="workload-status-banner__item">
                <span class="workload-status-banner__label">Режим нагрузки</span>
                <span class="workload-status-banner__value">{snapshot["mode_label"]}</span>
                <span class="workload-status-banner__hint">{snapshot["mode_details"]}</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_dashboard() -> None:
    st.set_page_config(page_title="Экран производительности кластера", layout="wide")
    apply_base_page_styles(
        """
        div[data-testid="stPopover"] button[kind="secondary"] {
            min-height: auto;
            height: auto;
            width: 100%;
            padding: 0 0 0.35rem;
            border: none;
            border-radius: 0;
            background: transparent;
            box-shadow: none;
            justify-content: flex-start;
            font-size: 1.05rem;
            font-weight: 800 !important;
            color: #0f172a;
            text-align: left;
        }
        div[data-testid="stPopover"] button[kind="secondary"] * {
            font-weight: 800 !important;
        }
        div[data-testid="stPopover"] button[kind="secondary"]:hover {
            color: #2563eb;
        }
        .workload-status-banner {
            display: grid;
            grid-template-columns: repeat(3, minmax(0, 1fr));
            gap: 0.75rem;
            padding: 0.95rem 1.1rem;
            border-radius: 1rem;
            border: 2px solid transparent;
            background: #ffffff;
            box-shadow: 0 8px 24px rgba(15, 23, 42, 0.08);
            min-height: 100%;
        }
        .workload-status-banner--running {
            border-color: #22c55e;
            background: linear-gradient(180deg, #f0fdf4 0%, #ffffff 100%);
        }
        .workload-status-banner--stopped {
            border-color: #ef4444;
            background: linear-gradient(180deg, #fef2f2 0%, #ffffff 100%);
        }
        .workload-status-banner__item {
            display: flex;
            flex-direction: column;
            gap: 0.2rem;
            min-width: 0;
        }
        .workload-status-banner__label {
            font-size: 0.82rem;
            color: #475569;
        }
        .workload-status-banner__value {
            font-size: 1.08rem;
            font-weight: 700;
            color: #0f172a;
            line-height: 1.25;
        }
        .workload-status-banner__hint {
            font-size: 0.78rem;
            color: #64748b;
        }
        @media (max-width: 1200px) {
            .workload-status-banner {
                grid-template-columns: 1fr;
            }
        }
        """,
    )
    st.session_state.setdefault("cluster_dashboard_cfg_path", "config/cluster.json")
    st.session_state.setdefault("cluster_dashboard_target_db", "")
    st.session_state.setdefault("cluster_dashboard_auto_refresh", True)
    st.session_state.setdefault("cluster_dashboard_interval_sec", 10)
    st.session_state.setdefault("cluster_dashboard_window_minutes", 30)
    st.session_state.setdefault("cluster_dashboard_compact_grid", False)

    raw_cfg_path = str(st.session_state["cluster_dashboard_cfg_path"]).strip()
    if not raw_cfg_path:
        st.error("Укажите путь к JSON-конфигу кластера.")
        st.stop()
    cfg_path = Path(raw_cfg_path)
    if not cfg_path.exists():
        st.error(f"Конфиг не найден: {cfg_path}")
        st.stop()
    if not cfg_path.is_file():
        st.error(f"Ожидался JSON-файл конфига, но указан каталог: {cfg_path}")
        st.stop()

    try:
        cluster = load_cluster_config(cfg_path)
    except Exception as exc:
        LOGGER.exception("Не удалось прочитать конфиг кластера: %s", cfg_path)
        st.error(f"Не удалось прочитать конфиг: {exc}")
        st.stop()
    if not cluster.nodes:
        st.error("В конфиге не найдено узлов")
        st.stop()

    primary, standby = resolve_cluster_roles(cluster)

    persisted_status = read_workload_status()
    default_target_db = str(persisted_status.get("target_db") or extract_dbname_from_dsn(primary.dsn)).strip()
    target_db = str(st.session_state["cluster_dashboard_target_db"]).strip()
    if not target_db:
        target_db = default_target_db
        st.session_state["cluster_dashboard_target_db"] = default_target_db
    auto_refresh = bool(st.session_state["cluster_dashboard_auto_refresh"])
    interval_sec = int(st.session_state["cluster_dashboard_interval_sec"])
    window_minutes = int(st.session_state["cluster_dashboard_window_minutes"])
    compact_grid = bool(st.session_state["cluster_dashboard_compact_grid"])

    history_limit = int((window_minutes * 60) / interval_sec) + 30

    session_key = f"{primary.name}|{standby.name if standby else 'none'}|{target_db}"
    collector = get_async_collector(session_key, interval_sec, history_limit)
    collector.update(interval_sec=interval_sec, history_limit=history_limit)

    if auto_refresh:
        collector.start(lambda: fetch_snapshot(primary, standby, target_db))
    def render_live_dashboard_section() -> None:
        title_col, status_col = st.columns([1.45, 1.55], vertical_alignment="center")
        with title_col:
            st.title("Экран производительности кластера")
        with status_col:
            render_workload_status_banner()

        series = build_timeseries(collector.history(), window_minutes)
        if not series:
            st.info("Соберите минимум два среза метрик для отображения графиков.")
            return

        def render_sessions_chart() -> None:
            render_chart_help("sessions", "Active sessions by state")
            sessions_df = series["sessions"].dropna(subset=["value"])
            if sessions_df.empty:
                st.info("Недостаточно данных")
            else:
                chart = (
                    alt.Chart(sessions_df)
                    .mark_area(opacity=0.85)
                    .encode(
                        x=alt.X("timestamp:T", title="Время"),
                        y=alt.Y("value:Q", title="число сессий", stack="zero"),
                        color=alt.Color("state:N", legend=alt.Legend(title=None)),
                        tooltip=["timestamp:T", "state:N", alt.Tooltip("value:Q", format=".0f")],
                    )
                    .properties(height=CHART_HEIGHT)
                )
                st.altair_chart(theme_chart(chart), width="stretch")

        def render_tps_chart() -> None:
            render_chart_help("tps", "TPS (транзакции/с)")
            line_chart(series["tps"], "metric", "транзакции/с", alt.Scale(zero=True))

        def render_latency_chart() -> None:
            render_chart_help("latency", "Latency p95 (мс)")
            line_chart(series["latency"], "metric", "мс", alt.Scale(zero=True))

        def render_cpu_chart() -> None:
            render_chart_help("cpu", "CPU primary / standby (%)")
            line_chart(series["cpu"], "node", "%", cpu_y_scale(series["cpu"]))

        def render_disk_chart() -> None:
            render_chart_help("disk", "Disk IO (Primary, KB/s)")
            line_chart(
                series["disk"],
                "metric",
                "КБ/с",
                alt.Scale(zero=True),
                alt.Scale(domain=["Read", "Write"], range=["#2563eb", "#dc2626"]),
                "время",
            )

        def render_wal_chart() -> None:
            render_chart_help("wal", "WAL generation rate (MB/s)")
            line_chart(series["wal"], "metric", "МБ/с", alt.Scale(zero=True))

        charts: list[Callable[[], None]] = [
            render_tps_chart,
            render_latency_chart,
            render_sessions_chart,
            render_cpu_chart,
            render_disk_chart,
            render_wal_chart,
        ]

        if compact_grid:
            for idx in range(0, len(charts), 2):
                row_cols = st.columns(2)
                for col, chart_renderer in zip(row_cols, charts[idx : idx + 2], strict=False):
                    with col:
                        chart_renderer()
        else:
            first_row = st.columns(4)
            for col, chart_renderer in zip(first_row, charts[:4], strict=True):
                with col:
                    chart_renderer()

            second_row = st.columns(2)
            for col, chart_renderer in zip(second_row, charts[4:], strict=True):
                with col:
                    chart_renderer()

    live_dashboard_fragment = st.fragment(render_live_dashboard_section, run_every=1 if auto_refresh else None)
    live_dashboard_fragment()

    st.divider()

    controls_col1, controls_col2, controls_col3 = st.columns(3)
    controls_col1.checkbox("Автообновление", key="cluster_dashboard_auto_refresh")
    controls_col2.select_slider(
        "Шаг агрегации (сек)",
        options=[5, 10, 15, 30],
        key="cluster_dashboard_interval_sec",
    )
    controls_col3.select_slider(
        "Интервал по X (мин)",
        options=[15, 30, 45, 60],
        key="cluster_dashboard_window_minutes",
    )
    st.checkbox("Вертикальная сетка (4 ряда × 2 графика)", key="cluster_dashboard_compact_grid")
    if st.button("Снять новый срез", type="primary", width="stretch"):
        collector.collect_once(lambda: fetch_snapshot(primary, standby, target_db))
        st.rerun()

    st.divider()
    st.caption("Параметры источника данных")
    source_col1, source_col2 = st.columns(2)
    source_col1.text_input("Путь к конфигу", key="cluster_dashboard_cfg_path")
    source_col2.text_input("Рабочая БД для TPS", key="cluster_dashboard_target_db")

    if auto_refresh:
        st.caption("Сбор метрик выполняется в фоновом потоке. Графики и статус обновляются асинхронно без полной перерисовки страницы.")
    else:
        collector.stop()


if __name__ == "__main__":
    render_dashboard()
