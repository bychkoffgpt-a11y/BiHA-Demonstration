from __future__ import annotations

import json
import shlex
import subprocess
import threading
import time
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Any, Callable

import altair as alt
import pandas as pd
import psycopg
import streamlit as st

try:
    from streamlit_autorefresh import st_autorefresh
except ImportError:
    st_autorefresh = None

from logging_utils import setup_file_logger
from ui_styles import apply_base_page_styles

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
        "latency_p95_ms": None,
        "active": 0,
        "idle": 0,
        "idle_xact": 0,
        "waiting": 0,
        "wal_bytes": None,
        "replay_lag_sec": None,
        "cpu_primary": None,
        "cpu_standby": None,
        "disk_read_ms": None,
        "disk_write_ms": None,
    }

    try:
        with psycopg.connect(primary.dsn, connect_timeout=3, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT xact_commit, xact_rollback
                    FROM pg_stat_database
                    WHERE datname = %s
                    """,
                    (target_db,),
                )
                row = cur.fetchone()
                if row:
                    snapshot["xact_commit"] = int(row[0])
                    snapshot["xact_rollback"] = int(row[1])

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

                cur.execute("SELECT wal_bytes FROM pg_stat_wal")
                row = cur.fetchone()
                if row:
                    snapshot["wal_bytes"] = int(row[0])

                cur.execute(
                    """
                    SELECT COALESCE(MAX(EXTRACT(EPOCH FROM replay_lag)), 0)
                    FROM pg_stat_replication
                    """
                )
                row = cur.fetchone()
                snapshot["replay_lag_sec"] = float(row[0] or 0)

                if snapshot["latency_p95_ms"] is None:
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
                        snapshot["latency_p95_ms"] = float(row[0])
    except Exception:
        LOGGER.exception("Не удалось собрать метрики PostgreSQL для узла=%s", primary.name)

    snapshot["cpu_primary"] = fetch_cpu_pct(primary)
    if standby:
        snapshot["cpu_standby"] = fetch_cpu_pct(standby)

    disk_metrics = fetch_disk_latency(primary)
    snapshot["disk_read_ms"] = disk_metrics["read_ms"]
    snapshot["disk_write_ms"] = disk_metrics["write_ms"]

    return snapshot


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


def fetch_disk_latency(node: NodeConfig) -> dict[str, float | None]:
    metrics = {"read_ms": None, "write_ms": None}
    output = run_ssh_metric(node, 'bash -lc "LC_ALL=C iostat -dx 1 2"')
    if output is not None:
        sections = [section.strip() for section in output.split("\n\n") if section.strip() and "Device" in section]
        if sections:
            lines = [line.strip() for line in sections[-1].splitlines() if line.strip()]
            header_idx = next((idx for idx, line in enumerate(lines) if line.startswith("Device")), None)
            if header_idx is not None and header_idx + 1 < len(lines):
                headers = lines[header_idx].split()
                rows = [line.split() for line in lines[header_idx + 1 :] if not line.startswith("avg-cpu")]
                if node.disk_device:
                    aliases = {node.disk_device.strip(), node.disk_device.strip().removeprefix('/dev/')}
                    rows = [row for row in rows if row and row[0] in aliases]
                if rows:
                    values_by_header = dict(zip(headers[1:], rows[0][1:]))
                    read_value = values_by_header.get("r_await")
                    write_value = values_by_header.get("w_await")
                    try:
                        metrics["read_ms"] = float(read_value) if read_value is not None else None
                    except ValueError:
                        metrics["read_ms"] = None
                    try:
                        metrics["write_ms"] = float(write_value) if write_value is not None else None
                    except ValueError:
                        metrics["write_ms"] = None
                    if metrics["read_ms"] is not None or metrics["write_ms"] is not None:
                        return metrics

    device_filter = "$3 !~ /^(loop|ram|fd)/"
    if node.disk_device:
        disk_name = node.disk_device.strip().removeprefix('/dev/')
        device_filter = f'$3 == "{disk_name}"'

    fallback_cmd = (
        "bash -lc \""
        f"awk '{device_filter} {{r+=$4; rt+=$7; w+=$8; wt+=$11}} END {{print r,rt,w,wt}}' /proc/diskstats; "
        "sleep 1; "
        f"awk '{device_filter} {{r+=$4; rt+=$7; w+=$8; wt+=$11}} END {{print r,rt,w,wt}}' /proc/diskstats\""
    )
    fallback_output = run_ssh_metric(node, fallback_cmd)
    if fallback_output is None:
        return metrics
    lines = [line.strip() for line in fallback_output.splitlines() if line.strip()]
    if len(lines) < 2:
        return metrics
    try:
        r1, rt1, w1, wt1 = [float(x) for x in lines[0].split()[:4]]
        r2, rt2, w2, wt2 = [float(x) for x in lines[1].split()[:4]]
        dr = max(r2 - r1, 0)
        dw = max(w2 - w1, 0)
        metrics["read_ms"] = max(rt2 - rt1, 0) / dr if dr > 0 else 0.0
        metrics["write_ms"] = max(wt2 - wt1, 0) / dw if dw > 0 else 0.0
    except (ValueError, IndexError, ZeroDivisionError):
        return metrics
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
                {"timestamp": ts, "metric": "Read", "value": curr["disk_read_ms"]},
                {"timestamp": ts, "metric": "Write", "value": curr["disk_write_ms"]},
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
- Основной способ: `iostat -dx 1 2`
- Резервный способ: расчёт по `/proc/diskstats`

**Как считается latency**
- При использовании `iostat` берутся поля:
  - `r_await` → latency чтения
  - `w_await` → latency записи
- При fallback-режиме приложение считает среднее время на операцию по дельте счётчиков чтения/записи между двумя замерами.

**Что показывает график**
- `Read` — средняя latency чтения диска в миллисекундах
- `Write` — средняя latency записи диска в миллисекундах

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


def line_chart(df: pd.DataFrame, color_field: str, y_title: str, title: str, y_scale: alt.Scale | None = None) -> None:
    if df.empty:
        st.info("Недостаточно данных")
        return
    clean_df = df.dropna(subset=["value"])
    if clean_df.empty:
        st.info("Недостаточно валидных данных")
        return
    chart = (
        alt.Chart(clean_df)
        .mark_line(strokeWidth=4)
        .encode(
            x=alt.X("timestamp:T", title="Время"),
            y=alt.Y("value:Q", title=y_title, scale=y_scale),
            color=alt.Color(f"{color_field}:N", legend=alt.Legend(title=None)),
            tooltip=["timestamp:T", color_field, alt.Tooltip("value:Q", format=".2f")],
        )
        .properties(title=title, height=CHART_HEIGHT)
    )
    st.altair_chart(theme_chart(chart), width="stretch")


def render_chart_help(chart_key: str) -> None:
    help_text = CHART_EXPLANATIONS[chart_key]
    help_cols = st.columns([0.12, 0.88])
    with help_cols[0]:
        with st.popover("i", help="Откуда и как считаются данные графика", use_container_width=True):
            st.markdown(help_text)


def schedule_ui_refresh(interval_ms: int, key: str) -> None:
    if st_autorefresh is not None:
        st_autorefresh(interval=interval_ms, key=key)
        return

    time.sleep(interval_ms / 1000)
    st.rerun()


def render_dashboard() -> None:
    st.set_page_config(page_title="Экран производительности кластера", layout="wide")
    apply_base_page_styles(
        """
        div[data-testid="stPopover"] button[kind="secondary"] {
            min-height: 1.75rem;
            height: 1.75rem;
            padding: 0 0.45rem;
            border-radius: 999px;
            font-size: 0.85rem;
            font-weight: 600;
        }
        """,
    )
    st.title("Экран производительности кластера")
    st.caption("Верхний ряд — результат нагрузки, нижний ряд — цена и устойчивость кластера.")

    st.session_state.setdefault("cluster_dashboard_cfg_path", "config/cluster.json")
    st.session_state.setdefault("cluster_dashboard_target_db", "postgres")

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

    primary = next((n for n in cluster.nodes if n.role_hint.lower() in {"master", "primary", "leader"}), cluster.nodes[0])
    standby = next((n for n in cluster.nodes if n.role_hint.lower() in {"slave", "replica", "standby"}), None)

    target_db = st.session_state["cluster_dashboard_target_db"]
    col1, col2, col3 = st.columns(3)
    auto_refresh = col1.checkbox("Автообновление", value=True)
    interval_sec = col2.select_slider("Шаг агрегации (сек)", options=[5, 10, 15, 30], value=10)
    window_minutes = col3.select_slider("Интервал по X (мин)", options=[15, 30, 45, 60], value=30)
    compact_grid = st.checkbox("Вертикальная сетка (4 ряда × 2 графика)", value=False)

    history_limit = int((window_minutes * 60) / interval_sec) + 30

    session_key = f"{primary.name}|{standby.name if standby else 'none'}|{target_db}"
    collector = get_async_collector(session_key, interval_sec, history_limit)
    collector.update(interval_sec=interval_sec, history_limit=history_limit)

    if auto_refresh:
        collector.start(lambda: fetch_snapshot(primary, standby, target_db))

    if st.button("Снять новый срез", type="primary", width="stretch"):
        collector.collect_once(lambda: fetch_snapshot(primary, standby, target_db))

    series = build_timeseries(collector.history(), window_minutes)
    if not series:
        st.info("Соберите минимум два среза метрик для отображения графиков.")
    else:
        def render_sessions_chart() -> None:
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
                    .properties(title="Active sessions by state", height=CHART_HEIGHT)
                )
                st.altair_chart(theme_chart(chart), width="stretch")
            render_chart_help("sessions")
    
        def render_tps_chart() -> None:
            line_chart(series["tps"], "metric", "транзакции/с", "TPS (транзакции/с)", alt.Scale(zero=True))
            render_chart_help("tps")
    
        def render_latency_chart() -> None:
            line_chart(series["latency"], "metric", "мс", "Latency p95 (мс)", alt.Scale(zero=True))
            render_chart_help("latency")
    
        def render_cpu_chart() -> None:
            line_chart(series["cpu"], "node", "%", "CPU primary / standby (%)", alt.Scale(domain=[0, 100]))
            render_chart_help("cpu")
    
        def render_disk_chart() -> None:
            line_chart(series["disk"], "metric", "мс", "Disk latency (Primary, мс)", alt.Scale(zero=True))
            render_chart_help("disk")
    
        def render_wal_chart() -> None:
            line_chart(series["wal"], "metric", "МБ/с", "WAL generation rate (MB/s)", alt.Scale(zero=True))
            render_chart_help("wal")
    
        charts: list[Callable[[], None]] = [
            render_tps_chart,
            render_latency_chart,
            render_sessions_chart,
            render_cpu_chart,
            render_disk_chart,
            render_wal_chart,
        ]

        charts_container = st.empty()
        with charts_container.container():
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

    st.divider()
    st.caption("Параметры источника данных")
    source_col1, source_col2 = st.columns(2)
    source_col1.text_input("Путь к конфигу", key="cluster_dashboard_cfg_path")
    source_col2.text_input("Рабочая БД для TPS", key="cluster_dashboard_target_db")

    if auto_refresh:
        st.caption("Сбор метрик выполняется в фоновом потоке. Интерфейс обновляется отдельно.")
        schedule_ui_refresh(interval_ms=1000, key=f"cluster-load-refresh-{session_key}")
    else:
        collector.stop()


if __name__ == "__main__":
    render_dashboard()
