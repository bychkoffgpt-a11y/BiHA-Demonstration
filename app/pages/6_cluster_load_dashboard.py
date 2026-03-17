from __future__ import annotations

import json
import shlex
import subprocess
import time
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import altair as alt
import pandas as pd
import psycopg
import streamlit as st

from logging_utils import setup_file_logger

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


@dataclass
class ClusterConfig:
    nodes: list[NodeConfig]


def load_cluster_config(path: Path) -> ClusterConfig:
    cfg = json.loads(path.read_text(encoding="utf-8"))
    nodes = [NodeConfig(**item) for item in cfg.get("nodes", [])]
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
        "top_sql": [],
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

                cur.execute(
                    """
                    SELECT
                        queryid::text,
                        LEFT(REGEXP_REPLACE(query, '\\s+', ' ', 'g'), 60) AS query_short,
                        total_exec_time,
                        calls
                    FROM pg_stat_statements
                    WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
                    ORDER BY total_exec_time DESC
                    LIMIT 10
                    """
                )
                top_sql_rows = cur.fetchall()
                sql_entries: list[dict[str, Any]] = []
                lat_candidates: list[tuple[float, int]] = []
                for queryid, query_short, total_exec, calls in top_sql_rows:
                    calls_i = int(calls or 0)
                    total_exec_f = float(total_exec or 0)
                    sql_entries.append({
                        "queryid": str(queryid),
                        "query": query_short or "<empty>",
                        "total_exec_time": total_exec_f,
                        "calls": calls_i,
                    })
                    if calls_i > 0:
                        lat_candidates.append((total_exec_f / calls_i, calls_i))
                snapshot["top_sql"] = sql_entries
                if lat_candidates:
                    series = []
                    for latency, weight in lat_candidates:
                        series.extend([latency] * min(weight, 50))
                    if series:
                        snapshot["latency_p95_ms"] = float(pd.Series(series).quantile(0.95))
    except psycopg.errors.UndefinedTable:
        LOGGER.warning("pg_stat_statements недоступен на узле %s", primary.name)
    except Exception:
        LOGGER.exception("Не удалось собрать метрики PostgreSQL для узла=%s", primary.name)

    snapshot["cpu_primary"] = fetch_cpu_pct(primary)
    if standby:
        snapshot["cpu_standby"] = fetch_cpu_pct(standby)

    disk_metrics = fetch_disk_latency(primary)
    snapshot["disk_read_ms"] = disk_metrics["read_ms"]
    snapshot["disk_write_ms"] = disk_metrics["write_ms"]

    return snapshot


def fetch_cpu_pct(node: NodeConfig) -> float | None:
    cpu_cmd = (
        "bash -lc \""
        "read -r _ u1 n1 s1 i1 iw1 irq1 sirq1 st1 _ < /proc/stat; "
        "t1=$((u1+n1+s1+i1+iw1+irq1+sirq1+st1)); "
        "idle1=$((i1+iw1)); "
        "sleep 1; "
        "read -r _ u2 n2 s2 i2 iw2 irq2 sirq2 st2 _ < /proc/stat; "
        "t2=$((u2+n2+s2+i2+iw2+irq2+sirq2+st2)); "
        "idle2=$((i2+iw2)); "
        "dt=$((t2-t1)); didle=$((idle2-idle1)); "
        "awk -v dt=$dt -v di=$didle 'BEGIN {if (dt<=0) print 0; else printf \\\"%.2f\\\", (dt-di)*100/dt}'"
        "\""
    )
    output = run_ssh_metric(node, cpu_cmd)
    if output is None:
        return None
    try:
        return float(output)
    except ValueError:
        return None


def fetch_disk_latency(node: NodeConfig) -> dict[str, float | None]:
    metrics = {"read_ms": None, "write_ms": None}
    remote_cmd = (
        "bash -lc \"iostat -dx 1 2 | "
        "awk 'NF && $1 !~ /^(Device|Linux|avg-cpu:)/ {last=$0} END {print last}'\""
    )
    output = run_ssh_metric(node, remote_cmd)
    if output is None:
        return metrics
    parts = output.split()
    if len(parts) < 12:
        return metrics
    try:
        metrics["read_ms"] = float(parts[10])
        metrics["write_ms"] = float(parts[11])
    except (ValueError, IndexError):
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
    sql_rows: list[dict[str, Any]] = []
    cpu_rows: list[dict[str, Any]] = []
    disk_rows: list[dict[str, Any]] = []
    wal_rows: list[dict[str, Any]] = []
    lag_rows: list[dict[str, Any]] = []

    for prev, curr in zip(points, points[1:]):
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

        prev_sql = {item["queryid"]: item for item in prev.get("top_sql", [])}
        for item in curr.get("top_sql", []):
            prev_item = prev_sql.get(item["queryid"])
            if not prev_item:
                continue
            delta_time = item["total_exec_time"] - prev_item["total_exec_time"]
            if delta_time < 0:
                continue
            sql_rows.append(
                {
                    "timestamp": ts,
                    "query": item["query"],
                    "value": delta_time / dt_sec,
                }
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

        lag_rows.append({"timestamp": ts, "metric": "Replay lag", "value": curr["replay_lag_sec"]})

    result = {
        "tps": pd.DataFrame(tps_rows),
        "latency": pd.DataFrame(latency_rows),
        "sessions": pd.DataFrame(sessions_rows),
        "top_sql": pd.DataFrame(sql_rows),
        "cpu": pd.DataFrame(cpu_rows),
        "disk": pd.DataFrame(disk_rows),
        "wal": pd.DataFrame(wal_rows),
        "lag": pd.DataFrame(lag_rows),
    }

    if not result["top_sql"].empty:
        recent = result["top_sql"].sort_values("timestamp").groupby("query", as_index=False).tail(1)
        top_queries = set(recent.sort_values("value", ascending=False).head(5)["query"].tolist())
        result["top_sql"] = result["top_sql"][result["top_sql"]["query"].isin(top_queries)]

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

    def start(self, collect_fn: Callable[[], dict[str, Any]]) -> None:
        if self._thread and self._thread.is_alive():
            return

        self._stop_event.clear()

        def worker() -> None:
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


def theme_chart(chart: alt.Chart) -> alt.Chart:
    return chart.configure_view(strokeOpacity=0).configure_axis(labelColor="#d8dbe2", titleColor="#f5f7fa", gridColor="#2c2f36").configure_legend(labelColor="#d8dbe2", titleColor="#f5f7fa", orient="bottom").configure_title(color="#f5f7fa", fontSize=18)


def line_chart(df: pd.DataFrame, color_field: str, y_title: str, title: str, y_scale: alt.Scale | None = None) -> None:
    if df.empty:
        st.info("Недостаточно данных")
        return
    chart = (
        alt.Chart(df.dropna(subset=["value"]))
        .mark_line(strokeWidth=4)
        .encode(
            x=alt.X("timestamp:T", title="Время"),
            y=alt.Y("value:Q", title=y_title, scale=y_scale),
            color=alt.Color(f"{color_field}:N", legend=alt.Legend(title=None)),
            tooltip=["timestamp:T", color_field, alt.Tooltip("value:Q", format=".2f")],
        )
        .properties(title=title, height=260)
    )
    st.altair_chart(theme_chart(chart), width="stretch")


st.set_page_config(page_title="Экран производительности кластера", layout="wide")
st.title("Экран производительности кластера")
st.caption("Верхний ряд — результат нагрузки, нижний ряд — цена и устойчивость кластера.")

cfg_path = Path(st.text_input("Путь к конфигу", "config/cluster.json"))
if not cfg_path.exists():
    st.error(f"Конфиг не найден: {cfg_path}")
    st.stop()

cluster = load_cluster_config(cfg_path)
if not cluster.nodes:
    st.error("В конфиге не найдено узлов")
    st.stop()

primary = next((n for n in cluster.nodes if n.role_hint.lower() in {"master", "primary", "leader"}), cluster.nodes[0])
standby = next((n for n in cluster.nodes if n.role_hint.lower() in {"slave", "replica", "standby"}), None)

target_db = st.text_input("Рабочая БД для TPS", "postgres")
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
    layout = [st.columns(2) for _ in range(4)] if compact_grid else [st.columns(4) for _ in range(2)]
    slots = [cell for row in layout for cell in row]

    with slots[0]:
        line_chart(series["tps"], "metric", "транзакции/с", "TPS (транзакции/с)", alt.Scale(zero=True))
    with slots[1]:
        line_chart(series["latency"], "metric", "мс", "Latency p95 (мс)", alt.Scale(zero=True))
    with slots[2]:
        if series["sessions"].empty:
            st.info("Недостаточно данных")
        else:
            chart = (
                alt.Chart(series["sessions"])
                .mark_area(opacity=0.85)
                .encode(
                    x=alt.X("timestamp:T", title="Время"),
                    y=alt.Y("value:Q", title="число сессий", stack="zero"),
                    color=alt.Color("state:N", legend=alt.Legend(title=None)),
                    tooltip=["timestamp:T", "state:N", alt.Tooltip("value:Q", format=".0f")],
                )
                .properties(title="Active sessions by state", height=260)
            )
            st.altair_chart(theme_chart(chart), width="stretch")
    with slots[3]:
        line_chart(series["top_sql"], "query", "мс/с", "Top SQL by total time (мс/с)", alt.Scale(zero=True))
    with slots[4]:
        line_chart(series["cpu"], "node", "%", "CPU primary / standby (%)", alt.Scale(domain=[0, 100]))
    with slots[5]:
        line_chart(series["disk"], "metric", "мс", "Disk latency (Primary, мс)", alt.Scale(zero=True))
    with slots[6]:
        line_chart(series["wal"], "metric", "МБ/с", "WAL generation rate (MB/s)", alt.Scale(zero=True))
    with slots[7]:
        line_chart(series["lag"], "metric", "сек", "Replication lag (с)", alt.Scale(zero=True))

if auto_refresh:
    st.caption("Сбор метрик выполняется в фоновом потоке. Интерфейс обновляется отдельно.")
    time.sleep(0.7)
    st.rerun()
else:
    collector.stop()
