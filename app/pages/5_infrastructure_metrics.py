from __future__ import annotations

import json
import shlex
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import altair as alt
import pandas as pd
import psycopg
import streamlit as st

from logging_utils import setup_file_logger

LOGGER = setup_file_logger()
MAX_HISTORY = 120


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
        LOGGER.warning("SSH метрики недоступны: узел=%s причина=ssh_disabled", node.name)
        return None

    cmd = build_ssh_command(node, remote_cmd)
    log_cmd = " ".join(shlex.quote(part) for part in cmd)
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_sec)
    except subprocess.TimeoutExpired:
        LOGGER.error("SSH метрики timeout: узел=%s команда=%s", node.name, log_cmd)
        return None
    except Exception as exc:
        LOGGER.exception("Ошибка запуска SSH команды для узла=%s: %s", node.name, exc)
        return None

    if proc.returncode != 0:
        LOGGER.error(
            "SSH метрики завершились с ошибкой: узел=%s returncode=%s stdout=%r stderr=%r команда=%s",
            node.name,
            proc.returncode,
            proc.stdout.strip(),
            proc.stderr.strip(),
            log_cmd,
        )
        return None

    output = proc.stdout.strip()
    if not output:
        LOGGER.warning("SSH метрики вернули пустой вывод: узел=%s команда=%s", node.name, log_cmd)
        return None
    return output


def parse_float(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def get_db_metrics(node: NodeConfig) -> dict[str, float | int | None]:
    metrics: dict[str, float | int | None] = {
        "active_transactions": None,
        "active_sessions": None,
        "active_locks": None,
        "avg_query_duration_ms": None,
    }
    try:
        with psycopg.connect(node.dsn, connect_timeout=3, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        (SELECT count(*) FROM pg_stat_activity WHERE xact_start IS NOT NULL AND state <> 'idle'),
                        (SELECT count(*) FROM pg_stat_activity WHERE state = 'active'),
                        (SELECT count(*) FROM pg_locks WHERE granted),
                        (
                            SELECT COALESCE(AVG(EXTRACT(EPOCH FROM (clock_timestamp() - query_start)) * 1000), 0)
                            FROM pg_stat_activity
                            WHERE state = 'active' AND query_start IS NOT NULL
                        )
                    """
                )
                row = cur.fetchone()
                if row:
                    metrics["active_transactions"] = int(row[0])
                    metrics["active_sessions"] = int(row[1])
                    metrics["active_locks"] = int(row[2])
                    metrics["avg_query_duration_ms"] = float(row[3])
    except Exception:
        LOGGER.exception("Не удалось получить метрики БД для узла=%s", node.name)
    return metrics


def get_disk_metrics(node: NodeConfig) -> dict[str, float | None]:
    metrics = {"disk_read_wait_ms": None, "disk_iops": None}
    remote_cmd = (
        "bash -lc \"iostat -dx 1 2 | "
        "awk 'NF && $1 !~ /^(Device|Linux|avg-cpu:)/ {last=$0} END {print last}'\""
    )
    output = run_ssh_metric(node, remote_cmd)
    if output is None:
        return metrics

    parts = output.split()
    if len(parts) < 11:
        LOGGER.error("Неожиданный формат iostat на узле=%s: %r", node.name, output)
        return metrics

    try:
        read_iops = float(parts[3])
        write_iops = float(parts[4])
        read_wait_ms = float(parts[10])
        metrics["disk_iops"] = read_iops + write_iops
        metrics["disk_read_wait_ms"] = read_wait_ms
    except ValueError:
        LOGGER.error("Не удалось распарсить iostat на узле=%s: %r", node.name, output)
    return metrics


def get_cpu_mem_metrics(node: NodeConfig) -> dict[str, float | None]:
    metrics = {"cpu_usage_pct": None, "memory_usage_mb": None}

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
        "awk -v dt=$dt -v di=$didle 'BEGIN {if (dt<=0) print 0; else printf \"%.2f\", (dt-di)*100/dt}'"
        "\""
    )
    mem_cmd = "bash -lc \"free -m | awk '/^Mem:/ {print $3}'\""

    cpu_output = run_ssh_metric(node, cpu_cmd)
    mem_output = run_ssh_metric(node, mem_cmd)

    metrics["cpu_usage_pct"] = parse_float(cpu_output)
    metrics["memory_usage_mb"] = parse_float(mem_output)

    if cpu_output is not None and metrics["cpu_usage_pct"] is None:
        LOGGER.error("Не удалось распарсить загрузку CPU на узле=%s: %r", node.name, cpu_output)
    if mem_output is not None and metrics["memory_usage_mb"] is None:
        LOGGER.error("Не удалось распарсить использование RAM на узле=%s: %r", node.name, mem_output)

    return metrics


def append_history(history_key: str, row: dict[str, Any]) -> None:
    history = st.session_state.get(history_key, [])
    history.append(row)
    if len(history) > MAX_HISTORY:
        history = history[-MAX_HISTORY:]
    st.session_state[history_key] = history


def render_chart(history_key: str, title: str, y_title: str) -> None:
    history = st.session_state.get(history_key, [])
    if not history:
        st.info("Недостаточно данных для графика. Нажмите «Обновить метрики».")
        return

    df = pd.DataFrame(history)
    id_columns = ["timestamp"]
    value_columns = [col for col in df.columns if col not in id_columns]
    long_df = df.melt(id_vars=id_columns, value_vars=value_columns, var_name="Показатель", value_name="Значение")

    chart = (
        alt.Chart(long_df)
        .mark_line(point=True)
        .encode(
            x=alt.X("timestamp:T", title="Время"),
            y=alt.Y("Значение:Q", title=y_title),
            color=alt.Color("Показатель:N", title="Показатель"),
            tooltip=["timestamp:T", "Показатель:N", alt.Tooltip("Значение:Q", format=".2f")],
        )
        .properties(title=title, height=290)
    )
    st.altair_chart(chart, width="stretch")


st.set_page_config(page_title="Инфраструктурные графики", layout="wide")
st.title("Инфраструктурные графики")
st.caption("Три графика: метрики из PostgreSQL и метрики хостов master/slave по SSH.")

cfg_path = Path(st.text_input("Путь к конфигу", "config/cluster.json"))
if not cfg_path.exists():
    st.error(f"Конфиг не найден: {cfg_path}")
    st.stop()

try:
    cluster = load_cluster_config(cfg_path)
except Exception as exc:
    st.error(f"Не удалось прочитать конфиг: {exc}")
    st.stop()

if not cluster.nodes:
    st.warning("В конфиге нет узлов.")
    st.stop()

master_node = next((node for node in cluster.nodes if node.role_hint.lower() in {"master", "primary", "leader"}), cluster.nodes[0])
slave_node = next((node for node in cluster.nodes if node.role_hint.lower() in {"slave", "replica", "standby"}), None)

selected_db_node_name = st.selectbox("Узел БД для графика 1", options=[node.name for node in cluster.nodes], index=0)
db_node = next(node for node in cluster.nodes if node.name == selected_db_node_name)

col1, col2 = st.columns([1, 1])
auto_refresh = col1.checkbox("Автообновление", value=True)
interval_sec = col2.slider("Интервал обновления (сек)", min_value=2, max_value=30, value=5)

if st.button("Обновить метрики", type="primary", width="stretch") or auto_refresh:
    now = pd.Timestamp.now(tz="Europe/Moscow")

    db_metrics = get_db_metrics(db_node)
    append_history(
        "infra_db_history",
        {
            "timestamp": now,
            "Активные транзакции": db_metrics["active_transactions"],
            "Активные сессии": db_metrics["active_sessions"],
            "Активные блокировки": db_metrics["active_locks"],
            "Средняя длительность запросов, мс": db_metrics["avg_query_duration_ms"],
        },
    )

    master_disk = get_disk_metrics(master_node)
    slave_disk = get_disk_metrics(slave_node) if slave_node else {"disk_read_wait_ms": None, "disk_iops": None}
    append_history(
        "infra_disk_history",
        {
            "timestamp": now,
            "Ожидание чтения диска master, мс": master_disk["disk_read_wait_ms"],
            "Ожидание чтения диска slave, мс": slave_disk["disk_read_wait_ms"],
            "IOPS master": master_disk["disk_iops"],
            "IOPS slave": slave_disk["disk_iops"],
        },
    )

    master_cpu_mem = get_cpu_mem_metrics(master_node)
    slave_cpu_mem = get_cpu_mem_metrics(slave_node) if slave_node else {"cpu_usage_pct": None, "memory_usage_mb": None}
    append_history(
        "infra_cpu_mem_history",
        {
            "timestamp": now,
            "Загрузка CPU master, %": master_cpu_mem["cpu_usage_pct"],
            "Загрузка CPU slave, %": slave_cpu_mem["cpu_usage_pct"],
            "Потребление RAM master, МБ": master_cpu_mem["memory_usage_mb"],
            "Потребление RAM slave, МБ": slave_cpu_mem["memory_usage_mb"],
        },
    )

render_chart("infra_db_history", "График 1. Метрики PostgreSQL", "Значение")
render_chart("infra_disk_history", "График 2. Диск (SSH)", "Значение")
render_chart("infra_cpu_mem_history", "График 3. CPU и RAM (SSH)", "Значение")

if auto_refresh:
    time.sleep(interval_sec)
    st.rerun()
