from __future__ import annotations

import json
import shlex
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
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
    service_name: str = "postgrespro"
    collect_disk_metrics_via_ssh: bool = True
    disk_device: str | None = None


@dataclass
class ClusterConfig:
    nodes: list[NodeConfig]


def load_cluster_config(path: Path) -> ClusterConfig:
    if not path.exists():
        raise FileNotFoundError(f"Config not found: {path}")
    if not path.is_file():
        raise ValueError(f"Expected a JSON config file, got a directory: {path}")
    cfg = json.loads(path.read_text(encoding="utf-8"))
    nodes = [NodeConfig(**item) for item in cfg.get("nodes", [])]
    return ClusterConfig(nodes=nodes)


def run_ssh_check(node: NodeConfig, command: str, timeout_sec: int = 5) -> tuple[bool, str]:
    if not node.control_via_ssh or not node.ssh_host:
        LOGGER.warning("SSH check skipped for node=%s reason=control_disabled", node.name)
        return False, "SSH control disabled for this node in config"

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

    cmd.extend([f"{user_prefix}{node.ssh_host}", command])
    log_command = " ".join(shlex.quote(part) for part in cmd)
    started_at = time.perf_counter()
    LOGGER.info(
        "SSH check started node=%s host=%s timeout_sec=%s command=%s",
        node.name,
        node.ssh_host,
        timeout_sec,
        log_command,
    )
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_sec)
    except subprocess.TimeoutExpired:
        elapsed_ms = round((time.perf_counter() - started_at) * 1000)
        LOGGER.error(
            "SSH check timeout node=%s host=%s timeout_sec=%s elapsed_ms=%s command=%s",
            node.name,
            node.ssh_host,
            timeout_sec,
            elapsed_ms,
            log_command,
        )
        return False, f"Command timed out after {timeout_sec} seconds"

    output = (proc.stdout + "\n" + proc.stderr).strip()
    elapsed_ms = round((time.perf_counter() - started_at) * 1000)
    LOGGER.info(
        "SSH check finished node=%s host=%s returncode=%s elapsed_ms=%s stdout=%r stderr=%r",
        node.name,
        node.ssh_host,
        proc.returncode,
        elapsed_ms,
        proc.stdout.strip(),
        proc.stderr.strip(),
    )
    return proc.returncode == 0, output or "OK"


def available_checks(node: NodeConfig) -> dict[str, str]:
    service = shlex.quote(node.service_name)
    return {
        "SSH handshake (whoami)": "whoami",
        "Passwordless sudo": "sudo -n true",
        "Service control rights (systemctl status)": f"sudo -n systemctl status {service} --no-pager --lines=0",
        "Firewall management tool availability": "sudo -n bash -lc 'command -v firewall-cmd || command -v ufw || command -v nft || command -v iptables'",
        "sshd configuration check": "sudo -n sshd -T | egrep \"^(passwordauthentication|pubkeyauthentication|permitrootlogin|kexalgorithms|hostkeyalgorithms)\"",
    }


def run_selected_checks(node: NodeConfig, checks: list[str]) -> list[dict[str, Any]]:
    check_map = available_checks(node)
    results: list[dict[str, Any]] = []
    for check_name in checks:
        command = check_map.get(check_name)
        if not command:
            results.append(
                {
                    "Узел (Node)": node.name,
                    "Проверка (Check)": check_name,
                    "Команда (Command)": "N/A",
                    "Статус (Status)": "FAIL",
                    "Вывод (Output)": "Unknown check name",
                }
            )
            continue
        ok, output = run_ssh_check(node, command)
        results.append(
            {
                "Узел (Node)": node.name,
                "Проверка (Check)": check_name,
                "Команда (Command)": command,
                "Статус (Status)": "OK" if ok else "FAIL",
                "Вывод (Output)": output,
            }
        )
    return results


def run_checks_for_nodes(cluster: ClusterConfig, selected_nodes: list[str], selected_checks: list[str]) -> list[dict[str, Any]]:
    nodes_to_check = [node for node in cluster.nodes if node.name in selected_nodes]
    if not nodes_to_check:
        return []

    worker_count = max(1, min(8, len(nodes_to_check)))
    results_by_node: dict[str, list[dict[str, Any]]] = {}
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_to_node = {executor.submit(run_selected_checks, node, selected_checks): node.name for node in nodes_to_check}
        for future in as_completed(future_to_node):
            node_name = future_to_node[future]
            try:
                results_by_node[node_name] = future.result()
            except Exception as exc:
                results_by_node[node_name] = [
                    {
                        "Узел (Node)": node_name,
                        "Проверка (Check)": "execution",
                        "Команда (Command)": "N/A",
                        "Статус (Status)": "FAIL",
                        "Вывод (Output)": str(exc),
                    }
                ]

    ordered_results: list[dict[str, Any]] = []
    for node_name in selected_nodes:
        ordered_results.extend(results_by_node.get(node_name, []))
    return ordered_results


st.set_page_config(page_title="SSH доступ к узлам", layout="wide")
st.title("Проверка SSH-доступа к узлам кластера")
st.caption(
    "Страница для проверки, что приложение может подключаться по SSH и выполнять привилегированные команды "
    "для управления сервисами и файрволом на каждом узле."
)

raw_cfg_path = st.text_input("Путь к конфигу (Path to config)", "config/cluster.json").strip()
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
    st.error(f"Не удалось прочитать конфиг (Cannot parse config): {exc}")
    st.stop()

if not cluster.nodes:
    st.warning("В конфиге нет узлов (No nodes configured).")
    st.stop()

if "ssh_check_results" not in st.session_state:
    st.session_state.ssh_check_results = []

st.subheader("Параметры проверки")
selected_nodes = st.multiselect(
    "Узлы для проверки (Nodes to check)",
    options=[node.name for node in cluster.nodes],
    default=[node.name for node in cluster.nodes],
)

check_labels = list(available_checks(cluster.nodes[0]).keys())
selected_checks = st.multiselect(
    "Проверки (Checks)",
    options=check_labels,
    default=check_labels,
)

col1, col2 = st.columns(2)
if col1.button("Запустить проверки (Run checks)", type="primary", width="stretch"):
    if not selected_nodes:
        st.warning("Выберите хотя бы один узел.")
    elif not selected_checks:
        st.warning("Выберите хотя бы одну проверку.")
    else:
        with st.spinner("Выполняются SSH-проверки... (Running SSH checks...)"):
            st.session_state.ssh_check_results = run_checks_for_nodes(cluster, selected_nodes, selected_checks)

if col2.button("Очистить результаты (Clear results)", width="stretch"):
    st.session_state.ssh_check_results = []

results = st.session_state.ssh_check_results
if results:
    df = pd.DataFrame(results)
    df.insert(0, "Индикатор", df["Статус (Status)"].map({"OK": "🟢", "FAIL": "🔴"}).fillna("⚪"))
    st.dataframe(df, width="stretch")

    fail_count = int((df["Статус (Status)"] == "FAIL").sum())
    if fail_count:
        st.error(f"Проверки с ошибками: {fail_count}")
    else:
        st.success("Все выбранные проверки прошли успешно.")
else:
    st.info("Результатов пока нет. Запустите проверки.")
