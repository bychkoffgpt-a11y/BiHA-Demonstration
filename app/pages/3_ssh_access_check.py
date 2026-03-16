from __future__ import annotations

import json
import shlex
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st


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


def load_cluster_config(path: Path) -> ClusterConfig:
    cfg = json.loads(path.read_text(encoding="utf-8"))
    nodes = [NodeConfig(**item) for item in cfg.get("nodes", [])]
    return ClusterConfig(nodes=nodes)


def run_ssh_check(node: NodeConfig, command: str, timeout_sec: int = 10) -> tuple[bool, str]:
    if not node.control_via_ssh or not node.ssh_host:
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
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_sec)
    except subprocess.TimeoutExpired:
        return False, f"Command timed out after {timeout_sec} seconds"

    output = (proc.stdout + "\n" + proc.stderr).strip()
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
        command = check_map[check_name]
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


st.set_page_config(page_title="SSH доступ к узлам", layout="wide")
st.title("Проверка SSH-доступа к узлам кластера")
st.caption(
    "Страница для проверки, что приложение может подключаться по SSH и выполнять привилегированные команды "
    "для управления сервисами и файрволом на каждом узле."
)

cfg_path = Path(st.text_input("Путь к конфигу (Path to config)", "config/cluster.example.json"))
if not cfg_path.exists():
    st.error(f"Конфиг не найден (Config not found): {cfg_path}")
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
        all_results: list[dict[str, Any]] = []
        for node in cluster.nodes:
            if node.name not in selected_nodes:
                continue
            all_results.extend(run_selected_checks(node, selected_checks))
        st.session_state.ssh_check_results = all_results

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
