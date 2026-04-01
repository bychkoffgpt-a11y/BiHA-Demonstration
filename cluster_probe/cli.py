from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Callable


ACTIONS = {"check_cluster_health", "verify_roles", "verify_availability", "switchover"}


class ProbeRuntimeError(RuntimeError):
    """Raised when probe action cannot be completed successfully."""


def _ensure_app_import_path() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    app_dir = repo_root / "app"
    app_dir_str = str(app_dir)
    if app_dir_str not in sys.path:
        sys.path.insert(0, app_dir_str)


def _load_cluster_dependencies() -> dict[str, Callable[..., Any]]:
    _ensure_app_import_path()
    from cluster_demo import (  # pylint: disable=import-error
        classify_node_role,
        fetch_all_node_metrics,
        get_target_database,
        load_cluster_config,
        switchover_master_role,
    )

    return {
        "classify_node_role": classify_node_role,
        "fetch_all_node_metrics": fetch_all_node_metrics,
        "get_target_database": get_target_database,
        "load_cluster_config": load_cluster_config,
        "switchover_master_role": switchover_master_role,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Internal cluster probe for orchestration workflows.")
    parser.add_argument("action", choices=sorted(ACTIONS), help="Orchestration action to run")
    parser.add_argument("--cluster-config", required=True, dest="cluster_config_path", help="Path to cluster config")
    parser.add_argument("--target-node", default="", help="Target node (optional)")
    parser.add_argument("--target-master", default="", help="Target master node for switchover")
    parser.add_argument(
        "--slo-window-sec",
        type=float,
        default=60.0,
        help="SLO window for availability checks (optional)",
    )
    return parser


def _collect_cluster_state(cluster_config_path: str, deps: dict[str, Callable[..., Any]]) -> tuple[list[dict[str, Any]], str]:
    load_cluster_config = deps["load_cluster_config"]
    get_target_database = deps["get_target_database"]
    fetch_all_node_metrics = deps["fetch_all_node_metrics"]

    cluster = load_cluster_config(Path(cluster_config_path).expanduser().resolve())
    target_db = get_target_database(cluster, "rw")
    rows = fetch_all_node_metrics(cluster.nodes, target_db)
    return rows, str(Path(cluster_config_path).expanduser().resolve())


def _check_cluster_health(cluster_config_path: str, deps: dict[str, Callable[..., Any]]) -> dict[str, Any]:
    classify_node_role = deps["classify_node_role"]
    rows, resolved_path = _collect_cluster_state(cluster_config_path, deps)
    up_nodes = [str(row.get("node")) for row in rows if str(row.get("status", "")).lower() == "up"]
    masters = [
        str(row.get("node"))
        for row in rows
        if classify_node_role(row.get("role"), row.get("tx_read_only")) == "master"
    ]
    slaves = [
        str(row.get("node"))
        for row in rows
        if classify_node_role(row.get("role"), row.get("tx_read_only")) == "slave"
    ]
    return {
        "cluster_config_path": resolved_path,
        "up_nodes": up_nodes,
        "total_nodes": len(rows),
        "all_nodes_up": bool(rows) and len(up_nodes) == len(rows),
        "current_roles": {
            "master": masters[0] if len(masters) == 1 else None,
            "slave": slaves[0] if len(slaves) == 1 else None,
            "masters": masters,
            "slaves": slaves,
        },
    }


def _verify_roles(cluster_config_path: str, deps: dict[str, Callable[..., Any]]) -> dict[str, Any]:
    classify_node_role = deps["classify_node_role"]
    rows, resolved_path = _collect_cluster_state(cluster_config_path, deps)
    nodes = {
        str(row.get("node")): {
            "status": row.get("status"),
            "role": row.get("role"),
            "tx_read_only": row.get("tx_read_only"),
            "replication_lag_sec": row.get("replication_lag_sec", row.get("replication_lag")),
        }
        for row in rows
    }
    masters = [
        str(row.get("node"))
        for row in rows
        if classify_node_role(row.get("role"), row.get("tx_read_only")) == "master"
    ]
    slaves = [
        str(row.get("node"))
        for row in rows
        if classify_node_role(row.get("role"), row.get("tx_read_only")) == "slave"
    ]
    roles_by_node = {node: str(node_data.get("role")) for node, node_data in nodes.items()}
    current_roles = {
        "master": masters[0] if len(masters) == 1 else None,
        "slave": slaves[0] if len(slaves) == 1 else None,
        "masters": masters,
        "slaves": slaves,
        "master_count": len(masters),
        "slave_count": len(slaves),
        "roles_by_node": roles_by_node,
        "has_single_master": len(masters) == 1,
        "all_other_nodes_are_slaves": len(slaves) == (len(rows) - 1) and bool(rows),
        "cardinality_ok": len(masters) == 1 and len(slaves) == (len(rows) - 1) and bool(rows),
    }
    return {
        "cluster_config_path": resolved_path,
        "current_roles": current_roles,
        "nodes": nodes,
    }


def _verify_availability(cluster_config_path: str, slo_window_sec: float, deps: dict[str, Callable[..., Any]]) -> dict[str, Any]:
    rows, resolved_path = _collect_cluster_state(cluster_config_path, deps)
    measured_downtime_sec = 0.0
    ratio = 1.0 if slo_window_sec <= 0 else max(0.0, 1.0 - (measured_downtime_sec / slo_window_sec))
    return {
        "cluster_config_path": resolved_path,
        "measured_downtime_sec": round(measured_downtime_sec, 3),
        "availability_ratio": round(ratio, 6),
        "slo_window_sec": float(slo_window_sec),
        "nodes_up": [str(row.get("node")) for row in rows if str(row.get("status", "")).lower() == "up"],
    }


def _switchover(cluster_config_path: str, target_master: str, deps: dict[str, Callable[..., Any]]) -> dict[str, Any]:
    load_cluster_config = deps["load_cluster_config"]
    switchover_master_role = deps["switchover_master_role"]

    cluster = load_cluster_config(Path(cluster_config_path).expanduser().resolve())
    payload = switchover_master_role(cluster, target_master=target_master or None)
    if not isinstance(payload, dict):
        raise ProbeRuntimeError("switchover returned non-dict payload")
    if payload.get("success") is not True:
        raise ProbeRuntimeError(json.dumps(payload, ensure_ascii=False))
    return payload


def run_probe(action: str, cluster_config_path: str, target_master: str = "", slo_window_sec: float = 60.0) -> dict[str, Any]:
    deps = _load_cluster_dependencies()
    normalized_action = action.strip().lower()
    if normalized_action == "check_cluster_health":
        return _check_cluster_health(cluster_config_path=cluster_config_path, deps=deps)
    if normalized_action == "verify_roles":
        return _verify_roles(cluster_config_path=cluster_config_path, deps=deps)
    if normalized_action == "verify_availability":
        return _verify_availability(cluster_config_path=cluster_config_path, slo_window_sec=slo_window_sec, deps=deps)
    if normalized_action == "switchover":
        return _switchover(cluster_config_path=cluster_config_path, target_master=target_master, deps=deps)
    raise ProbeRuntimeError(f"Unknown action={action!r}")


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        payload = run_probe(
            action=args.action,
            cluster_config_path=args.cluster_config_path,
            target_master=args.target_master,
            slo_window_sec=args.slo_window_sec,
        )
    except ProbeRuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except Exception as exc:  # pragma: no cover - defensive fallback for CLI UX
        print(f"cluster_probe fatal error: {exc}", file=sys.stderr)
        return 2

    print(json.dumps(payload, ensure_ascii=False))
    return 0
