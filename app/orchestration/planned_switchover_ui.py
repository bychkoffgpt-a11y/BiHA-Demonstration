from __future__ import annotations

from dataclasses import dataclass
from typing import Any


PLANNED_SWITCHOVER_SCENARIO_ID = "planned_switchover"


@dataclass(frozen=True)
class PlannedSwitchoverParamsBuildResult:
    params_override: dict[str, str]
    available_slaves: list[str]
    cluster_config_path: str | None
    fetch_error: str | None
    validation_error: str | None
    warning_message: str | None


def extract_cluster_config_path(scenario: Any) -> str | None:
    for step in getattr(scenario, "steps", []):
        if str(getattr(step, "action_type", "")).lower().strip() != "switchover":
            continue
        params = getattr(step, "params", {})
        if not isinstance(params, dict):
            continue
        raw_path = params.get("cluster_config_path")
        if raw_path:
            return str(raw_path)
    return None


def fetch_available_slaves(cluster_config_path: str) -> tuple[list[str], str | None]:
    try:
        from cluster_demo import classify_node_role, fetch_all_node_metrics, get_target_database, load_cluster_config

        cluster = load_cluster_config(cluster_config_path)
        target_db = get_target_database(cluster, "rw")
        rows = fetch_all_node_metrics(cluster.nodes, target_db)
        available_slaves = sorted(
            str(row.get("node"))
            for row in rows
            if classify_node_role(row.get("role"), row.get("tx_read_only")) == "slave"
        )
        return available_slaves, None
    except Exception as exc:  # noqa: BLE001
        return [], str(exc)


def build_params_override_for_planned_switchover(
    scenario: Any,
    selected_target_master: str | None,
) -> PlannedSwitchoverParamsBuildResult:
    if str(getattr(scenario, "id", "")).strip() != PLANNED_SWITCHOVER_SCENARIO_ID:
        return PlannedSwitchoverParamsBuildResult({}, [], None, None, None, None)

    cluster_config_path = extract_cluster_config_path(scenario)
    if not cluster_config_path:
        return PlannedSwitchoverParamsBuildResult(
            params_override={},
            available_slaves=[],
            cluster_config_path=None,
            fetch_error=None,
            validation_error="В сценарии planned_switchover не задан params.cluster_config_path для шага switchover.",
            warning_message=None,
        )

    available_slaves, fetch_error = fetch_available_slaves(cluster_config_path)
    if fetch_error:
        return PlannedSwitchoverParamsBuildResult(
            params_override={},
            available_slaves=available_slaves,
            cluster_config_path=cluster_config_path,
            fetch_error=fetch_error,
            validation_error=None,
            warning_message=None,
        )

    if not available_slaves:
        return PlannedSwitchoverParamsBuildResult(
            params_override={},
            available_slaves=[],
            cluster_config_path=cluster_config_path,
            fetch_error=None,
            validation_error=None,
            warning_message="Список standby-узлов пуст. Запуск planned_switchover недоступен.",
        )

    if selected_target_master not in available_slaves:
        return PlannedSwitchoverParamsBuildResult(
            params_override={},
            available_slaves=available_slaves,
            cluster_config_path=cluster_config_path,
            fetch_error=None,
            validation_error="Для planned_switchover нужно выбрать target_master из списка standby-узлов.",
            warning_message=None,
        )

    return PlannedSwitchoverParamsBuildResult(
        params_override={"target_master": selected_target_master},
        available_slaves=available_slaves,
        cluster_config_path=cluster_config_path,
        fetch_error=None,
        validation_error=None,
        warning_message=None,
    )
