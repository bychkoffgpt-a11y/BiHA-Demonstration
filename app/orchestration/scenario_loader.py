from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


REQUIRED_SCENARIO_FIELDS = ("id", "name", "description", "steps", "success_criteria")


class ScenarioLoadError(ValueError):
    """Ошибка валидации или парсинга demo-сценария."""


@dataclass(frozen=True)
class StepDTO:
    action_type: str
    target_node: str
    params: dict[str, Any]
    wait_condition: dict[str, Any]
    timeout: float
    expected: Any


@dataclass(frozen=True)
class ScenarioDTO:
    id: str
    name: str
    description: str
    steps: list[StepDTO]
    success_criteria: str


def load_scenarios_from_directory(directory: str | Path) -> list[ScenarioDTO]:
    base_path = Path(directory)
    if not base_path.exists() or not base_path.is_dir():
        return []

    scenarios: list[ScenarioDTO] = []
    for path in sorted(base_path.glob("*.yaml")):
        scenarios.append(_load_scenario_file(path))
    return scenarios


def _load_scenario_file(path: Path) -> ScenarioDTO:
    try:
        raw_data = yaml.safe_load(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ScenarioLoadError(f"{path}: failed to parse YAML: {exc}") from exc

    if not isinstance(raw_data, dict):
        raise ScenarioLoadError(f"{path}: root YAML value must be mapping")

    _validate_required_fields(path, raw_data)

    steps_raw = raw_data["steps"]
    if not isinstance(steps_raw, list) or not steps_raw:
        raise ScenarioLoadError(f"{path}: 'steps' must be a non-empty list")

    success_criteria = raw_data["success_criteria"]
    if isinstance(success_criteria, list):
        success_criteria_text = "; ".join(str(item) for item in success_criteria)
    else:
        success_criteria_text = str(success_criteria)

    steps = [_parse_step(path, index, step_data) for index, step_data in enumerate(steps_raw)]

    return ScenarioDTO(
        id=str(raw_data["id"]),
        name=str(raw_data["name"]),
        description=str(raw_data["description"]),
        steps=steps,
        success_criteria=success_criteria_text,
    )


def _validate_required_fields(path: Path, raw_data: dict[str, Any]) -> None:
    missing_fields = [field for field in REQUIRED_SCENARIO_FIELDS if field not in raw_data]
    if missing_fields:
        raise ScenarioLoadError(f"{path}: missing required fields: {', '.join(missing_fields)}")


def _parse_step(path: Path, index: int, step_data: Any) -> StepDTO:
    if not isinstance(step_data, dict):
        raise ScenarioLoadError(f"{path}: step #{index + 1} must be mapping")

    if "action_type" in step_data:
        action_type = str(step_data.get("action_type", "")).strip()
        target_node = str(step_data.get("target_node", "cluster")).strip() or "cluster"
        params = step_data.get("params") or {}
        wait_condition = step_data.get("wait_condition") or {"equals": "ok"}
        timeout_raw = step_data.get("timeout", 10.0)
        expected = step_data.get("expected", "ok")
    elif "action" in step_data:
        action_type = str(step_data.get("action", "")).strip()
        target_node = str(step_data.get("target_node", "cluster")).strip() or "cluster"
        params = dict(step_data.get("params") or {})
        if "details" in step_data and "details" not in params:
            params["details"] = step_data["details"]
        if "name" in step_data and "step_name" not in params:
            params["step_name"] = step_data["name"]

        wait_condition = step_data.get("wait_condition") or {"equals": "ok"}
        timeout_raw = step_data.get("timeout", 10.0)
        expected = step_data.get("expected", "ok")
    else:
        raise ScenarioLoadError(
            f"{path}: step #{index + 1} must contain 'action_type' or legacy 'action'"
        )

    if not action_type:
        raise ScenarioLoadError(f"{path}: step #{index + 1} has empty action/action_type")

    if not isinstance(params, dict):
        raise ScenarioLoadError(f"{path}: step #{index + 1} field 'params' must be mapping")

    if not isinstance(wait_condition, dict):
        raise ScenarioLoadError(f"{path}: step #{index + 1} field 'wait_condition' must be mapping")

    try:
        timeout = float(timeout_raw)
    except (TypeError, ValueError) as exc:
        raise ScenarioLoadError(f"{path}: step #{index + 1} field 'timeout' must be a number") from exc

    return StepDTO(
        action_type=action_type,
        target_node=target_node,
        params=params,
        wait_condition=wait_condition,
        timeout=timeout,
        expected=expected,
    )
