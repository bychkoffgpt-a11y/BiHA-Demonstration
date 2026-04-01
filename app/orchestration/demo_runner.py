from __future__ import annotations

import copy
import json
import shlex
import subprocess
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from datetime import UTC, datetime
from enum import Enum
from typing import Any, Callable

from logging_utils import setup_file_logger

from .fault_injection import FaultInjectionController
from .scenario_loader import ScenarioLoadError, load_scenarios_from_directory

LOGGER = setup_file_logger()

ORCHESTRATION_ACTIONS = {
    "check_cluster_health",
    "switchover",
    "verify_roles",
    "verify_availability",
}

FAULT_INJECTION_ACTIONS = {
    "stop_db_service",
    "kill_db_process",
    "network_partition",
    "pause_node",
    "recover_action",
    "resume",
}

CURRENT_LEADER_TARGET_MARKER = "__CURRENT_LEADER__"
CURRENT_STANDBY_TARGET_MARKER = "__CURRENT_STANDBY__"
ROLLBACK_ID_FROM_STEP_PREFIX = "__ROLLBACK_ID:"


@dataclass(frozen=True)
class ScenarioCatalogStatus:
    loaded_from: str
    fallback_used: bool
    error: str | None = None


class RunStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass(frozen=True)
class Observation:
    timestamp: datetime
    source: str
    metric_event: str
    value: Any


@dataclass(frozen=True)
class Step:
    action_type: str
    target_node: str
    params: dict[str, Any] = field(default_factory=dict)
    wait_condition: dict[str, Any] = field(default_factory=dict)
    timeout: float = 10.0
    expected: Any = None


@dataclass(frozen=True)
class Scenario:
    id: str
    name: str
    description: str
    steps: list[Step]
    success_criteria: str


@dataclass
class StepRunLog:
    index: int
    action_type: str
    target_node: str
    timeout: float
    expected_result: Any
    started_at: datetime | None = None
    finished_at: datetime | None = None
    action_result: Any = None
    actual_result: Any = None
    status: str = "pending"
    error_reason: str | None = None


@dataclass
class ScenarioRun:
    run_id: str
    scenario_id: str
    scenario_name: str
    status: RunStatus
    created_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    current_step_index: int = -1
    error_reason: str | None = None
    step_logs: list[StepRunLog] = field(default_factory=list)


class CliOrchestrationBackend:
    """CLI backend для реального выполнения orchestration-команд."""

    def execute(self, step: Step, action_type: str) -> dict[str, Any]:
        started_monotonic = time.monotonic()
        command = self._build_command(step=step, action_type=action_type)
        completed = subprocess.run(command, capture_output=True, text=True, check=False)
        duration_sec = round(time.monotonic() - started_monotonic, 3)
        stdout = completed.stdout.strip()
        stderr = completed.stderr.strip()
        probe_output = self._parse_probe_output(stdout)

        if completed.returncode != 0:
            raise RuntimeError(
                "Orchestration CLI command failed: "
                f"action={action_type}, exit_code={completed.returncode}, stderr={stderr or '<empty>'}"
            )

        orchestration: dict[str, Any] = {
            "phase": "orchestration",
            "mode": "real",
            "success": True,
            "action_type": action_type,
            "cluster_config_path": str(step.params.get("cluster_config_path", "")),
            "duration_sec": duration_sec,
            "executed_command": shlex.join(command),
            "stdout": stdout,
            "stderr": stderr,
            "exit_code": completed.returncode,
            "probe_output": probe_output,
        }
        orchestration.update(self._derive_metrics(action_type=action_type, probe_output=probe_output))
        return {
            "action_type": action_type,
            "target_node": step.target_node,
            "params": step.params,
            "rollback_id": None,
            "orchestration": orchestration,
            "executed_at": datetime.now(UTC).isoformat(),
        }

    @staticmethod
    def _build_command(step: Step, action_type: str) -> list[str]:
        command = ["cluster_probe", action_type]
        cluster_config = str(step.params.get("cluster_config_path") or "").strip()
        if cluster_config:
            command.extend(["--cluster-config", cluster_config])
        target_node = str(step.target_node or "").strip()
        if target_node:
            command.extend(["--target-node", target_node])
        target_master = str(step.params.get("target_master") or "").strip()
        if target_master:
            command.extend(["--target-master", target_master])
        return command

    @staticmethod
    def _parse_probe_output(stdout: str) -> dict[str, Any] | None:
        if not stdout:
            return None
        try:
            parsed = json.loads(stdout)
        except json.JSONDecodeError:
            return None
        return parsed if isinstance(parsed, dict) else {"raw": parsed}

    @staticmethod
    def _derive_metrics(action_type: str, probe_output: dict[str, Any] | None) -> dict[str, Any]:
        if not isinstance(probe_output, dict):
            return {}
        if action_type != "switchover":
            return {}
        duration = probe_output.get("orchestration_duration_sec", probe_output.get("duration_sec"))
        downtime = probe_output.get("downtime_sec")
        roles = probe_output.get("roles") if isinstance(probe_output.get("roles"), dict) else {}
        derived: dict[str, Any] = {
            "switchover_duration_sec": float(duration) if isinstance(duration, (int, float)) else None,
            "downtime_sec": float(downtime) if isinstance(downtime, (int, float)) else None,
            "target_master": probe_output.get("target_master"),
            "old_leader": roles.get("slave"),
            "new_leader": roles.get("master"),
        }
        return {key: value for key, value in derived.items() if value is not None}


class DemoRunner:
    """In-memory сценарный раннер с API запуска/остановки и журналом шагов."""

    def __init__(
        self,
        scenarios: list[Scenario] | None = None,
        max_injection_duration_sec: float = 30.0,
        scenario_timeout_sec: float = 120.0,
    ) -> None:
        self._scenarios: dict[str, Scenario] = {scenario.id: scenario for scenario in scenarios or []}
        self._runs: dict[str, ScenarioRun] = {}
        self._run_threads: dict[str, threading.Thread] = {}
        self._cancel_events: dict[str, threading.Event] = {}
        self._run_metrics: dict[str, dict[str, Any]] = {}
        self._scenario_timeout_sec = scenario_timeout_sec
        self._fault_injection = FaultInjectionController(max_injection_duration_sec=max_injection_duration_sec)
        self._orchestration_backend = CliOrchestrationBackend()
        self._lock = threading.RLock()

    def register_scenario(self, scenario: Scenario) -> None:
        with self._lock:
            self._scenarios[scenario.id] = scenario

    def list_scenarios(self) -> list[Scenario]:
        with self._lock:
            return list(self._scenarios.values())

    def start_scenario(
        self,
        scenario_id: str,
        params_override: dict[str, Any] | None = None,
    ) -> str:
        with self._lock:
            scenario = self._scenarios.get(scenario_id)
            if scenario is None:
                raise ValueError(f"Unknown scenario_id: {scenario_id}")
            effective_scenario = self._apply_params_override(scenario, params_override or {})

            run_id = str(uuid.uuid4())
            run = ScenarioRun(
                run_id=run_id,
                scenario_id=effective_scenario.id,
                scenario_name=effective_scenario.name,
                status=RunStatus.PENDING,
                created_at=datetime.now(UTC),
                step_logs=[
                    StepRunLog(
                        index=index,
                        action_type=step.action_type,
                        target_node=step.target_node,
                        timeout=step.timeout,
                        expected_result=step.expected,
                    )
                    for index, step in enumerate(effective_scenario.steps)
                ],
            )
            self._runs[run_id] = run
            cancel_event = threading.Event()
            self._cancel_events[run_id] = cancel_event
            self._run_metrics[run_id] = {}
            thread = threading.Thread(
                target=self._run_scenario,
                args=(run_id, effective_scenario, cancel_event),
                name=f"scenario-run-{run_id[:8]}",
                daemon=True,
            )
            self._run_threads[run_id] = thread
            thread.start()
            LOGGER.info(
                "Scenario run started | run_id=%s scenario_id=%s scenario_name=%s steps=%s",
                run_id,
                effective_scenario.id,
                effective_scenario.name,
                len(effective_scenario.steps),
            )
            return run_id

    @staticmethod
    def _apply_params_override(scenario: Scenario, params_override: dict[str, Any]) -> Scenario:
        if not params_override:
            return scenario

        placeholder_map = {f"__REQUIRED_{key.upper()}__": value for key, value in params_override.items()}

        def _resolve(value: Any) -> Any:
            if isinstance(value, dict):
                return {k: _resolve(v) for k, v in value.items()}
            if isinstance(value, list):
                return [_resolve(item) for item in value]
            if isinstance(value, tuple):
                return tuple(_resolve(item) for item in value)
            if isinstance(value, str):
                if value in placeholder_map:
                    return placeholder_map[value]
                if value.startswith("{{") and value.endswith("}}"):
                    token = value[2:-2].strip()
                    if token.startswith("params."):
                        key = token.removeprefix("params.")
                        if key in params_override:
                            return params_override[key]
            return copy.deepcopy(value)

        overridden_steps = [
            Step(
                action_type=step.action_type,
                target_node=step.target_node,
                params=_resolve(step.params),
                wait_condition=_resolve(step.wait_condition),
                timeout=step.timeout,
                expected=_resolve(step.expected),
            )
            for step in scenario.steps
        ]
        return Scenario(
            id=scenario.id,
            name=scenario.name,
            description=scenario.description,
            steps=overridden_steps,
            success_criteria=scenario.success_criteria,
        )

    def stop_scenario(self, run_id: str) -> None:
        with self._lock:
            cancel_event = self._cancel_events.get(run_id)
            run = self._runs.get(run_id)
            if cancel_event is None or run is None:
                raise ValueError(f"Unknown run_id: {run_id}")
            cancel_event.set()
            if run.status in {RunStatus.PENDING, RunStatus.RUNNING}:
                run.status = RunStatus.CANCELLED
                run.finished_at = datetime.now(UTC)
                run.error_reason = "Cancelled by user"

    def get_run_status(self, run_id: str) -> ScenarioRun:
        with self._lock:
            run = self._runs.get(run_id)
            if run is None:
                raise ValueError(f"Unknown run_id: {run_id}")
            return run

    def _run_scenario(self, run_id: str, scenario: Scenario, cancel_event: threading.Event) -> None:
        scenario_started_monotonic = time.monotonic()
        with self._lock:
            run = self._runs[run_id]
            run.status = RunStatus.RUNNING
            run.started_at = datetime.now(UTC)
        LOGGER.info(
            "Scenario execution entered RUNNING state | run_id=%s scenario_id=%s",
            run_id,
            scenario.id,
        )

        try:
            for step_index, step in enumerate(scenario.steps):
                if time.monotonic() - scenario_started_monotonic > self._scenario_timeout_sec:
                    rollback_summary = self._fault_injection.rollback()
                    LOGGER.error(
                        "Scenario timeout reached | run_id=%s timeout_sec=%s rollback=%s",
                        run_id,
                        self._scenario_timeout_sec,
                        rollback_summary,
                    )
                    raise TimeoutError(
                        f"Scenario timeout after {self._scenario_timeout_sec} sec. "
                        f"Automatic rollback executed: {rollback_summary}"
                    )
                if cancel_event.is_set():
                    self._mark_cancelled(run_id, step_index, "Cancelled before step execution")
                    rollback_summary = self._fault_injection.rollback()
                    LOGGER.warning(
                        "Scenario cancelled before step execution | run_id=%s step=%s rollback=%s",
                        run_id,
                        step_index + 1,
                        rollback_summary,
                    )
                    return

                self._execute_step(run_id, step_index, step, cancel_event)

            with self._lock:
                run = self._runs[run_id]
                if run.status not in {RunStatus.CANCELLED, RunStatus.FAILED}:
                    run.status = RunStatus.SUCCEEDED
                    run.finished_at = datetime.now(UTC)
            rollback_summary = self._fault_injection.rollback()
            LOGGER.info(
                "Scenario finished successfully | run_id=%s rollback=%s",
                run_id,
                rollback_summary,
            )
        except Exception as exc:
            rollback_summary = self._fault_injection.rollback()
            with self._lock:
                run = self._runs[run_id]
                run.status = RunStatus.FAILED
                run.error_reason = f"{exc}; rollback={rollback_summary}"
                run.finished_at = datetime.now(UTC)
            LOGGER.exception(
                "Scenario failed | run_id=%s scenario_id=%s error=%s rollback=%s",
                run_id,
                scenario.id,
                exc,
                rollback_summary,
            )
        finally:
            with self._lock:
                self._run_threads.pop(run_id, None)
                self._cancel_events.pop(run_id, None)
                self._run_metrics.pop(run_id, None)
            LOGGER.info("Scenario execution finalized | run_id=%s", run_id)

    def _execute_step(self, run_id: str, step_index: int, step: Step, cancel_event: threading.Event) -> None:
        LOGGER.info(
            "Step execution started | run_id=%s step=%s action=%s target=%s timeout=%s wait_condition=%s expected=%r",
            run_id,
            step_index + 1,
            step.action_type,
            step.target_node,
            step.timeout,
            step.wait_condition,
            step.expected,
        )
        with self._lock:
            run = self._runs[run_id]
            run.current_step_index = step_index
            log = run.step_logs[step_index]
            log.status = "running"
            log.started_at = datetime.now(UTC)
        try:
            action_result = self.execute_action(run_id, step)
            self._record_step_runtime_context(run_id, step, action_result)
        except Exception as exc:
            last_observed = getattr(exc, "last_observed", None)
            timeout_payload = getattr(exc, "timeout_payload", None)
            diagnostic_hint = getattr(exc, "diagnostic_hint", None)
            if diagnostic_hint is None:
                diagnostic_hint = self._infer_roles_hint(last_observed)
            with self._lock:
                run = self._runs[run_id]
                log = run.step_logs[step_index]
                if timeout_payload is not None:
                    log.actual_result = timeout_payload
                elif last_observed is not None:
                    log.actual_result = {
                        "source": "wait_until.timeout",
                        "value": last_observed,
                    }
                log.status = "failed"
                log.error_reason = self._compose_error_reason(str(exc), diagnostic_hint)
                log.finished_at = datetime.now(UTC)
                run.status = RunStatus.FAILED
                run.error_reason = self._compose_error_reason(f"Step {step_index + 1} failed: {exc}", diagnostic_hint)
                run.finished_at = datetime.now(UTC)
            LOGGER.exception(
                "Step action failed | run_id=%s step=%s action=%s error=%s",
                run_id,
                step_index + 1,
                step.action_type,
                exc,
            )
            raise

        LOGGER.info(
            "Step action executed | run_id=%s step=%s rollback_id=%s details=%s",
            run_id,
            step_index + 1,
            action_result.get("rollback_id"),
            action_result.get("fault_injection") or action_result.get("orchestration"),
        )
        try:
            with self._lock:
                run = self._runs[run_id]
                log = run.step_logs[step_index]
                log.action_result = action_result
            self._record_run_metrics(run_id, step.action_type, action_result)

            observation = self.wait_until(
                run_id,
                step.wait_condition,
                step.timeout,
                cancel_event,
                observation_source=self._resolve_observation_source(run_id, step),
            )
            self._record_observation_runtime_context(run_id, step, observation)
            is_valid, actual_result, reason = self.validate(run_id, step.expected, observation)

            with self._lock:
                run = self._runs[run_id]
                log = run.step_logs[step_index]
                log.actual_result = actual_result
                log.finished_at = datetime.now(UTC)
                if is_valid:
                    log.status = "succeeded"
                    LOGGER.info(
                        "Step execution succeeded | run_id=%s step=%s actual=%s",
                        run_id,
                        step_index + 1,
                        actual_result,
                    )
                else:
                    raise RuntimeError(reason or "Step validation failed")
        except Exception as exc:
            last_observed = getattr(exc, "last_observed", None)
            timeout_payload = getattr(exc, "timeout_payload", None)
            diagnostic_hint = getattr(exc, "diagnostic_hint", None)
            if diagnostic_hint is None:
                diagnostic_hint = self._infer_roles_hint(last_observed)
            with self._lock:
                run = self._runs[run_id]
                log = run.step_logs[step_index]
                if timeout_payload is not None:
                    log.actual_result = timeout_payload
                elif last_observed is not None:
                    log.actual_result = {
                        "source": "wait_until.timeout",
                        "value": last_observed,
                    }
                log.status = "failed"
                log.error_reason = self._compose_error_reason(str(exc), diagnostic_hint)
                log.finished_at = datetime.now(UTC)
                run.status = RunStatus.FAILED
                run.error_reason = self._compose_error_reason(f"Step {step_index + 1} failed: {exc}", diagnostic_hint)
                run.finished_at = datetime.now(UTC)
            LOGGER.exception(
                "Step execution failed after action | run_id=%s step=%s error=%s",
                run_id,
                step_index + 1,
                exc,
            )
            raise

    def _record_observation_runtime_context(self, run_id: str, step: Step, observation: Observation) -> None:
        step_name = str(step.params.get("step_name") or "").strip()
        if not step_name:
            return
        with self._lock:
            metrics = self._run_metrics.setdefault(run_id, {})
            observations_by_step = metrics.setdefault("observations_by_step", {})
            observations_by_step[step_name] = observation.value
            capture_key = str(step.params.get("store_current_master_as") or "").strip()
            if capture_key and isinstance(observation.value, dict):
                current_roles = observation.value.get("current_roles")
                if isinstance(current_roles, dict) and current_roles.get("master"):
                    metrics[capture_key] = current_roles["master"]

    def _mark_cancelled(self, run_id: str, step_index: int, reason: str) -> None:
        with self._lock:
            run = self._runs[run_id]
            run.status = RunStatus.CANCELLED
            run.current_step_index = step_index
            run.error_reason = reason
            run.finished_at = datetime.now(UTC)

    def execute_action(self, run_id: str, step: Step) -> dict[str, Any]:
        """Выполнение действия шага с fault-injection guardrails и rollback metadata."""
        action_type = step.action_type.lower().strip()
        resolved_target_node = self._resolve_action_target_node(step)
        resolved_params = self._resolve_action_params(run_id, step, action_type)
        resolved_step = Step(
            action_type=step.action_type,
            target_node=resolved_target_node,
            params=resolved_params,
            wait_condition=step.wait_condition,
            timeout=step.timeout,
            expected=step.expected,
        )
        step_label = str(step.params.get("step_name") or step.action_type)
        LOGGER.info(
            "Starting scenario action execution | step=%s action_type=%s target=%s timeout_sec=%s params=%s",
            step_label,
            action_type,
            resolved_target_node,
            step.timeout,
            resolved_params,
        )
        if action_type in ORCHESTRATION_ACTIONS:
            result = self._execute_orchestration_action(resolved_step, action_type)
            LOGGER.info(
                "Completed scenario orchestration action | step=%s action_type=%s result=%s",
                step_label,
                action_type,
                result,
            )
            return result

        if action_type not in FAULT_INJECTION_ACTIONS:
            raise ValueError(f"Unknown action_type={action_type!r}")

        result = self._fault_injection.execute(
            action_type=action_type,
            target_node=resolved_target_node,
            params=resolved_params,
        )
        payload = {
            "action_type": result.action_type,
            "target_node": resolved_target_node,
            "params": resolved_params,
            "rollback_id": result.rollback_id,
            "fault_injection": result.details,
            "executed_at": datetime.now(UTC).isoformat(),
        }
        LOGGER.info(
            "Completed scenario fault-injection action | step=%s action_type=%s result=%s",
            step_label,
            action_type,
            payload,
        )
        return payload

    def _resolve_action_target_node(self, step: Step) -> str:
        if step.target_node not in {CURRENT_LEADER_TARGET_MARKER, CURRENT_STANDBY_TARGET_MARKER}:
            return step.target_node

        from cluster_demo import classify_node_role, fetch_all_node_metrics, get_target_database, load_cluster_config

        configured_standby = str(step.params.get("target_standby") or "").strip()
        if step.target_node == CURRENT_STANDBY_TARGET_MARKER and configured_standby:
            return configured_standby

        cluster_config_raw = step.params.get("cluster_config_path")
        if not cluster_config_raw:
            if step.target_node == CURRENT_STANDBY_TARGET_MARKER:
                raise RuntimeError(
                    "Runtime validation failed: standby target cannot be resolved. "
                    "Provide params.target_standby or params.cluster_config_path with at least one standby node."
                )
            raise ValueError(
                "cluster_config_path is required for orchestration steps. "
                "Set params.cluster_config_path in the scenario YAML."
            )

        cluster_config_path = Path(str(cluster_config_raw)).expanduser().resolve()
        cluster = load_cluster_config(cluster_config_path)
        target_db = get_target_database(cluster, "rw")
        metrics_rows = fetch_all_node_metrics(cluster.nodes, target_db)
        standby_nodes = sorted(
            str(row.get("node"))
            for row in metrics_rows
            if classify_node_role(row.get("role"), row.get("tx_read_only")) == "slave"
        )
        if step.target_node == CURRENT_STANDBY_TARGET_MARKER:
            if not standby_nodes:
                raise RuntimeError(
                    "Runtime validation failed: standby target cannot be resolved from cluster state. "
                    "No standby nodes detected. Provide params.target_standby or restore replication first."
                )
            return standby_nodes[0]

        masters = sorted(
            str(row.get("node"))
            for row in metrics_rows
            if classify_node_role(row.get("role"), row.get("tx_read_only")) == "master"
        )
        if len(masters) != 1:
            raise RuntimeError(
                "Runtime validation failed: expected exactly one current master to resolve "
                f"target_node={CURRENT_LEADER_TARGET_MARKER!r}, found {len(masters)} (masters={masters})."
            )
        return masters[0]

    def _resolve_action_params(self, run_id: str, step: Step, action_type: str) -> dict[str, Any]:
        resolved_params = copy.deepcopy(step.params)
        if action_type != "recover_action":
            return resolved_params

        rollback_id_value = resolved_params.get("rollback_id")
        if not isinstance(rollback_id_value, str):
            return resolved_params
        if not (
            rollback_id_value.startswith(ROLLBACK_ID_FROM_STEP_PREFIX)
            and rollback_id_value.endswith("__")
            and len(rollback_id_value) > len(ROLLBACK_ID_FROM_STEP_PREFIX) + 2
        ):
            return resolved_params

        source_step = rollback_id_value[len(ROLLBACK_ID_FROM_STEP_PREFIX) : -2]
        with self._lock:
            run_metrics = self._run_metrics.get(run_id, {})
            rollback_ids_by_step = run_metrics.get("rollback_ids_by_step", {})
            resolved_rollback_id = rollback_ids_by_step.get(source_step)
        if not resolved_rollback_id:
            raise RuntimeError(
                "Runtime validation failed: rollback_id marker "
                f"{rollback_id_value!r} cannot be resolved because step {source_step!r} "
                "has no recorded rollback_id in current run context."
            )
        resolved_params["rollback_id"] = resolved_rollback_id
        return resolved_params

    def _record_step_runtime_context(self, run_id: str, step: Step, action_result: dict[str, Any]) -> None:
        step_name = str(step.params.get("step_name") or "").strip()
        if not step_name:
            return
        with self._lock:
            metrics = self._run_metrics.setdefault(run_id, {})
            resolved_targets = metrics.setdefault("resolved_targets_by_step", {})
            resolved_targets[step_name] = action_result.get("target_node")
            rollback_id = action_result.get("rollback_id")
            if rollback_id:
                rollback_ids = metrics.setdefault("rollback_ids_by_step", {})
                rollback_ids[step_name] = rollback_id

    def _execute_orchestration_action(self, step: Step, action_type: str) -> dict[str, Any]:
        result = self._orchestration_backend.execute(step=step, action_type=action_type)
        orchestration = result.get("orchestration")
        if isinstance(orchestration, dict) and orchestration.get("success") is False:
            raise RuntimeError(
                "Orchestration action failed: "
                f"action={action_type}, details={orchestration.get('probe_output') or orchestration}"
            )
        return result

    @staticmethod
    def _resolve_cluster_config_path(params: dict[str, Any]) -> Path:
        raw_path = params.get("cluster_config_path")
        if not raw_path:
            raise ValueError(
                "cluster_config_path is required for orchestration steps. "
                "Set params.cluster_config_path in the scenario YAML."
            )
        return Path(str(raw_path)).expanduser().resolve()

    def wait_until(
        self,
        run_id: str,
        condition: dict[str, Any],
        timeout: float,
        cancel_event: threading.Event,
        observation_source: Callable[[], Observation] | None = None,
    ) -> Observation:
        """Ожидание условия до timeout с периодическим опросом."""
        source_fn = observation_source or self._observe_default
        deadline = time.monotonic() + timeout
        last_observed: Any = None
        recent_observations: deque[dict[str, Any]] = deque(maxlen=5)
        LOGGER.info(
            "Waiting for condition | timeout_sec=%s condition=%s",
            timeout,
            condition,
        )

        while time.monotonic() < deadline:
            if cancel_event.is_set():
                LOGGER.warning("Step wait cancelled by event")
                raise RuntimeError("Step cancelled")
            observation = source_fn()
            last_observed = observation.value
            recent_observations.append(
                {
                    "timestamp": observation.timestamp.isoformat(),
                    "source": observation.source,
                    "metric_event": observation.metric_event,
                    "value": observation.value,
                }
            )
            if self._matches_condition(observation.value, condition, run_id=run_id):
                LOGGER.info("Condition met | observed_value=%r source=%s", observation.value, observation.source)
                return observation
            time.sleep(0.5)

        LOGGER.error(
            "Step wait timeout | timeout_sec=%s condition=%s last_observed=%r",
            timeout,
            condition,
            last_observed,
        )
        timeout_error = TimeoutError(
            f"Step timeout after {timeout} sec waiting for condition={condition}; "
            f"last_observed={last_observed!r}"
        )
        diagnostic_hint = self._infer_timeout_hint(condition, list(recent_observations))
        timeout_payload = {
            "source": "wait_until.timeout",
            "timeout_sec": timeout,
            "condition": condition,
            "hint": diagnostic_hint,
            "last_observed": last_observed,
            "observations": list(recent_observations),
        }
        setattr(timeout_error, "last_observed", last_observed)
        setattr(timeout_error, "timeout_payload", timeout_payload)
        setattr(timeout_error, "diagnostic_hint", diagnostic_hint)
        raise timeout_error

    @staticmethod
    def _normalize_hint(hint: str | None) -> str | None:
        if not hint:
            return None
        normalized = "_".join(str(hint).strip().lower().split())
        return normalized or None

    def _compose_error_reason(self, message: str, hint: str | None) -> str:
        normalized_hint = self._normalize_hint(hint)
        if not normalized_hint:
            return message
        return f"{message}; hint={normalized_hint}"

    def _infer_timeout_hint(self, condition: dict[str, Any], observations: list[dict[str, Any]]) -> str | None:
        hints = [
            self._infer_roles_hint(observation.get("value"))
            for observation in observations
            if isinstance(observation, dict)
        ]
        for preferred_hint in ("multiple_masters", "no_master", "master_still_alive"):
            if preferred_hint in hints:
                return preferred_hint

        role_snapshots: list[dict[str, Any]] = []
        for observation in observations:
            if not isinstance(observation, dict):
                continue
            value = observation.get("value")
            if not isinstance(value, dict):
                continue
            current_roles = value.get("current_roles")
            if isinstance(current_roles, dict):
                role_snapshots.append(current_roles)
        if len(role_snapshots) >= 2 and all(snapshot == role_snapshots[0] for snapshot in role_snapshots[1:]):
            return "no_role_change_detected"
        return None

    @staticmethod
    def _infer_roles_hint(value: Any) -> str | None:
        if not isinstance(value, dict):
            return None
        current_roles = value.get("current_roles")
        if not isinstance(current_roles, dict):
            return None
        masters_raw = current_roles.get("masters")
        masters = masters_raw if isinstance(masters_raw, list) else []
        if len(masters) > 1:
            return "multiple_masters"
        if len(masters) == 0:
            return "no_master"
        master_name = current_roles.get("master")
        per_node = value.get("nodes")
        if isinstance(master_name, str) and isinstance(per_node, dict):
            master_info = per_node.get(master_name)
            if isinstance(master_info, dict) and str(master_info.get("status", "")).lower() == "up":
                return "master_still_alive"
        return None

    def validate(self, run_id: str, expected: Any, observation: Observation) -> tuple[bool, Any, str | None]:
        """Проверка результата шага на соответствие expected."""
        actual = {
            "timestamp": observation.timestamp.isoformat(),
            "source": observation.source,
            "metric_event": observation.metric_event,
            "value": observation.value,
        }
        if expected is None:
            return True, actual, None
        if self._matches_condition(observation.value, expected, run_id=run_id):
            return True, actual, None
        return False, actual, f"expected={expected!r}, actual={observation.value!r}"

    @staticmethod
    def _observe_default() -> Observation:
        return Observation(
            timestamp=datetime.now(UTC),
            source="demo-runner",
            metric_event="state",
            value="ok",
        )

    def _resolve_observation_source(self, run_id: str, step: Step) -> Callable[[], Observation]:
        action_type = step.action_type.lower().strip()
        if action_type == "check_cluster_health":
            return lambda: self._observe_cluster_health(step)
        if action_type == "verify_roles":
            return lambda: self._observe_roles(step)
        if action_type == "verify_availability":
            return lambda: self._observe_availability(run_id, step)
        return self._observe_default

    def _observe_cluster_health(self, step: Step) -> Observation:
        from cluster_demo import classify_node_role

        cluster_state = self._fetch_cluster_state(step)
        up_nodes = [row.get("node") for row in cluster_state["rows"] if str(row.get("status")).lower() == "up"]
        masters = [
            str(row.get("node"))
            for row in cluster_state["rows"]
            if classify_node_role(row.get("role"), row.get("tx_read_only")) == "master"
        ]
        slaves = [
            str(row.get("node"))
            for row in cluster_state["rows"]
            if classify_node_role(row.get("role"), row.get("tx_read_only")) == "slave"
        ]
        value = {
            "cluster_config_path": str(cluster_state["cluster_config_path"]),
            "up_nodes": up_nodes,
            "total_nodes": len(cluster_state["rows"]),
            "all_nodes_up": len(up_nodes) == len(cluster_state["rows"]) and bool(cluster_state["rows"]),
            "current_roles": {
                "master": masters[0] if len(masters) == 1 else None,
                "slave": slaves[0] if len(slaves) == 1 else None,
            },
        }
        return Observation(
            timestamp=datetime.now(UTC),
            source="cluster-state",
            metric_event="cluster_health",
            value=value,
        )

    def _observe_roles(self, step: Step) -> Observation:
        from cluster_demo import classify_node_role

        cluster_state = self._fetch_cluster_state(step)
        nodes = {
            str(row.get("node")): {
                "status": row.get("status"),
                "role": row.get("role"),
                "tx_read_only": row.get("tx_read_only"),
                "replication_lag_sec": row.get("replication_lag_sec", row.get("replication_lag")),
            }
            for row in cluster_state["rows"]
        }
        roles_by_node = {node: str(node_data.get("role")) for node, node_data in nodes.items()}
        masters = [
            str(row.get("node"))
            for row in cluster_state["rows"]
            if classify_node_role(row.get("role"), row.get("tx_read_only")) == "master"
        ]
        slaves = [
            str(row.get("node"))
            for row in cluster_state["rows"]
            if classify_node_role(row.get("role"), row.get("tx_read_only")) == "slave"
        ]
        current_roles: dict[str, Any] = {
            "master": masters[0] if len(masters) == 1 else None,
            "slave": slaves[0] if len(slaves) == 1 else None,
            "masters": masters,
            "slaves": slaves,
            "master_count": len(masters),
            "slave_count": len(slaves),
            "roles_by_node": roles_by_node,
            "has_single_master": len(masters) == 1,
            "all_other_nodes_are_slaves": len(slaves) == (len(cluster_state["rows"]) - 1) and bool(cluster_state["rows"]),
            "cardinality_ok": len(masters) == 1
            and len(slaves) == (len(cluster_state["rows"]) - 1)
            and bool(cluster_state["rows"]),
        }
        value = {
            "cluster_config_path": str(cluster_state["cluster_config_path"]),
            "current_roles": current_roles,
            "nodes": nodes,
        }
        value["hint"] = self._infer_roles_hint(value)
        return Observation(
            timestamp=datetime.now(UTC),
            source="cluster-state",
            metric_event="roles",
            value=value,
        )

    def _observe_availability(self, run_id: str, step: Step) -> Observation:
        cluster_state = self._fetch_cluster_state(step)
        with self._lock:
            run_metrics = dict(self._run_metrics.get(run_id, {}))
        measured_downtime_sec = float(
            run_metrics.get(
                "last_switchover_downtime_sec",
                run_metrics.get("last_switchover_duration_sec", 0.0),
            )
        )
        slo_window_sec = float(step.params.get("slo_window_sec", 60.0))
        availability_ratio = 1.0 if slo_window_sec <= 0 else max(0.0, 1.0 - (measured_downtime_sec / slo_window_sec))
        value = {
            "cluster_config_path": str(cluster_state["cluster_config_path"]),
            "measured_downtime_sec": round(measured_downtime_sec, 3),
            "availability_ratio": round(availability_ratio, 6),
            "slo_window_sec": slo_window_sec,
            "nodes_up": [
                row.get("node") for row in cluster_state["rows"] if str(row.get("status")).lower() == "up"
            ],
        }
        return Observation(
            timestamp=datetime.now(UTC),
            source="cluster-state",
            metric_event="availability",
            value=value,
        )

    def _fetch_cluster_state(self, step: Step) -> dict[str, Any]:
        from cluster_demo import fetch_all_node_metrics, get_target_database, load_cluster_config

        cluster_config_path = self._resolve_cluster_config_path(step.params)
        cluster = load_cluster_config(cluster_config_path)
        target_db = get_target_database(cluster, "rw")
        rows = fetch_all_node_metrics(cluster.nodes, target_db)
        return {
            "cluster_config_path": cluster_config_path,
            "rows": rows,
        }

    def _record_run_metrics(self, run_id: str, action_type: str, action_result: dict[str, Any]) -> None:
        normalized_action = action_type.lower().strip()
        if normalized_action != "switchover":
            return
        orchestration_duration = (
            action_result.get("orchestration", {}).get("switchover_duration_sec")
            if isinstance(action_result.get("orchestration"), dict)
            else None
        )
        if orchestration_duration is None:
            return
        switchover_downtime = (
            action_result.get("orchestration", {}).get("downtime_sec")
            if isinstance(action_result.get("orchestration"), dict)
            else None
        )
        with self._lock:
            metrics = self._run_metrics.setdefault(run_id, {})
            metrics["last_switchover_duration_sec"] = float(orchestration_duration)
            metrics["last_switchover_orchestration_duration_sec"] = float(orchestration_duration)
            if isinstance(switchover_downtime, (float, int)):
                metrics["last_switchover_downtime_sec"] = float(switchover_downtime)

    def _matches_condition(self, actual: Any, condition: Any, run_id: str | None = None) -> bool:
        if condition in ({}, None):
            return True
        if isinstance(condition, str):
            condition = self._resolve_runtime_reference(run_id, condition)
        if isinstance(condition, dict):
            operator_keys = {
                "equals",
                "not_equals",
                "lte",
                "gte",
                "lt",
                "gt",
                "in",
                "count_equals",
                "count_gte",
                "count_lte",
            }
            if condition.keys() and set(condition.keys()).issubset(operator_keys):
                return self._apply_scalar_condition(actual, condition, run_id=run_id)
            if not isinstance(actual, dict):
                return False
            return all(
                key in actual and self._matches_condition(actual.get(key), expected_value, run_id=run_id)
                for key, expected_value in condition.items()
            )
        return actual == condition

    def _apply_scalar_condition(self, actual: Any, condition: dict[str, Any], run_id: str | None = None) -> bool:
        def _collection_size(value: Any) -> int | None:
            if isinstance(value, (list, tuple, set, dict, str)):
                return len(value)
            return None

        equals_value = self._resolve_runtime_condition_value(run_id, condition.get("equals"))
        if "equals" in condition and actual != equals_value:
            return False
        not_equals_value = self._resolve_runtime_condition_value(run_id, condition.get("not_equals"))
        if "not_equals" in condition and actual == not_equals_value:
            return False
        if "lte" in condition and not (actual <= condition["lte"]):
            return False
        if "gte" in condition and not (actual >= condition["gte"]):
            return False
        if "lt" in condition and not (actual < condition["lt"]):
            return False
        if "gt" in condition and not (actual > condition["gt"]):
            return False
        if "in" in condition and actual not in condition["in"]:
            return False
        collection_size = _collection_size(actual)
        if "count_equals" in condition:
            if collection_size is None or collection_size != int(condition["count_equals"]):
                return False
        if "count_gte" in condition:
            if collection_size is None or collection_size < int(condition["count_gte"]):
                return False
        if "count_lte" in condition:
            if collection_size is None or collection_size > int(condition["count_lte"]):
                return False
        return True

    def _resolve_runtime_condition_value(self, run_id: str | None, value: Any) -> Any:
        if isinstance(value, str):
            return self._resolve_runtime_reference(run_id, value)
        return value

    def _resolve_runtime_reference(self, run_id: str | None, value: str) -> Any:
        if run_id is None:
            return value
        if value.startswith("__RUN_METRIC:") and value.endswith("__"):
            key = value[len("__RUN_METRIC:") : -2].strip()
            if not key:
                return value
            with self._lock:
                return self._run_metrics.get(run_id, {}).get(key, value)
        if value.startswith("__STEP_METRIC:") and value.endswith("__"):
            payload = value[len("__STEP_METRIC:") : -2]
            try:
                step_name, lookup_path = payload.split(":", 1)
            except ValueError:
                return value
            with self._lock:
                step_metrics = (
                    self._run_metrics.get(run_id, {}).get("observations_by_step", {}).get(step_name.strip())
                )
            return self._lookup_dict_path(step_metrics, lookup_path.strip(), value)
        return value

    @staticmethod
    def _lookup_dict_path(container: Any, path: str, fallback: Any) -> Any:
        if not isinstance(container, dict) or not path:
            return fallback
        current: Any = container
        for key in path.split("."):
            if not isinstance(current, dict) or key not in current:
                return fallback
            current = current[key]
        return current


def build_default_scenarios() -> list[Scenario]:
    return [
        Scenario(
            id="failover-smoke",
            name="Failover smoke test",
            description="Проверка сценария перезапуска standby и валидации восстановления репликации.",
            success_criteria="Все шаги завершились без тайм-аутов и с ожидаемыми observation.value.",
            steps=[
                Step(
                    action_type="stop_db_service",
                    target_node=CURRENT_STANDBY_TARGET_MARKER,
                    params={"service": "postgrespro", "environment": "demo", "demo_mode": True},
                    wait_condition={"equals": "ok"},
                    timeout=15,
                    expected="ok",
                ),
                Step(
                    action_type="recover_action",
                    target_node=CURRENT_STANDBY_TARGET_MARKER,
                    params={"environment": "demo", "demo_mode": True},
                    wait_condition={"equals": "ok"},
                    timeout=20,
                    expected="ok",
                ),
            ],
        )
    ]


_DEFAULT_RUNNER: DemoRunner | None = None
_DEFAULT_RUNNER_LOCK = threading.Lock()
_DEFAULT_SCENARIO_CATALOG_STATUS = ScenarioCatalogStatus(
    loaded_from="defaults",
    fallback_used=True,
    error="Demo runner is not initialized yet",
)


def _load_scenarios_with_fallback() -> tuple[list[Scenario], ScenarioCatalogStatus]:
    scenarios_dir = Path(__file__).resolve().parents[2] / "config" / "demo_scenarios"
    try:
        loaded_dto = load_scenarios_from_directory(scenarios_dir)
        if not loaded_dto:
            return (
                build_default_scenarios(),
                ScenarioCatalogStatus(
                    loaded_from="defaults",
                    fallback_used=True,
                    error=None,
                ),
            )

        loaded_scenarios = [
            Scenario(
                id=item.id,
                name=item.name,
                description=item.description,
                steps=[
                    Step(
                        action_type=step.action_type,
                        target_node=step.target_node,
                        params=step.params,
                        wait_condition=step.wait_condition,
                        timeout=step.timeout,
                        expected=step.expected,
                    )
                    for step in item.steps
                ],
                success_criteria=item.success_criteria,
            )
            for item in loaded_dto
        ]
        return (
            loaded_scenarios,
            ScenarioCatalogStatus(
                loaded_from=str(scenarios_dir),
                fallback_used=False,
                error=None,
            ),
        )
    except ScenarioLoadError as exc:
        return (
            build_default_scenarios(),
            ScenarioCatalogStatus(
                loaded_from="defaults",
                fallback_used=True,
                error=f"Failed to load YAML scenarios: {exc}",
            ),
        )


def get_scenario_catalog_status() -> ScenarioCatalogStatus:
    return _DEFAULT_SCENARIO_CATALOG_STATUS


def get_demo_runner() -> DemoRunner:
    global _DEFAULT_RUNNER, _DEFAULT_SCENARIO_CATALOG_STATUS
    with _DEFAULT_RUNNER_LOCK:
        if _DEFAULT_RUNNER is None:
            scenarios, status = _load_scenarios_with_fallback()
            _DEFAULT_RUNNER = DemoRunner(scenarios)
            _DEFAULT_SCENARIO_CATALOG_STATUS = status
    return _DEFAULT_RUNNER
