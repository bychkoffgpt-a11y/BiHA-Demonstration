from __future__ import annotations

import threading
import time
import uuid
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
        self._scenario_timeout_sec = scenario_timeout_sec
        self._fault_injection = FaultInjectionController(max_injection_duration_sec=max_injection_duration_sec)
        self._lock = threading.RLock()

    def register_scenario(self, scenario: Scenario) -> None:
        with self._lock:
            self._scenarios[scenario.id] = scenario

    def list_scenarios(self) -> list[Scenario]:
        with self._lock:
            return list(self._scenarios.values())

    def start_scenario(self, scenario_id: str) -> str:
        with self._lock:
            scenario = self._scenarios.get(scenario_id)
            if scenario is None:
                raise ValueError(f"Unknown scenario_id: {scenario_id}")

            run_id = str(uuid.uuid4())
            run = ScenarioRun(
                run_id=run_id,
                scenario_id=scenario.id,
                scenario_name=scenario.name,
                status=RunStatus.PENDING,
                created_at=datetime.now(UTC),
                step_logs=[
                    StepRunLog(
                        index=index,
                        action_type=step.action_type,
                        target_node=step.target_node,
                        expected_result=step.expected,
                    )
                    for index, step in enumerate(scenario.steps)
                ],
            )
            self._runs[run_id] = run
            cancel_event = threading.Event()
            self._cancel_events[run_id] = cancel_event
            thread = threading.Thread(
                target=self._run_scenario,
                args=(run_id, scenario, cancel_event),
                name=f"scenario-run-{run_id[:8]}",
                daemon=True,
            )
            self._run_threads[run_id] = thread
            thread.start()
            LOGGER.info(
                "Scenario run started | run_id=%s scenario_id=%s scenario_name=%s steps=%s",
                run_id,
                scenario.id,
                scenario.name,
                len(scenario.steps),
            )
            return run_id

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
        action_result = self.execute_action(step)
        LOGGER.info(
            "Step action executed | run_id=%s step=%s rollback_id=%s details=%s",
            run_id,
            step_index + 1,
            action_result.get("rollback_id"),
            action_result.get("fault_injection"),
        )

        with self._lock:
            run = self._runs[run_id]
            run.current_step_index = step_index
            log = run.step_logs[step_index]
            log.status = "running"
            log.started_at = datetime.now(UTC)
            log.action_result = action_result

        observation = self.wait_until(step.wait_condition, step.timeout, cancel_event)
        is_valid, actual_result, reason = self.validate(step.expected, observation)

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
                log.status = "failed"
                log.error_reason = reason
                run.status = RunStatus.FAILED
                run.error_reason = f"Step {step_index + 1} failed: {reason}"
                run.finished_at = datetime.now(UTC)
                LOGGER.error(
                    "Step execution failed | run_id=%s step=%s reason=%s actual=%s",
                    run_id,
                    step_index + 1,
                    reason,
                    actual_result,
                )
                raise RuntimeError(run.error_reason)

    def _mark_cancelled(self, run_id: str, step_index: int, reason: str) -> None:
        with self._lock:
            run = self._runs[run_id]
            run.status = RunStatus.CANCELLED
            run.current_step_index = step_index
            run.error_reason = reason
            run.finished_at = datetime.now(UTC)

    def execute_action(self, step: Step) -> dict[str, Any]:
        """Выполнение действия шага с fault-injection guardrails и rollback metadata."""
        action_type = step.action_type.lower().strip()
        if action_type in ORCHESTRATION_ACTIONS:
            return {
                "action_type": action_type,
                "target_node": step.target_node,
                "params": step.params,
                "rollback_id": None,
                "fault_injection": {
                    "phase": "orchestration",
                    "simulated": True,
                    "step_name": step.params.get("step_name"),
                    "details": step.params.get("details"),
                },
                "executed_at": datetime.now(UTC).isoformat(),
            }

        result = self._fault_injection.execute(
            action_type=action_type,
            target_node=step.target_node,
            params=step.params,
        )
        return {
            "action_type": result.action_type,
            "target_node": step.target_node,
            "params": step.params,
            "rollback_id": result.rollback_id,
            "fault_injection": result.details,
            "executed_at": datetime.now(UTC).isoformat(),
        }

    def wait_until(
        self,
        condition: dict[str, Any],
        timeout: float,
        cancel_event: threading.Event,
        observation_source: Callable[[], Observation] | None = None,
    ) -> Observation:
        """Ожидание условия до timeout с периодическим опросом."""
        source_fn = observation_source or self._default_observation
        deadline = time.monotonic() + timeout
        expected_value = condition.get("equals")
        LOGGER.info(
            "Waiting for condition | expected=%r timeout_sec=%s condition=%s",
            expected_value,
            timeout,
            condition,
        )

        while time.monotonic() < deadline:
            if cancel_event.is_set():
                LOGGER.warning("Step wait cancelled by event")
                raise RuntimeError("Step cancelled")
            observation = source_fn()
            if expected_value is None or observation.value == expected_value:
                LOGGER.info("Condition met | observed_value=%r source=%s", observation.value, observation.source)
                return observation
            time.sleep(0.5)

        LOGGER.error(
            "Step wait timeout | timeout_sec=%s expected=%r condition=%s",
            timeout,
            expected_value,
            condition,
        )
        raise TimeoutError(f"Step timeout after {timeout} sec waiting for condition={condition}")

    def validate(self, expected: Any, observation: Observation) -> tuple[bool, Any, str | None]:
        """Проверка результата шага на соответствие expected."""
        actual = {
            "timestamp": observation.timestamp.isoformat(),
            "source": observation.source,
            "metric_event": observation.metric_event,
            "value": observation.value,
        }
        if expected is None:
            return True, actual, None
        if observation.value == expected:
            return True, actual, None
        return False, actual, f"expected={expected!r}, actual={observation.value!r}"

    @staticmethod
    def _default_observation() -> Observation:
        return Observation(
            timestamp=datetime.now(UTC),
            source="demo-runner",
            metric_event="state",
            value="ok",
        )


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
                    target_node="pg-node-2",
                    params={"service": "postgrespro", "environment": "demo", "demo_mode": True},
                    wait_condition={"equals": "ok"},
                    timeout=15,
                    expected="ok",
                ),
                Step(
                    action_type="recover_action",
                    target_node="pg-node-2",
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
