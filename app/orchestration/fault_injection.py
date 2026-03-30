from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Callable


RECOVER_ACTIONS = {"resume", "recover_action"}
ALLOWED_ACTIONS = {
    "stop_db_service",
    "kill_db_process",
    "network_partition",
    "pause_node",
    *RECOVER_ACTIONS,
}


@dataclass
class FaultInjectionResult:
    action_type: str
    target_node: str
    rollback_id: str | None
    details: dict[str, Any]


@dataclass
class _RollbackEntry:
    rollback_id: str
    action_type: str
    target_node: str
    rollback_fn: Callable[[], dict[str, Any]]


class FaultInjectionController:
    """Fault-injection слой с guardrails и процедурами rollback."""

    def __init__(self, max_injection_duration_sec: float = 30.0) -> None:
        self.max_injection_duration_sec = max_injection_duration_sec
        self._service_stopped_nodes: set[str] = set()
        self._killed_process_nodes: set[str] = set()
        self._partitioned_nodes: set[str] = set()
        self._paused_nodes: set[str] = set()
        self._rollback_stack: list[_RollbackEntry] = []
        self._rollback_seq = 0

    def execute(self, action_type: str, target_node: str, params: dict[str, Any]) -> FaultInjectionResult:
        normalized_action = action_type.lower().strip()
        if normalized_action not in ALLOWED_ACTIONS:
            raise ValueError(
                f"Action '{action_type}' is not allowed. Allowed actions: {sorted(ALLOWED_ACTIONS)}"
            )

        if normalized_action in RECOVER_ACTIONS:
            details = self.rollback(target_node=target_node, rollback_id=params.get("rollback_id"))
            return FaultInjectionResult(
                action_type=normalized_action,
                target_node=target_node,
                rollback_id=params.get("rollback_id"),
                details={"phase": "recover", **details},
            )

        self._run_precheck(target_node=target_node, params=params)
        self._validate_guardrails(action_type=normalized_action, params=params)

        rollback_entry = self._apply(action_type=normalized_action, target_node=target_node, params=params)
        verify = self._verify(action_type=normalized_action, target_node=target_node)
        if not verify["ok"]:
            rollback_payload = rollback_entry.rollback_fn()
            raise RuntimeError(
                f"Verification failed for action '{action_type}' on '{target_node}': {verify['reason']}. "
                f"Rollback applied: {rollback_payload}"
            )

        self._rollback_stack.append(rollback_entry)
        return FaultInjectionResult(
            action_type=normalized_action,
            target_node=target_node,
            rollback_id=rollback_entry.rollback_id,
            details={"phase": "inject", "verify": verify, "injected_at": datetime.now(UTC).isoformat()},
        )

    def rollback(self, target_node: str | None = None, rollback_id: str | None = None) -> dict[str, Any]:
        if rollback_id:
            return self._rollback_by_id(rollback_id)

        results: list[dict[str, Any]] = []
        retained: list[_RollbackEntry] = []
        for entry in reversed(self._rollback_stack):
            if target_node and entry.target_node != target_node:
                retained.insert(0, entry)
                continue
            results.append(entry.rollback_fn())

        self._rollback_stack = retained
        return {
            "ok": True,
            "rolled_back": len(results),
            "target_node": target_node,
            "results": results,
            "rolled_back_at": datetime.now(UTC).isoformat(),
        }

    def _rollback_by_id(self, rollback_id: str) -> dict[str, Any]:
        for index in range(len(self._rollback_stack) - 1, -1, -1):
            entry = self._rollback_stack[index]
            if entry.rollback_id == rollback_id:
                payload = entry.rollback_fn()
                del self._rollback_stack[index]
                return {
                    "ok": True,
                    "rolled_back": 1,
                    "rollback_id": rollback_id,
                    "results": [payload],
                    "rolled_back_at": datetime.now(UTC).isoformat(),
                }
        return {
            "ok": False,
            "rolled_back": 0,
            "rollback_id": rollback_id,
            "reason": "rollback_id not found",
            "rolled_back_at": datetime.now(UTC).isoformat(),
        }

    def _run_precheck(self, target_node: str, params: dict[str, Any]) -> None:
        if not target_node:
            raise ValueError("target_node is required")
        if params.get("node_reachable", True) is not True:
            raise RuntimeError(f"Pre-check failed: target node '{target_node}' is unreachable")
        if params.get("has_privileges", True) is not True:
            raise PermissionError(f"Pre-check failed: insufficient privileges for target node '{target_node}'")

    def _validate_guardrails(self, action_type: str, params: dict[str, Any]) -> None:
        environment = str(params.get("environment", "demo")).lower()
        demo_mode = params.get("demo_mode", False)
        if environment in {"prod", "production"} and demo_mode is not True:
            raise PermissionError(
                "Guardrail violation: fault injection against production is blocked unless demo_mode=true"
            )

        duration_sec = float(params.get("duration_sec", 0.0))
        if duration_sec < 0:
            raise ValueError("duration_sec must be >= 0")
        if duration_sec > self.max_injection_duration_sec:
            raise ValueError(
                f"Guardrail violation: duration_sec={duration_sec} exceeds limit={self.max_injection_duration_sec} "
                f"for action '{action_type}'"
            )

    def _apply(self, action_type: str, target_node: str, params: dict[str, Any]) -> _RollbackEntry:
        rollback_id = self._next_rollback_id(target_node)

        if action_type == "stop_db_service":
            self._service_stopped_nodes.add(target_node)

            def rollback_fn() -> dict[str, Any]:
                self._service_stopped_nodes.discard(target_node)
                return {"action": "start_db_service", "target_node": target_node, "ok": True}

        elif action_type == "kill_db_process":
            self._killed_process_nodes.add(target_node)

            def rollback_fn() -> dict[str, Any]:
                self._killed_process_nodes.discard(target_node)
                return {"action": "restart_db_process", "target_node": target_node, "ok": True}

        elif action_type == "network_partition":
            self._partitioned_nodes.add(target_node)
            peers = params.get("peers", [])

            def rollback_fn() -> dict[str, Any]:
                self._partitioned_nodes.discard(target_node)
                return {"action": "heal_network_partition", "target_node": target_node, "peers": peers, "ok": True}

        elif action_type == "pause_node":
            self._paused_nodes.add(target_node)

            def rollback_fn() -> dict[str, Any]:
                self._paused_nodes.discard(target_node)
                return {"action": "resume_node", "target_node": target_node, "ok": True}

        else:
            raise ValueError(f"Unsupported action '{action_type}'")

        return _RollbackEntry(
            rollback_id=rollback_id,
            action_type=action_type,
            target_node=target_node,
            rollback_fn=rollback_fn,
        )

    def _verify(self, action_type: str, target_node: str) -> dict[str, Any]:
        if action_type == "stop_db_service":
            ok = target_node in self._service_stopped_nodes
        elif action_type == "kill_db_process":
            ok = target_node in self._killed_process_nodes
        elif action_type == "network_partition":
            ok = target_node in self._partitioned_nodes
        elif action_type == "pause_node":
            ok = target_node in self._paused_nodes
        else:
            ok = False

        return {
            "ok": ok,
            "reason": None if ok else f"state not applied for action '{action_type}'",
            "verified_at": datetime.now(UTC).isoformat(),
        }

    def _next_rollback_id(self, target_node: str) -> str:
        self._rollback_seq += 1
        return f"rb-{target_node}-{self._rollback_seq}"
