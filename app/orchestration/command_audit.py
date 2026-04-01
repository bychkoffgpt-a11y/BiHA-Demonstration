from __future__ import annotations

from datetime import UTC, datetime
import threading
from typing import Any


class CommandAudit:
    """Thread-safe collector for commands executed during a scenario step."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._active_scopes: list[str] = []
        self._records_by_scope: dict[str, list[dict[str, Any]]] = {}

    def start_scope(self, scope_id: str) -> None:
        with self._lock:
            self._active_scopes.append(scope_id)
            self._records_by_scope.setdefault(scope_id, [])

    def finish_scope(self, scope_id: str) -> list[dict[str, Any]]:
        with self._lock:
            if scope_id in self._active_scopes:
                self._active_scopes = [scope for scope in self._active_scopes if scope != scope_id]
            return list(self._records_by_scope.pop(scope_id, []))

    def record(self, command_type: str, command: str, **meta: Any) -> None:
        rendered_command = str(command).strip()
        if not rendered_command:
            return
        with self._lock:
            if not self._active_scopes:
                return
            active_scope = self._active_scopes[-1]
            record: dict[str, Any] = {
                "timestamp": datetime.now(UTC).isoformat(),
                "type": command_type,
                "command": rendered_command,
            }
            for key, value in meta.items():
                if value is None:
                    continue
                record[key] = value
            self._records_by_scope.setdefault(active_scope, []).append(record)


COMMAND_AUDIT = CommandAudit()


def start_command_scope(scope_id: str) -> None:
    COMMAND_AUDIT.start_scope(scope_id)


def finish_command_scope(scope_id: str) -> list[dict[str, Any]]:
    return COMMAND_AUDIT.finish_scope(scope_id)


def record_command(command_type: str, command: str, **meta: Any) -> None:
    COMMAND_AUDIT.record(command_type=command_type, command=command, **meta)
