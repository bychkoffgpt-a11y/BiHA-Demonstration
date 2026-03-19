from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

STATUS_FILE = Path("logs/workload_status.json")
WORKLOAD_STATUS_HEARTBEAT_TIMEOUT_SEC = 5


def _parse_status_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    normalized_value = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized_value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def normalize_workload_status(
    status: dict[str, Any],
    *,
    local_running: bool = False,
    local_session_id: str | None = None,
) -> dict[str, Any]:
    normalized = dict(status)

    if local_running:
        normalized["is_running"] = True
        if local_session_id:
            normalized["owner_session_id"] = local_session_id
        return normalized

    if not bool(normalized.get("is_running")):
        return normalized

    owner_session_id = normalized.get("owner_session_id")
    updated_at = _parse_status_timestamp(normalized.get("updated_at"))
    heartbeat_is_fresh = (
        updated_at is not None
        and (datetime.now(timezone.utc) - updated_at).total_seconds() <= WORKLOAD_STATUS_HEARTBEAT_TIMEOUT_SEC
    )
    if owner_session_id and heartbeat_is_fresh:
        return normalized

    normalized["is_running"] = False
    normalized["owner_session_id"] = None
    return normalized


def read_workload_status() -> dict[str, Any]:
    if not STATUS_FILE.exists():
        return {}
    try:
        return json.loads(STATUS_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def write_workload_status(payload: dict[str, Any]) -> None:
    STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
    current = read_workload_status()
    current.update(payload)
    with NamedTemporaryFile("w", encoding="utf-8", dir=STATUS_FILE.parent, delete=False) as tmp:
        json.dump(current, tmp, ensure_ascii=False)
        tmp.flush()
        Path(tmp.name).replace(STATUS_FILE)


def issue_workload_command(command: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
    current = read_workload_status()
    next_command_id = int(current.get("command_id", 0)) + 1
    current.update(payload or {})
    current.update(
        {
            "command_id": next_command_id,
            "command": command,
        }
    )
    with NamedTemporaryFile("w", encoding="utf-8", dir=STATUS_FILE.parent, delete=False) as tmp:
        json.dump(current, tmp, ensure_ascii=False)
        tmp.flush()
        Path(tmp.name).replace(STATUS_FILE)
    return current
