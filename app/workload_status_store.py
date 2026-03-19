from __future__ import annotations

import json
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

STATUS_FILE = Path("logs/workload_status.json")


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
