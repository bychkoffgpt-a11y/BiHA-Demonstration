from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Iterator

import fcntl

STATUS_FILE = Path("logs/workload_status.json")
STATUS_LOCK_FILE = Path("logs/workload_status.lock")
DEFAULT_DESIRED_STATE = {
    "is_running": False,
    "mode": "rw",
    "clients": 10,
    "threads_per_client": 1,
    "read_ratio": 0.7,
}


class WorkloadStatusConflictError(RuntimeError):
    """Raised when the shared desired workload state changed before a write completed."""


def _status_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_desired_state(payload: dict[str, Any] | None) -> dict[str, Any]:
    desired = dict(DEFAULT_DESIRED_STATE)
    if payload:
        desired.update(payload)
    desired["is_running"] = bool(desired["is_running"])
    desired["mode"] = str(desired["mode"])
    desired["clients"] = max(1, int(desired["clients"]))
    desired["threads_per_client"] = max(1, int(desired["threads_per_client"]))
    desired["read_ratio"] = max(0.0, min(1.0, float(desired["read_ratio"])))
    return desired


def _build_default_status() -> dict[str, Any]:
    desired = _normalize_desired_state(None)
    return {
        "revision": 0,
        "desired": desired,
        "desired_updated_at": None,
        "runtime": {
            "is_running": False,
            "requested_threads": desired["clients"] * desired["threads_per_client"],
            "applied_revision": 0,
            "updated_at": None,
            "last_error": None,
        },
    }


def _normalize_status_document(status: dict[str, Any]) -> dict[str, Any]:
    normalized = _build_default_status()
    if not isinstance(status, dict):
        return normalized

    desired = _normalize_desired_state(status.get("desired") if isinstance(status.get("desired"), dict) else None)
    revision = max(0, int(status.get("revision", 0) or 0))

    runtime_payload = status.get("runtime") if isinstance(status.get("runtime"), dict) else {}
    requested_threads = runtime_payload.get("requested_threads", desired["clients"] * desired["threads_per_client"])
    applied_revision = runtime_payload.get("applied_revision", 0)
    last_error = runtime_payload.get("last_error")

    normalized.update(
        {
            "revision": revision,
            "desired": desired,
            "desired_updated_at": status.get("desired_updated_at"),
            "runtime": {
                "is_running": bool(runtime_payload.get("is_running", False)),
                "requested_threads": max(1, int(requested_threads)),
                "applied_revision": max(0, int(applied_revision or 0)),
                "updated_at": runtime_payload.get("updated_at"),
                "last_error": None if last_error in (None, "") else str(last_error),
            },
        }
    )
    return normalized


def normalize_workload_status(status: dict[str, Any]) -> dict[str, Any]:
    normalized = _normalize_status_document(status)
    desired = normalized["desired"]
    runtime = normalized["runtime"]
    requested_threads = max(1, int(runtime.get("requested_threads", desired["clients"] * desired["threads_per_client"])))
    return {
        "revision": int(normalized["revision"]),
        "desired_updated_at": normalized.get("desired_updated_at"),
        "mode": desired["mode"],
        "clients": int(desired["clients"]),
        "threads_per_client": int(desired["threads_per_client"]),
        "read_ratio": float(desired["read_ratio"]),
        "desired_is_running": bool(desired["is_running"]),
        "is_running": bool(runtime.get("is_running")),
        "requested_threads": requested_threads,
        "applied_revision": int(runtime.get("applied_revision", 0) or 0),
        "runtime_updated_at": runtime.get("updated_at"),
        "last_error": runtime.get("last_error"),
        "is_applied": int(runtime.get("applied_revision", 0) or 0) >= int(normalized["revision"]),
    }


def _read_status_file_unlocked() -> dict[str, Any]:
    if not STATUS_FILE.exists():
        return _build_default_status()
    try:
        parsed = json.loads(STATUS_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return _build_default_status()
    return _normalize_status_document(parsed)


def _write_status_file_unlocked(status: dict[str, Any]) -> dict[str, Any]:
    STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
    normalized = _normalize_status_document(status)
    with NamedTemporaryFile("w", encoding="utf-8", dir=STATUS_FILE.parent, delete=False) as tmp:
        json.dump(normalized, tmp, ensure_ascii=False)
        tmp.flush()
        Path(tmp.name).replace(STATUS_FILE)
    return normalized


@contextmanager
def _status_lock() -> Iterator[None]:
    STATUS_LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
    with STATUS_LOCK_FILE.open("a+", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def read_workload_status() -> dict[str, Any]:
    return _read_status_file_unlocked()


def update_workload_desired_state(
    desired_patch: dict[str, Any],
    *,
    expected_revision: int | None = None,
) -> dict[str, Any]:
    with _status_lock():
        current = _read_status_file_unlocked()
        current_revision = int(current["revision"])
        if expected_revision is not None and current_revision != int(expected_revision):
            raise WorkloadStatusConflictError(
                f"Shared desired workload state changed: expected revision {expected_revision}, actual {current_revision}"
            )

        desired = dict(current["desired"])
        desired.update(desired_patch)
        current["desired"] = _normalize_desired_state(desired)
        current["revision"] = current_revision + 1
        current["desired_updated_at"] = _status_now_iso()
        return _write_status_file_unlocked(current)


def write_runtime_workload_status(runtime_patch: dict[str, Any]) -> dict[str, Any]:
    with _status_lock():
        current = _read_status_file_unlocked()
        runtime = dict(current["runtime"])
        runtime.update(runtime_patch)
        current["runtime"] = runtime
        return _write_status_file_unlocked(current)
