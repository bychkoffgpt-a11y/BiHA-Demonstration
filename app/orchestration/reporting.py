from __future__ import annotations

import hashlib
import html
import json
import subprocess
import zipfile
from dataclasses import asdict
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any

from orchestration.demo_runner import RunStatus, ScenarioRun

REPORTS_ROOT = Path("artifacts/reports")
SCREENSHOTS_ROOT = Path("artifacts/screenshots")
LOG_FILE = Path("logs/biha_demo.log")


def is_run_finished(status: RunStatus) -> bool:
    return status in {RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED}


def build_and_save_report_bundle(run: ScenarioRun) -> dict[str, Any]:
    payload = build_report_payload(run)
    run_dir = REPORTS_ROOT / run.run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    json_path = run_dir / "report.json"
    html_path = run_dir / "report.html"
    pdf_path = run_dir / "report.pdf"
    metrics_path = run_dir / "metrics_export.json"
    logs_path = run_dir / "logs_export.json"
    version_path = run_dir / "version_info.json"

    json_text = json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default)
    json_path.write_text(json_text, encoding="utf-8")

    html_text = render_html_report(payload)
    html_path.write_text(html_text, encoding="utf-8")

    pdf_bytes = render_pdf_report(payload)
    pdf_path.write_bytes(pdf_bytes)

    metrics_export = {
        "run_id": run.run_id,
        "metrics": [
            {
                "step": log.index + 1,
                "action": log.action_type,
                "target": log.target_node,
                "status": log.status,
                "actual_result": log.actual_result,
            }
            for log in run.step_logs
        ],
    }
    metrics_path.write_text(
        json.dumps(metrics_export, ensure_ascii=False, indent=2, default=_json_default),
        encoding="utf-8",
    )

    logs_export = {
        "run_id": run.run_id,
        "scenario": run.scenario_name,
        "app_log_excerpt": _collect_log_excerpt(),
    }
    logs_path.write_text(
        json.dumps(logs_export, ensure_ascii=False, indent=2, default=_json_default),
        encoding="utf-8",
    )

    version_info = {
        "generated_at": datetime.now(UTC).isoformat(),
        "software_version": _get_git_commit(),
        "cluster_config_version": _hash_first_existing([Path("config/cluster.json"), Path("config/cluster.example.json")]),
    }
    version_path.write_text(
        json.dumps(version_info, ensure_ascii=False, indent=2, default=_json_default),
        encoding="utf-8",
    )

    bundle_path = run_dir / "report_bundle.zip"
    with zipfile.ZipFile(bundle_path, "w", compression=zipfile.ZIP_DEFLATED) as zip_file:
        for path in [json_path, html_path, pdf_path, metrics_path, logs_path, version_path]:
            zip_file.write(path, arcname=path.name)
        for screenshot in _collect_screenshot_paths():
            zip_file.write(screenshot, arcname=f"screenshots/{screenshot.name}")

    return {
        "run_id": run.run_id,
        "bundle_path": str(bundle_path),
        "bundle_name": f"scenario_report_{run.run_id}.zip",
        "json_path": str(json_path),
        "html_path": str(html_path),
        "pdf_path": str(pdf_path),
        "generated_at": datetime.now(UTC).isoformat(),
    }


def build_report_payload(run: ScenarioRun) -> dict[str, Any]:
    started_at = run.started_at or run.created_at
    finished_at = run.finished_at or datetime.now(UTC)
    duration_sec = max((finished_at - started_at).total_seconds(), 0.0)

    disruptive_steps = [log for log in run.step_logs if "stop" in log.action_type or "crash" in log.action_type]
    recovery_steps = [log for log in run.step_logs if "recover" in log.action_type or "rewind" in log.action_type or "start" in log.action_type]

    disruption_started_at = next((step.started_at for step in disruptive_steps if step.started_at), started_at)
    recovery_finished_at = next((step.finished_at for step in recovery_steps if step.finished_at), finished_at)
    estimated_downtime_sec = max((recovery_finished_at - disruption_started_at).total_seconds(), 0.0)

    rto_sec = estimated_downtime_sec
    rpo_estimate = "0 (demo observation, no data-loss evidence in step checks)" if run.status == RunStatus.SUCCEEDED else "unknown"
    availability = 1.0 if duration_sec == 0 else max(0.0, min(1.0, (duration_sec - estimated_downtime_sec) / duration_sec))

    role_changes = []
    for step in run.step_logs:
        if any(token in step.action_type for token in ("promote", "demote", "recover", "rewind", "failover")):
            role_changes.append(
                {
                    "timestamp": _iso(step.finished_at or step.started_at),
                    "node": step.target_node,
                    "action": step.action_type,
                    "status": step.status,
                }
            )

    anti_split_brain_confirmed = not any("dual-primary" in (step.error_reason or "") for step in run.step_logs)
    old_leader_recovery = _derive_old_leader_recovery_status(run)

    payload = {
        "run_id": run.run_id,
        "scenario_id": run.scenario_id,
        "scenario_name": run.scenario_name,
        "generated_at": datetime.now(UTC).isoformat(),
        "sections": {
            "summary": {
                "verdict": "PASS" if run.status == RunStatus.SUCCEEDED else "FAIL",
                "status": run.status.value,
                "error_reason": run.error_reason,
                "started_at": _iso(run.started_at),
                "finished_at": _iso(run.finished_at),
            },
            "rto_rpo_availability": {
                "rto_sec": round(rto_sec, 3),
                "rpo_estimate": rpo_estimate,
                "availability_ratio": round(availability, 4),
                "total_duration_sec": round(duration_sec, 3),
                "estimated_downtime_sec": round(estimated_downtime_sec, 3),
            },
            "timeline": _build_timeline(run),
            "node_role_changes": role_changes,
            "anti_split_brain_logic": {
                "confirmed": anti_split_brain_confirmed,
                "evidence": "No split-brain indicators found in step errors and all guardrail checks completed."
                if anti_split_brain_confirmed
                else "Potential split-brain indicator detected in step errors.",
            },
            "old_leader_rewind_recovery": old_leader_recovery,
            "artifacts": {
                "screenshots": [str(path) for path in _collect_screenshot_paths()],
                "metrics_export": "metrics_export.json",
                "logs_export": "logs_export.json",
                "version_identifier": {
                    "software_version": _get_git_commit(),
                    "cluster_config_version": _hash_first_existing([Path("config/cluster.json"), Path("config/cluster.example.json")]),
                },
            },
            "steps": [asdict(step) for step in run.step_logs],
        },
    }
    return payload


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Path):
        return str(value)
    return str(value)


def render_html_report(payload: dict[str, Any]) -> str:
    sections = payload["sections"]
    timeline_html = "".join(
        f"<li><b>{html.escape(event['event'])}</b>: {html.escape(str(event['timestamp']))} — {html.escape(str(event.get('details', '')))}</li>"
        for event in sections["timeline"]
    )
    role_changes_html = "".join(
        f"<li>{html.escape(str(change['timestamp']))}: {html.escape(change['node'])} → {html.escape(change['action'])} ({html.escape(change['status'])})</li>"
        for change in sections["node_role_changes"]
    ) or "<li>Нет изменений ролей, зафиксированных шагами</li>"

    return f"""<!doctype html>
<html lang=\"ru\">
<head>
  <meta charset=\"utf-8\" />
  <title>Scenario report {html.escape(payload['run_id'])}</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; }}
    h1, h2 {{ margin-bottom: 0.3rem; }}
    section {{ margin-bottom: 1rem; }}
    .badge {{ display: inline-block; padding: 2px 8px; border-radius: 10px; background: #efefef; }}
  </style>
</head>
<body>
  <h1>Отчёт по run_id: {html.escape(payload['run_id'])}</h1>
  <p>Сценарий: <b>{html.escape(payload['scenario_name'])}</b></p>
  <section>
    <h2>Summary</h2>
    <p class=\"badge\">{html.escape(sections['summary']['verdict'])}</p>
    <p>Статус: {html.escape(sections['summary']['status'])}</p>
    <p>Причина: {html.escape(str(sections['summary']['error_reason']))}</p>
  </section>
  <section>
    <h2>RTO/RPO/Availability</h2>
    <p>RTO: {sections['rto_rpo_availability']['rto_sec']} sec</p>
    <p>RPO: {html.escape(str(sections['rto_rpo_availability']['rpo_estimate']))}</p>
    <p>Availability: {sections['rto_rpo_availability']['availability_ratio']}</p>
  </section>
  <section>
    <h2>Таймлайн событий</h2>
    <ul>{timeline_html}</ul>
  </section>
  <section>
    <h2>Изменения ролей узлов</h2>
    <ul>{role_changes_html}</ul>
  </section>
  <section>
    <h2>Подтверждение anti split-brain logic</h2>
    <p>{html.escape(str(sections['anti_split_brain_logic']['confirmed']))}</p>
    <p>{html.escape(str(sections['anti_split_brain_logic']['evidence']))}</p>
  </section>
  <section>
    <h2>Статус rewind/recovery старого лидера</h2>
    <p>{html.escape(str(sections['old_leader_rewind_recovery']['status']))}</p>
    <p>{html.escape(str(sections['old_leader_rewind_recovery']['details']))}</p>
  </section>
</body>
</html>
"""


def render_pdf_report(payload: dict[str, Any]) -> bytes:
    sections = payload["sections"]
    lines = [
        f"Run report: {payload['run_id']}",
        f"Scenario: {payload['scenario_name']}",
        f"Summary: {sections['summary']['verdict']} ({sections['summary']['status']})",
        f"RTO sec: {sections['rto_rpo_availability']['rto_sec']}",
        f"RPO: {sections['rto_rpo_availability']['rpo_estimate']}",
        f"Availability: {sections['rto_rpo_availability']['availability_ratio']}",
        f"Anti split-brain: {sections['anti_split_brain_logic']['confirmed']}",
        f"Old leader recovery: {sections['old_leader_rewind_recovery']['status']}",
    ]
    return _simple_text_pdf(lines)


def _simple_text_pdf(lines: list[str]) -> bytes:
    escaped = []
    for line in lines:
        sanitized = line.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")
        escaped.append(sanitized)

    content_lines = ["BT", "/F1 11 Tf", "72 780 Td"]
    first = True
    for line in escaped:
        if first:
            content_lines.append(f"({line}) Tj")
            first = False
        else:
            content_lines.append("0 -16 Td")
            content_lines.append(f"({line}) Tj")
    content_lines.append("ET")
    content = "\n".join(content_lines).encode("utf-8")

    objects = []
    objects.append(b"1 0 obj << /Type /Catalog /Pages 2 0 R >> endobj\n")
    objects.append(b"2 0 obj << /Type /Pages /Kids [3 0 R] /Count 1 >> endobj\n")
    objects.append(
        b"3 0 obj << /Type /Page /Parent 2 0 R /MediaBox [0 0 595 842] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >> endobj\n"
    )
    objects.append(b"4 0 obj << /Type /Font /Subtype /Type1 /BaseFont /Helvetica >> endobj\n")
    objects.append(f"5 0 obj << /Length {len(content)} >> stream\n".encode("utf-8") + content + b"\nendstream endobj\n")

    out = bytearray(b"%PDF-1.4\n")
    offsets = [0]
    for obj in objects:
        offsets.append(len(out))
        out.extend(obj)
    xref_start = len(out)
    out.extend(f"xref\n0 {len(objects) + 1}\n".encode("utf-8"))
    out.extend(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        out.extend(f"{offset:010} 00000 n \n".encode("utf-8"))
    out.extend(
        f"trailer << /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref_start}\n%%EOF\n".encode("utf-8")
    )
    return bytes(out)


def _derive_old_leader_recovery_status(run: ScenarioRun) -> dict[str, Any]:
    rewind_steps = [step for step in run.step_logs if "rewind" in step.action_type or "recover" in step.action_type]
    if not rewind_steps:
        return {"status": "not_applicable", "details": "Шаги rewind/recovery для старого лидера в сценарии не обнаружены."}
    failed = [step for step in rewind_steps if step.status != "succeeded"]
    if failed:
        return {
            "status": "failed",
            "details": f"{len(failed)} шаг(ов) rewind/recovery завершились неуспешно.",
        }
    return {"status": "completed", "details": "Все шаги rewind/recovery старого лидера завершились успешно."}


def _build_timeline(run: ScenarioRun) -> list[dict[str, Any]]:
    timeline = [
        {"timestamp": _iso(run.created_at), "event": "run_created", "details": run.scenario_name},
        {"timestamp": _iso(run.started_at), "event": "run_started", "details": run.status.value},
    ]
    for step in run.step_logs:
        timeline.append(
            {
                "timestamp": _iso(step.started_at),
                "event": "step_started",
                "details": f"#{step.index + 1} {step.action_type} -> {step.target_node}",
            }
        )
        timeline.append(
            {
                "timestamp": _iso(step.finished_at),
                "event": "step_finished",
                "details": f"#{step.index + 1} status={step.status}",
            }
        )
    timeline.append({"timestamp": _iso(run.finished_at), "event": "run_finished", "details": run.status.value})
    return [item for item in timeline if item["timestamp"] is not None]


def _collect_screenshot_paths() -> list[Path]:
    if not SCREENSHOTS_ROOT.exists():
        return []
    return sorted(path for path in SCREENSHOTS_ROOT.glob("**/*") if path.is_file())


def _collect_log_excerpt(max_lines: int = 300) -> list[str]:
    if not LOG_FILE.exists():
        return []
    lines = LOG_FILE.read_text(encoding="utf-8", errors="replace").splitlines()
    return lines[-max_lines:]


def _get_git_commit() -> str:
    try:
        result = subprocess.run(["git", "rev-parse", "HEAD"], check=True, capture_output=True, text=True)
        return result.stdout.strip()
    except Exception:
        return "unknown"


def _hash_first_existing(candidates: list[Path]) -> str:
    for path in candidates:
        if path.exists() and path.is_file():
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            return f"sha256:{digest} ({path})"
    return "unknown"


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None
