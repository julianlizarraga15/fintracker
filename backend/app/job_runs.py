from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Optional

from pydantic import BaseModel, Field


class JobRunNotFound(RuntimeError):
    """Raised when no recorded job run can be located."""


JOB_RUNS_ROOT = Path(os.getenv("JOB_RUNS_LOCAL_DIR", "data/job_runs"))
DEFAULT_HISTORY_LIMIT = 14
MAX_HISTORY_LIMIT = 90


class JobRunSummary(BaseModel):
    job: str
    run_id: str
    status: str
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    duration_seconds: Optional[float] = None
    positions_count: Optional[int] = None
    valuations_saved: bool = False
    s3_uploaded: bool = False
    warnings_count: int = 0
    errors_count: int = 0


class JobRunResponse(BaseModel):
    job: str
    run_id: str
    status: str
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    duration_seconds: Optional[float] = None
    exit_code: Optional[int] = None
    positions_count: Optional[int] = None
    loaded: dict[str, int] = Field(default_factory=dict)
    pricing: dict[str, Any] = Field(default_factory=dict)
    outputs: dict[str, Any] = Field(default_factory=dict)
    uploads: list[str] = Field(default_factory=list)
    s3_uploaded: bool = False
    warnings: list[str] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    log_file: Optional[str] = None


def _job_dir(job_name: str) -> Path:
    sanitized = "".join(ch for ch in job_name if ch.isalnum() or ch in {"-", "_"}).strip()
    if not sanitized:
        raise JobRunNotFound("Invalid job name.")
    return JOB_RUNS_ROOT / sanitized


def _read_json(path: Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as fh:
            payload = json.load(fh)
    except FileNotFoundError as exc:
        raise JobRunNotFound(f"No job run found at {path}.") from exc
    except json.JSONDecodeError as exc:
        raise JobRunNotFound(f"Job run file {path} is not valid JSON.") from exc
    if not isinstance(payload, dict):
        raise JobRunNotFound(f"Job run file {path} did not contain an object.")
    return payload


def _run_files(job_name: str) -> list[Path]:
    base_dir = _job_dir(job_name)
    if not base_dir.exists():
        return []
    return sorted(
        [path for path in base_dir.glob("*.json") if path.name != "latest.json"],
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )


def _summarize(payload: dict[str, Any]) -> JobRunSummary:
    warnings = payload.get("warnings") if isinstance(payload.get("warnings"), list) else []
    errors = payload.get("errors") if isinstance(payload.get("errors"), list) else []
    return JobRunSummary(
        job=str(payload.get("job") or "unknown"),
        run_id=str(payload.get("run_id") or "unknown"),
        status=str(payload.get("status") or "unknown"),
        started_at=payload.get("started_at"),
        finished_at=payload.get("finished_at"),
        duration_seconds=payload.get("duration_seconds"),
        positions_count=payload.get("positions_count"),
        valuations_saved=bool(payload.get("outputs", {}).get("valuations")) if isinstance(payload.get("outputs"), dict) else False,
        s3_uploaded=bool(payload.get("s3_uploaded")),
        warnings_count=len(warnings),
        errors_count=len(errors),
    )


def get_latest_job_run(job_name: str = "valuations") -> JobRunResponse:
    latest_path = _job_dir(job_name) / "latest.json"
    if latest_path.exists():
        return JobRunResponse(**_read_json(latest_path))

    files = _run_files(job_name)
    if not files:
        raise JobRunNotFound(f"No recorded runs for job '{job_name}'.")
    return JobRunResponse(**_read_json(files[0]))


def get_job_run_history(job_name: str = "valuations", limit: int = DEFAULT_HISTORY_LIMIT) -> list[JobRunSummary]:
    capped_limit = max(1, min(limit, MAX_HISTORY_LIMIT))
    files = _run_files(job_name)[:capped_limit]
    return [_summarize(_read_json(path)) for path in files]
