from __future__ import annotations

import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from pydantic import BaseModel

from backend.app import job_runs as job_runs_module


VALUATIONS_JOB_NAME = "valuations"
LOCK_DIR_NAME = ".running.lock"
LOCK_START_GRACE_SECONDS = 30
RUN_ID_FORMAT = "%Y-%m-%d_%H%M%S"
UTC_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
PROCESS_EXISTS_SIGNAL = 0
LOCK_FILE_NAMES = ("pid", "run_id", "started_at")


class JobAlreadyRunning(RuntimeError):
    """Raised when a valuation run is already active."""


class JobStartError(RuntimeError):
    """Raised when the valuation job could not be started."""


class JobTriggerResponse(BaseModel):
    job: str
    run_id: str
    status: str
    started_at: str


def app_root() -> Path:
    return Path(__file__).resolve().parents[2]


def valuation_job_script() -> Path:
    return app_root() / "scripts" / "run_valuations.sh"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _format_utc(value: datetime) -> str:
    return value.strftime(UTC_TIMESTAMP_FORMAT)


def _job_lock_dir(job_name: str = VALUATIONS_JOB_NAME) -> Path:
    return job_runs_module._job_dir(job_name) / LOCK_DIR_NAME


def _read_pid(lock_dir: Path) -> int | None:
    try:
        return int((lock_dir / "pid").read_text(encoding="utf-8").strip())
    except (FileNotFoundError, ValueError):
        return None


def _process_exists(pid: int) -> bool:
    try:
        os.kill(pid, PROCESS_EXISTS_SIGNAL)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def release_job_lock(lock_dir: Path) -> None:
    for name in LOCK_FILE_NAMES:
        try:
            (lock_dir / name).unlink()
        except FileNotFoundError:
            pass
    try:
        lock_dir.rmdir()
    except OSError:
        pass


def is_job_running(job_name: str = VALUATIONS_JOB_NAME) -> bool:
    lock_dir = _job_lock_dir(job_name)
    if not lock_dir.exists():
        return False

    pid = _read_pid(lock_dir)
    if pid is not None:
        if _process_exists(pid):
            return True
        release_job_lock(lock_dir)
        return False

    try:
        lock_age_seconds = time.time() - lock_dir.stat().st_mtime
    except FileNotFoundError:
        return False
    if lock_age_seconds <= LOCK_START_GRACE_SECONDS:
        return True

    release_job_lock(lock_dir)
    return False


def _write_lock_metadata(lock_dir: Path, *, run_id: str, started_at: str, pid: int) -> None:
    (lock_dir / "run_id").write_text(f"{run_id}\n", encoding="utf-8")
    (lock_dir / "started_at").write_text(f"{started_at}\n", encoding="utf-8")
    (lock_dir / "pid").write_text(f"{pid}\n", encoding="utf-8")


def _acquire_job_lock(*, run_id: str, started_at: str) -> Path:
    lock_dir = _job_lock_dir()
    lock_dir.parent.mkdir(parents=True, exist_ok=True)
    try:
        lock_dir.mkdir()
    except FileExistsError as exc:
        if is_job_running():
            raise JobAlreadyRunning("Valuations job is already running.") from exc
        release_job_lock(lock_dir)
        try:
            lock_dir.mkdir()
        except FileExistsError as retry_exc:
            raise JobAlreadyRunning("Valuations job is already running.") from retry_exc

    _write_lock_metadata(lock_dir, run_id=run_id, started_at=started_at, pid=os.getpid())
    return lock_dir


def start_valuation_job() -> JobTriggerResponse:
    now = _utc_now()
    run_id = now.strftime(RUN_ID_FORMAT)
    started_at = _format_utc(now)
    lock_dir = _acquire_job_lock(run_id=run_id, started_at=started_at)

    env = os.environ.copy()
    env["RUN_STAMP"] = run_id
    env["FINTRACKER_SKIP_JOB_LOCK"] = "1"

    command = [
        sys.executable,
        "-m",
        "backend.app.run_valuations_job",
        "--run-id",
        run_id,
        "--lock-dir",
        str(lock_dir),
    ]

    try:
        process = subprocess.Popen(
            command,
            cwd=str(app_root()),
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
    except OSError as exc:
        release_job_lock(lock_dir)
        raise JobStartError("Failed to start valuations job.") from exc

    _write_lock_metadata(lock_dir, run_id=run_id, started_at=started_at, pid=process.pid)
    return JobTriggerResponse(job=VALUATIONS_JOB_NAME, run_id=run_id, status="started", started_at=started_at)
