from fastapi import APIRouter, Depends, HTTPException, Query

from backend.app.job_trigger import (
    JobAlreadyRunning,
    JobStartError,
    JobTriggerResponse,
    start_valuation_job,
)
from backend.app.job_runs import (
    DEFAULT_HISTORY_LIMIT,
    JobRunNotFound,
    JobRunResponse,
    JobRunSummary,
    get_job_run_history,
    get_latest_job_run,
)
from backend.app.security import require_jwt

router = APIRouter()


@router.post("/jobs/valuations/run", response_model=JobTriggerResponse, status_code=202)
def trigger_valuation_job(_: dict = Depends(require_jwt)):
    try:
        return start_valuation_job()
    except JobAlreadyRunning as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except JobStartError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/jobs/{job_name}/latest", response_model=JobRunResponse)
def latest_job_run(job_name: str, _: dict = Depends(require_jwt)):
    try:
        return get_latest_job_run(job_name=job_name)
    except JobRunNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:  # pragma: no cover - FastAPI will log the stack trace
        raise HTTPException(status_code=500, detail="Failed to load job run.") from exc


@router.get("/jobs/{job_name}/history", response_model=list[JobRunSummary])
def job_run_history(
    job_name: str,
    limit: int = Query(
        DEFAULT_HISTORY_LIMIT,
        ge=1,
        description=f"History depth to return (default {DEFAULT_HISTORY_LIMIT}).",
    ),
    _: dict = Depends(require_jwt),
):
    try:
        return get_job_run_history(job_name=job_name, limit=limit)
    except JobRunNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:  # pragma: no cover - FastAPI will log the stack trace
        raise HTTPException(status_code=500, detail="Failed to load job run history.") from exc
