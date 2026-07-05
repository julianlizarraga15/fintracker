from fastapi import APIRouter, Depends, HTTPException, Query

from backend.app.security import require_jwt
from backend.app.valuations import (
    LatestValuationResponse,
    SnapshotNotFound,
    get_latest_valuation_snapshot,
)

router = APIRouter()


@router.get("/valuations/latest", response_model=LatestValuationResponse)
def latest_valuations(
    account_id: str = Query(
        description="Account partition id as seen in data/positions/valuations/dt=*/account=…",
    ),
    _: dict = Depends(require_jwt),
):
    try:
        return get_latest_valuation_snapshot(account_id=account_id)
    except SnapshotNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:  # pragma: no cover - FastAPI will log the stack trace
        raise HTTPException(status_code=500, detail="Failed to load valuation snapshot.") from exc
