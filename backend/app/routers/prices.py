from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from backend.app.prices_history import (
    DEFAULT_WINDOW_DAYS,
    MAX_WINDOW_DAYS,
    PriceHistoryResponse,
    get_price_history,
)
from backend.app.security import require_jwt

router = APIRouter()


@router.get("/prices/history", response_model=PriceHistoryResponse)
def price_history(
    symbol: str = Query(..., description="Asset symbol to load price history for."),
    days: int = Query(
        DEFAULT_WINDOW_DAYS,
        ge=1,
        description=f"Optional lookback window in days (default {DEFAULT_WINDOW_DAYS}, capped at {MAX_WINDOW_DAYS}).",
    ),
    base_currency: Optional[str] = Query(
        None,
        description="Target/base currency for conversions; defaults to the valuation base.",
    ),
    _: dict = Depends(require_jwt),
):
    try:
        return get_price_history(symbol=symbol, days=days, base_currency=base_currency)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:  # pragma: no cover - FastAPI will log the stack trace
        raise HTTPException(status_code=500, detail="Failed to load price history.") from exc
