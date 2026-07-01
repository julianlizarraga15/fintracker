import hmac
from datetime import datetime, timedelta
from typing import Optional

import jwt
from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jwt import ExpiredSignatureError, PyJWTError
from pydantic import BaseModel

from backend.app.prices_history import (
    DEFAULT_WINDOW_DAYS,
    MAX_WINDOW_DAYS,
    PriceHistoryResponse,
    get_price_history,
)
from backend.app.job_runs import (
    DEFAULT_HISTORY_LIMIT,
    JobRunNotFound,
    JobRunResponse,
    JobRunSummary,
    get_job_run_history,
    get_latest_job_run,
)
from backend.app.valuations import (
    LatestValuationResponse,
    SnapshotNotFound,
    get_latest_valuation_snapshot,
)
from backend.core.config import (
    ACCOUNT_ID,
    DEMO_AUTH_PASSWORD,
    DEMO_AUTH_USERNAME,
    JWT_EXPIRES_MINUTES,
    JWT_SECRET,
)

app = FastAPI(title="Fintracker API")
_auth_scheme = HTTPBearer(auto_error=False)
JWT_ALGORITHM = "HS256"
JWT_EXPIRES_SECONDS = JWT_EXPIRES_MINUTES * 60


class LoginRequest(BaseModel):
    username: str
    password: str


class LoginResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int
    account_id: Optional[str] = None


def _ensure_auth_configured() -> None:
    if not DEMO_AUTH_USERNAME or not DEMO_AUTH_PASSWORD:
        raise HTTPException(status_code=500, detail="Demo credentials are not configured.")
    if not JWT_SECRET:
        raise HTTPException(status_code=500, detail="JWT secret is not configured.")


def _issue_token(subject: str) -> str:
    expiration = datetime.utcnow() + timedelta(minutes=JWT_EXPIRES_MINUTES)
    payload = {"sub": subject, "exp": expiration, "iat": datetime.utcnow()}
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def _detect_account_id() -> Optional[str]:
    return ACCOUNT_ID


def require_jwt(credentials: HTTPAuthorizationCredentials = Depends(_auth_scheme)) -> dict:
    if not JWT_SECRET:
        raise HTTPException(status_code=500, detail="JWT secret is not configured.")
    if credentials is None:
        raise HTTPException(status_code=401, detail="Authorization header missing.")
    token = credentials.credentials
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except ExpiredSignatureError as exc:
        raise HTTPException(status_code=401, detail="Token expired.") from exc
    except PyJWTError as exc:
        raise HTTPException(status_code=401, detail="Invalid token.") from exc


@app.get("/health")
def health():
    return {"ok": True}


@app.post("/auth/login", response_model=LoginResponse)
def login(payload: LoginRequest):
    _ensure_auth_configured()
    username_valid = hmac.compare_digest(payload.username, DEMO_AUTH_USERNAME)
    password_valid = hmac.compare_digest(payload.password, DEMO_AUTH_PASSWORD)
    if not (username_valid and password_valid):
        raise HTTPException(status_code=401, detail="Invalid credentials.")
    return LoginResponse(
        access_token=_issue_token(payload.username),
        expires_in=JWT_EXPIRES_SECONDS,
        account_id=_detect_account_id(),
    )


@app.get("/valuations/latest", response_model=LatestValuationResponse)
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


@app.get("/jobs/{job_name}/latest", response_model=JobRunResponse)
def latest_job_run(job_name: str, _: dict = Depends(require_jwt)):
    try:
        return get_latest_job_run(job_name=job_name)
    except JobRunNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:  # pragma: no cover - FastAPI will log the stack trace
        raise HTTPException(status_code=500, detail="Failed to load job run.") from exc


@app.get("/jobs/{job_name}/history", response_model=list[JobRunSummary])
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


@app.get("/prices/history", response_model=PriceHistoryResponse)
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
