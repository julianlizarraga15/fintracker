import hmac
import os
from datetime import datetime, timedelta

import jwt
from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jwt import ExpiredSignatureError, PyJWTError
from pydantic import BaseModel

from backend.app.valuations import (
    LatestValuationResponse,
    SnapshotNotFound,
    get_latest_valuation_snapshot,
)

app = FastAPI(title="Fintracker API")
_auth_scheme = HTTPBearer(auto_error=False)
DEFAULT_JWT_EXPIRES_MINUTES = 15
JWT_ALGORITHM = "HS256"

JWT_SECRET = os.getenv("JWT_SECRET")
DEMO_AUTH_USERNAME = os.getenv("DEMO_AUTH_USERNAME")
DEMO_AUTH_PASSWORD = os.getenv("DEMO_AUTH_PASSWORD")
try:
    JWT_EXPIRES_MINUTES = int(os.getenv("JWT_EXPIRES_MINUTES", str(DEFAULT_JWT_EXPIRES_MINUTES)))
except ValueError:
    JWT_EXPIRES_MINUTES = DEFAULT_JWT_EXPIRES_MINUTES
if JWT_EXPIRES_MINUTES <= 0:
    JWT_EXPIRES_MINUTES = DEFAULT_JWT_EXPIRES_MINUTES
JWT_EXPIRES_SECONDS = JWT_EXPIRES_MINUTES * 60


class LoginRequest(BaseModel):
    username: str
    password: str


class LoginResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int


def _ensure_auth_configured() -> None:
    if not DEMO_AUTH_USERNAME or not DEMO_AUTH_PASSWORD:
        raise HTTPException(status_code=500, detail="Demo credentials are not configured.")
    if not JWT_SECRET:
        raise HTTPException(status_code=500, detail="JWT secret is not configured.")


def _issue_token(subject: str) -> str:
    expiration = datetime.utcnow() + timedelta(minutes=JWT_EXPIRES_MINUTES)
    payload = {"sub": subject, "exp": expiration, "iat": datetime.utcnow()}
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


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
    return LoginResponse(access_token=_issue_token(payload.username), expires_in=JWT_EXPIRES_SECONDS)


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
