import hmac
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from backend.app.security import _detect_account_id, _issue_token, JWT_EXPIRES_SECONDS
from backend.core.config import DEMO_AUTH_PASSWORD, DEMO_AUTH_USERNAME, JWT_SECRET

router = APIRouter()


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


@router.post("/auth/login", response_model=LoginResponse)
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
