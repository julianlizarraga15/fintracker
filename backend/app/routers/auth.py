import hmac
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from backend.app.security import _detect_account_id, _issue_token, _jwt_expires_seconds
from backend.core import config
from backend.core.config import Settings

router = APIRouter()


class LoginRequest(BaseModel):
    username: str
    password: str


class LoginResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int
    account_id: Optional[str] = None


def _demo_auth_username(settings: Settings | None = None) -> str | None:
    return settings.DEMO_AUTH_USERNAME if settings is not None else config.DEMO_AUTH_USERNAME


def _demo_auth_password(settings: Settings | None = None) -> str | None:
    return settings.DEMO_AUTH_PASSWORD if settings is not None else config.DEMO_AUTH_PASSWORD


def _jwt_secret(settings: Settings | None = None) -> str | None:
    return settings.JWT_SECRET if settings is not None else config.JWT_SECRET


def _ensure_auth_configured(settings: Settings | None = None) -> None:
    if not _demo_auth_username(settings) or not _demo_auth_password(settings):
        raise HTTPException(status_code=500, detail="Demo credentials are not configured.")
    if not _jwt_secret(settings):
        raise HTTPException(status_code=500, detail="JWT secret is not configured.")


@router.post("/auth/login", response_model=LoginResponse)
def login(payload: LoginRequest):
    _ensure_auth_configured()
    username = _demo_auth_username()
    password = _demo_auth_password()
    username_valid = hmac.compare_digest(payload.username, username or "")
    password_valid = hmac.compare_digest(payload.password, password or "")
    if not (username_valid and password_valid):
        raise HTTPException(status_code=401, detail="Invalid credentials.")
    return LoginResponse(
        access_token=_issue_token(payload.username),
        expires_in=_jwt_expires_seconds(),
        account_id=_detect_account_id(),
    )
