from datetime import datetime, timedelta
from typing import Optional

import jwt
from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jwt import ExpiredSignatureError, PyJWTError

from backend.core import config
from backend.core.config import Settings

_auth_scheme = HTTPBearer(auto_error=False)
JWT_ALGORITHM = "HS256"
JWT_EXPIRES_SECONDS = config.JWT_EXPIRES_MINUTES * 60


def _jwt_secret(settings: Settings | None = None) -> str | None:
    return settings.JWT_SECRET if settings is not None else config.JWT_SECRET


def _jwt_expires_minutes(settings: Settings | None = None) -> int:
    return settings.JWT_EXPIRES_MINUTES if settings is not None else config.JWT_EXPIRES_MINUTES


def _jwt_expires_seconds(settings: Settings | None = None) -> int:
    return _jwt_expires_minutes(settings) * 60


def _issue_token(subject: str, settings: Settings | None = None) -> str:
    jwt_secret = _jwt_secret(settings)
    expiration = datetime.utcnow() + timedelta(minutes=_jwt_expires_minutes(settings))
    payload = {"sub": subject, "exp": expiration, "iat": datetime.utcnow()}
    return jwt.encode(payload, jwt_secret, algorithm=JWT_ALGORITHM)


def _detect_account_id(settings: Settings | None = None) -> Optional[str]:
    return settings.ACCOUNT_ID if settings is not None else config.ACCOUNT_ID


def require_jwt(credentials: HTTPAuthorizationCredentials = Depends(_auth_scheme)) -> dict:
    jwt_secret = _jwt_secret()
    if not jwt_secret:
        raise HTTPException(status_code=500, detail="JWT secret is not configured.")
    if credentials is None:
        raise HTTPException(status_code=401, detail="Authorization header missing.")
    token = credentials.credentials
    try:
        return jwt.decode(token, jwt_secret, algorithms=[JWT_ALGORITHM])
    except ExpiredSignatureError as exc:
        raise HTTPException(status_code=401, detail="Token expired.") from exc
    except PyJWTError as exc:
        raise HTTPException(status_code=401, detail="Invalid token.") from exc
