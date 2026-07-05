from datetime import datetime, timedelta
from typing import Optional

import jwt
from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jwt import ExpiredSignatureError, PyJWTError

from backend.core.config import ACCOUNT_ID, JWT_EXPIRES_MINUTES, JWT_SECRET

_auth_scheme = HTTPBearer(auto_error=False)
JWT_ALGORITHM = "HS256"
JWT_EXPIRES_SECONDS = JWT_EXPIRES_MINUTES * 60


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
