import os

import pytest
from fastapi import HTTPException

os.environ.setdefault("ACCOUNT_EMAIL", "test@example.com")
os.environ.setdefault("JWT_SECRET", "test-secret")
os.environ.setdefault("DEMO_AUTH_USERNAME", "demo")
os.environ.setdefault("DEMO_AUTH_PASSWORD", "password")

from backend.app.main import LoginRequest, app, login, require_jwt


def test_refactored_routes_are_registered_with_original_paths():
    route_paths = {getattr(route, "path", None) for route in app.routes}

    assert "/health" in route_paths
    assert "/auth/login" in route_paths
    assert "/valuations/latest" in route_paths
    assert "/jobs/{job_name}/latest" in route_paths
    assert "/jobs/{job_name}/history" in route_paths
    assert "/prices/history" in route_paths


def test_login_route_still_issues_bearer_token():
    response = login(LoginRequest(username="demo", password="password"))

    assert response.token_type == "bearer"
    assert response.expires_in == 900
    assert response.account_id
    assert response.access_token


def test_require_jwt_still_rejects_missing_authorization_header():
    with pytest.raises(HTTPException) as exc_info:
        require_jwt(None)

    assert exc_info.value.status_code == 401
    assert exc_info.value.detail == "Authorization header missing."
