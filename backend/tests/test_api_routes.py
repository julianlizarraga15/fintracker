import os

import pytest
from fastapi import HTTPException

os.environ.setdefault("ACCOUNT_EMAIL", "test@example.com")
os.environ.setdefault("JWT_SECRET", "test-secret")
os.environ.setdefault("DEMO_AUTH_USERNAME", "demo")
os.environ.setdefault("DEMO_AUTH_PASSWORD", "password")

from backend.app.main import LoginRequest, app, login, require_jwt
from backend.app.job_trigger import JobAlreadyRunning, JobTriggerResponse
from backend.app.routers import jobs as jobs_router


def test_refactored_routes_are_registered_with_original_paths():
    route_paths = {getattr(route, "path", None) for route in app.routes}

    assert "/health" in route_paths
    assert "/auth/login" in route_paths
    assert "/valuations/latest" in route_paths
    assert "/jobs/valuations/run" in route_paths
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


def test_trigger_valuation_job_returns_started_response(monkeypatch):
    expected = JobTriggerResponse(
        job="valuations",
        run_id="2026-07-06_120000",
        status="started",
        started_at="2026-07-06T12:00:00Z",
    )
    monkeypatch.setattr(jobs_router, "start_valuation_job", lambda: expected)

    response = jobs_router.trigger_valuation_job({})

    assert response == expected


def test_trigger_valuation_job_returns_conflict_when_running(monkeypatch):
    def _raise_already_running():
        raise JobAlreadyRunning("Valuations job is already running.")

    monkeypatch.setattr(jobs_router, "start_valuation_job", _raise_already_running)

    with pytest.raises(HTTPException) as exc_info:
        jobs_router.trigger_valuation_job({})

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail == "Valuations job is already running."
