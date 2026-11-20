from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import jwt
import pytest
from fastapi.testclient import TestClient

from backend.app import valuations
from backend.app.main import app
from backend.app.job_trigger import JobAlreadyRunning, JobTriggerResponse
from backend.app.routers import jobs as jobs_router
from backend.core import config

EXPIRED_TOKEN_AGE_MINUTES = 1


@pytest.fixture
def client(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "DEMO_AUTH_USERNAME", "demo")
    monkeypatch.setattr(config, "DEMO_AUTH_PASSWORD", "secret")
    monkeypatch.setattr(config, "JWT_SECRET", "test-signing-secret")
    monkeypatch.setattr(config, "JWT_EXPIRES_MINUTES", 15)
    monkeypatch.setattr(config, "ACCOUNT_ID", "acct-test")
    monkeypatch.setattr(config, "CRYPTO_HOLDINGS_FILE", str(tmp_path / "crypto.json"))
    return TestClient(app)


def test_login_and_manual_holdings_http_workflow(client):
    rejected = client.post("/auth/login", json={"username": "demo", "password": "wrong"})
    assert rejected.status_code == 401

    login = client.post("/auth/login", json={"username": "demo", "password": "secret"})
    assert login.status_code == 200
    token = login.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    assert client.get("/manual/crypto-holdings").status_code == 401
    assert client.get("/manual/crypto-holdings", headers=headers).json() == {"holdings": []}

    saved = client.put(
        "/manual/crypto-holdings",
        headers=headers,
        json={"holdings": [{"symbol": " btc ", "quantity": 0.25, "display_name": " BTC Wallet ", "currency": "usd"}]},
    )
    assert saved.status_code == 200
    assert saved.json()["holdings"][0]["symbol"] == "BTC"
    assert saved.json()["holdings"][0]["display_name"] == "BTC Wallet"

    persisted = json.loads(Path(config.CRYPTO_HOLDINGS_FILE).read_text(encoding="utf-8"))
    assert persisted[0]["quantity"] == 0.25
    assert client.get("/manual/crypto-holdings", headers=headers).json() == saved.json()

    invalid = client.put(
        "/manual/crypto-holdings",
        headers=headers,
        json={"holdings": [{"symbol": "BTC", "quantity": 0}]},
    )
    assert invalid.status_code == 422
    assert client.get("/manual/crypto-holdings", headers=headers).json() == saved.json()


def test_login_rejects_rate_limited_client(client, monkeypatch):
    from backend.app.routers import auth as auth_router

    monkeypatch.setattr(auth_router, "allow_login_attempt", lambda client_key: False)

    response = client.post("/auth/login", json={"username": "demo", "password": "secret"})

    assert response.status_code == 429
    assert response.json()["detail"] == "Too many login attempts."


def test_login_rate_limit_expires_after_its_window(monkeypatch):
    from backend.app import login_rate_limit

    now = [100.0]
    monkeypatch.setattr(login_rate_limit, "monotonic", lambda: now[0])
    client_key = "rate-limit-window-test"

    for _ in range(login_rate_limit.MAX_LOGIN_ATTEMPTS):
        assert login_rate_limit.allow_login_attempt(client_key)
    assert not login_rate_limit.allow_login_attempt(client_key)

    now[0] += login_rate_limit.ATTEMPT_WINDOW_SECONDS
    assert login_rate_limit.allow_login_attempt(client_key)


def test_health_and_protected_api_routes_are_reachable(client):
    assert client.get("/health").json() == {"ok": True}
    for path in (
        "/prices/history?symbol=BTC",
        "/jobs/valuations/latest",
        "/jobs/valuations/history",
    ):
        assert client.get(path).status_code == 401


def test_manual_holdings_rejects_invalid_and_expired_tokens(client):
    expired_token = jwt.encode(
        {"sub": "demo", "exp": datetime.now(timezone.utc) - timedelta(minutes=EXPIRED_TOKEN_AGE_MINUTES)},
        config.JWT_SECRET,
        algorithm="HS256",
    )
    for token, detail in (("not-a-jwt", "Invalid token."), (expired_token, "Token expired.")):
        response = client.get(
            "/manual/crypto-holdings",
            headers=