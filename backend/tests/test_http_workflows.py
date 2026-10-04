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
            headers={"Authorization": f"Bearer {token}"},
        )
        assert response.status_code == 401
        assert response.json()["detail"] == detail


def test_latest_valuation_http_response_reads_snapshot_file(client, monkeypatch, tmp_path):
    valuation_dir = tmp_path / "valuations" / "dt=2026-09-30" / "account=acct-test"
    valuation_dir.mkdir(parents=True)
    (valuation_dir / "valuations_2026-09-30.csv").write_text(
        "snapshot_dt,computed_ts,symbol,quantity,value_base,status,source\n"
        "2026-09-30,2026-09-30T12:00:00Z,BTC,0.5,30000,ok,binance\n"
        "2026-09-30,2026-09-30T12:00:00Z,UNKNOWN,2,,missing_price,manual\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(valuations, "VALUATIONS_DIR", tmp_path / "valuations")
    login = client.post("/auth/login", json={"username": "demo", "password": "secret"}).json()
    response = client.get(
        "/valuations/latest?account_id=acct-test",
        headers={"Authorization": f"Bearer {login['access_token']}"},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["totals"]["total_value_base"] == pytest.approx(30000)
    assert body["totals"]["positions"] == 2
    assert body["rows"][0]["symbol"] == "BTC"
    assert body["rows"][1]["status"] == "missing_price"


def test_job_trigger_http_status_and_conflict(client, monkeypatch):
    login = client.post("/auth/login", json={"username": "demo", "password": "secret"}).json()
    headers = {"Authorization": f"Bearer {login['access_token']}"}
    monkeypatch.setattr(
        jobs_router,
        "start_valuation_job",
        lambda: JobTriggerResponse(job="valuations", run_id="2026-09-30_120000", status="started", started_at="2026-09-30T12:00:00Z"),
    )
    started = client.post("/jobs/valuations/run", headers=headers)
    assert started.status_code == 202
    assert started.json()["status"] == "started"

    def already_running():
        raise JobAlreadyRunning("Valuations job is already running.")

    monkeypatch.setattr(jobs_router, "start_valuation_job", already_running)
    conflict = client.post("/jobs/valuations/run", headers=headers)
    assert conflict.status_code == 409
    assert conflict.json()["detail"] == "Valuations job is already running."
