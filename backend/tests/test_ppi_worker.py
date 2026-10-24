from __future__ import annotations

from typing import Any

import pytest

from backend.core import ppi_client
from backend.workers import ppi as ppi_worker


PUBLIC_KEY = "synthetic-public-key"
PRIVATE_KEY = "synthetic-private-key"
SESSION_TOKEN = "synthetic-session-token"


class SyntheticPPI:
    def __init__(self, sandbox: bool):
        self.sandbox = sandbox
        self.logged_in_with: tuple[str, str] | None = None
        self.account = self

    def login_api(self, public_key: str, private_key: str) -> None:
        self.logged_in_with = (public_key, private_key)
        self.session_token = SESSION_TOKEN

    def get_accounts(self) -> list[dict[str, str]]:
        return [{"accountNumber": "synthetic-account"}]

    def get_balance_and_positions(self, _account_number: str) -> dict[str, Any]:
        return {
            "groupedAvailability": [
                {
                    "currency": "ARS",
                    "availability": [
                        {
                            "name": "Pesos",
                            "symbol": "ARS",
                            "amount": 250,
                            "settlement": "INMEDIATA",
                        }
                    ],
                }
            ],
            "groupedInstruments": [
                {
                    "name": "CEDEARS",
                    "instruments": [
                        {
                            "ticker": "AAPL",
                            "description": "Apple",
                            "quantity": 2,
                            "price": 5000,
                            "amount": 10000,
                            "currency": "PESOS",
                        }
                    ],
                }
            ],
        }


@pytest.fixture
def synthetic_worker(monkeypatch):
    monkeypatch.setenv("ACCOUNT_ID", "acct-ppi")
    monkeypatch.setenv("PPI_PUBLIC_KEY_PARAMETER", "/workers/ppi/public")
    monkeypatch.setenv("PPI_PRIVATE_KEY_PARAMETER", "/workers/ppi/private")
    monkeypatch.delenv("PPI_ACCOUNT_NUMBER", raising=False)
    monkeypatch.setattr(
        ppi_worker,
        "load_parameters",
        lambda names: {"public_key": PUBLIC_KEY, "private_key": PRIVATE_KEY},
    )
    sdk = SyntheticPPI(sandbox=False)

    def build_synthetic_ppi(*, sandbox: bool = False):
        sdk.sandbox = sandbox
        return sdk

    monkeypatch.setattr(ppi_client, "_build_ppi", build_synthetic_ppi)
    return sdk


def test_ppi_worker_returns_only_normalized_positions_and_prices(synthetic_worker):
    response = ppi_worker.handler({}, None)

    assert response["status"] == "ok"
    assert response["source"] == "ppi"
    assert len(response["positions"]) == 2
    cash = next(row for row in response["positions"] if row["symbol"] == "CASH_ARS_INMEDIATA")
    stock = next(row for row in response["positions"] if row["symbol"] == "AAPL")
    assert cash["account_id"] == "acct-ppi"
    assert cash["quantity"] == 250
    assert stock["account_id"] == "acct-ppi"
    assert stock["instrument_type"] == "cedear"
    assert {price["symbol"] for price in response["prices"]} == {
        "CASH_ARS_INMEDIATA",
        "AAPL",
    }
    assert synthetic_worker.logged_in_with == (PUBLIC_KEY, PRIVATE_KEY)
    assert synthetic_worker.sandbox is False
    assert all(
        secret not in str(response)
        for secret in (PUBLIC_KEY, PRIVATE_KEY, SESSION_TOKEN, "groupedAvailability")
    )


def test_ppi_worker_sanitizes_sdk_failure(synthetic_worker, monkeypatch, capsys):
    def fail_with_sensitive_details(_account_number: str):
        raise RuntimeError(f"authorization failed: {PRIVATE_KEY}; token={SESSION_TOKEN}")

    monkeypatch.setattr(
        synthetic_worker, "get_balance_and_positions", fail_with_sensitive_details
    )

    response = ppi_worker.handler({}, None)
    captured = capsys.readouterr()

    assert response["status"] == "error"
    assert response["error_code"] == "source_unavailable"
    assert all(secret not in str(response) for secret in (PRIVATE_KEY, SESSION_TOKEN))
    assert all(secret not in captured.out + captured.err for secret in (PRIVATE_KEY, SESSION_TOKEN))
