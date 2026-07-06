import json
from pathlib import Path

import pytest
from fastapi import HTTPException

from backend.app.manual_crypto import (
    ManualCryptoHolding,
    ManualCryptoHoldingsUpdate,
    load_manual_crypto_holdings,
    save_manual_crypto_holdings,
)
from backend.app.routers import manual_crypto as manual_crypto_router


def test_load_manual_crypto_holdings_returns_empty_for_missing_file(tmp_path: Path):
    response = load_manual_crypto_holdings(str(tmp_path / "missing.json"))

    assert response.holdings == []


def test_save_manual_crypto_holdings_normalizes_and_persists_rows(tmp_path: Path):
    holdings_file = tmp_path / "manual" / "crypto_holdings.json"
    update = ManualCryptoHoldingsUpdate(
        holdings=[
            ManualCryptoHolding(
                symbol="btc",
                quantity=0.25,
                display_name=" BTC Wallet ",
                currency="usd",
                market="crypto",
                source="exodus",
            )
        ]
    )

    response = save_manual_crypto_holdings(update, str(holdings_file))

    assert response.holdings[0].symbol == "BTC"
    assert response.holdings[0].currency == "USD"
    assert response.holdings[0].display_name == "BTC Wallet"
    saved_rows = json.loads(holdings_file.read_text(encoding="utf-8"))
    assert saved_rows == [
        {
            "currency": "USD",
            "display_name": "BTC Wallet",
            "market": "crypto",
            "quantity": 0.25,
            "source": "exodus",
            "symbol": "BTC",
        }
    ]


def test_update_manual_crypto_holdings_route_reports_write_errors(monkeypatch):
    def _raise_error(payload):
        raise ValueError("Unable to save manual crypto holdings.")

    monkeypatch.setattr(manual_crypto_router, "save_manual_crypto_holdings", _raise_error)

    with pytest.raises(HTTPException) as exc_info:
        manual_crypto_router.update_manual_crypto_holdings(
            ManualCryptoHoldingsUpdate(holdings=[]),
            {},
        )

    assert exc_info.value.status_code == 500
    assert exc_info.value.detail == "Unable to save manual crypto holdings."
