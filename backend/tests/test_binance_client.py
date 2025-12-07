from __future__ import annotations

import pytest

from backend.core import binance_client


class DummyResponse:
    def __init__(self, status_code: int, json_data, text: str = ""):
        self.status_code = status_code
        self._json = json_data
        self.text = text or ""

    def json(self):
        return self._json


def test_get_account_balances_sums_and_filters(monkeypatch):
    captured = {}

    def fake_get(url, params=None, headers=None, timeout=None):
        captured["params"] = params
        return DummyResponse(
            200,
            {
                "balances": [
                    {"asset": "BTC", "free": "1.0", "locked": "0.5"},
                    {"asset": "ZERO", "free": "0", "locked": "0"},
                    {"asset": "USDT", "free": 5, "locked": "-1"},
                ]
            },
        )

    monkeypatch.setattr(binance_client.requests, "get", fake_get)

    balances = binance_client.get_account_balances(
        api_key="k",
        api_secret="s",
        base_url="https://api.test",
        recv_window_ms=1234,
    )

    assert captured["params"]["recvWindow"] == 1234
    assert len(balances) == 2
    btc = next(b for b in balances if b["asset"] == "BTC")
    assert btc["quantity"] == pytest.approx(1.5)
    assert all(b["asset"] != "ZERO" for b in balances)


def test_get_account_balances_auth_error(monkeypatch):
    def fake_get(url, params=None, headers=None, timeout=None):
        return DummyResponse(401, {"msg": "bad auth"})

    monkeypatch.setattr(binance_client.requests, "get", fake_get)

    with pytest.raises(binance_client.BinanceAPIError):
        binance_client.get_account_balances(api_key="k", api_secret="s", base_url="https://api.test")


def test_get_account_balances_rate_limit(monkeypatch):
    def fake_get(url, params=None, headers=None, timeout=None):
        return DummyResponse(429, {"msg": "slow down"})

    monkeypatch.setattr(binance_client.requests, "get", fake_get)

    with pytest.raises(binance_client.BinanceAPIError):
        binance_client.get_account_balances(api_key="k", api_secret="s", base_url="https://api.test")
