from __future__ import annotations

import pytest

from backend.core import binance_prices
from backend.core.binance_common import QUALITY_SCORE_BINANCE


class DummyResponse:
    def __init__(self, status_code: int, json_data, text: str = ""):
        self.status_code = status_code
        self._json = json_data
        self.text = text or ""

    def json(self):
        return self._json


def test_fetch_binance_prices_parses_tickers(monkeypatch):
    calls = []

    def fake_get(url, params=None, headers=None, timeout=None):
        calls.append(params["symbol"])
        if params["symbol"] == "BTCUSDT":
            return DummyResponse(200, {"price": "50000"})
        if params["symbol"] == "ETHUSDT":
            return DummyResponse(200, {"price": 3000})
        return DummyResponse(404, {})

    monkeypatch.setattr(binance_prices.requests, "get", fake_get)

    prices, missing = binance_prices.fetch_binance_prices(
        ["btc", "eth", "usdt"],
        allow_coingecko_fallback=False,
    )

    assert len(prices) == 3
    assert missing == []
    btc = next(p for p in prices if p.symbol == "BTC")
    assert btc.price == pytest.approx(50000)
    assert btc.currency == "USDT"
    assert btc.quality_score == QUALITY_SCORE_BINANCE
    assert all(call != "USDTUSDT" for call in calls)


def test_fetch_binance_prices_returns_missing_list(monkeypatch):
    def fake_get(url, params=None, headers=None, timeout=None):
        return DummyResponse(404, {})

    monkeypatch.setattr(binance_prices.requests, "get", fake_get)

    prices, missing = binance_prices.fetch_binance_prices(["unknown"], allow_coingecko_fallback=False)

    assert prices == []
    assert missing == ["UNKNOWN"]
