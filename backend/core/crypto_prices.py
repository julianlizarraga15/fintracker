"""Fetch crypto prices from CoinGecko."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Dict, List

import requests

COINGECKO_SIMPLE_PRICE_URL = "https://api.coingecko.com/api/v3/simple/price"
SUPPORTED_SYMBOL_TO_ID: Dict[str, str] = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
}
DEFAULT_PRICE_CCY = "USD"
QUALITY_SCORE_COINGECKO = 85
DEFAULT_VENUE = "COINGECKO"


def fetch_simple_prices(symbols: List[str]) -> List[dict]:
    """Return price rows for supported symbols via CoinGecko simple price."""

    normalized: Dict[str, str] = {}
    for symbol in symbols:
        if not symbol:
            continue
        sym = str(symbol).strip().upper()
        coin_id = SUPPORTED_SYMBOL_TO_ID.get(sym)
        if coin_id:
            normalized[sym] = coin_id

    if not normalized:
        return []

    try:
        resp = requests.get(
            COINGECKO_SIMPLE_PRICE_URL,
            params={
                "ids": ",".join(set(normalized.values())),
                "vs_currencies": DEFAULT_PRICE_CCY.lower(),
            },
            timeout=10,
        )
    except requests.RequestException as exc:
        print(f"[warn] Error calling CoinGecko: {exc}")
        return []

    if resp.status_code != 200:
        print(f"[warn] CoinGecko response {resp.status_code}: {resp.text[:200]}")
        return []

    try:
        payload = resp.json()
    except ValueError as exc:
        print(f"[warn] Invalid JSON from CoinGecko: {exc}")
        return []

    asof_dt = date.today()
    asof_ts = datetime.now(timezone.utc)
    rows: List[dict] = []

    for symbol, coin_id in normalized.items():
        price_info = payload.get(coin_id) or {}
        price = price_info.get(DEFAULT_PRICE_CCY.lower())
        if price is None:
            continue
        rows.append(
            {
                "symbol": symbol,
                "price": price,
                "currency": DEFAULT_PRICE_CCY,
                "venue": DEFAULT_VENUE,
                "source": "coingecko_simple",
                "quality_score": QUALITY_SCORE_COINGECKO,
                "asof_dt": asof_dt,
                "asof_ts": asof_ts,
            }
        )

    return rows
