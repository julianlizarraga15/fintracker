from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Iterable, List, Optional, Tuple

import requests

from backend.core.binance_common import (
    BINANCE_PRICE_SOURCE,
    BINANCE_PRICE_VENUE,
    BINANCE_STABLECOINS,
    QUALITY_SCORE_BINANCE,
)
from backend.core.config import BINANCE_BASE_URL, DEFAULT_BINANCE_BASE_URL
from backend.core.crypto_prices import fetch_simple_prices
from backend.valuation.models import Price

LOG = logging.getLogger(__name__)

BASE_URL = BINANCE_BASE_URL or DEFAULT_BINANCE_BASE_URL
DEFAULT_TIMEOUT = 10
TICKER_ENDPOINT = "/api/v3/ticker/price"


def _ticker_price(pair_symbol: str, base_url: str, timeout: int) -> Optional[Tuple[float, str]]:
    url = f"{base_url.rstrip('/')}{TICKER_ENDPOINT}"
    try:
        resp = requests.get(url, params={"symbol": pair_symbol}, timeout=timeout)
    except requests.RequestException as exc:
        LOG.debug("Binance ticker request error for %s: %s", pair_symbol, exc)
        return None

    if resp.status_code != 200:
        LOG.debug("Binance ticker miss %s status=%s body=%s", pair_symbol, resp.status_code, resp.text[:160])
        return None

    try:
        payload = resp.json()
        price_value = float(payload.get("price"))
    except (ValueError, TypeError):
        return None

    if price_value <= 0:
        return None

    currency = "USDT" if pair_symbol.endswith("USDT") else "USD"
    return price_value, currency


def _build_price(symbol: str, price_value: float, currency: str, asof_dt: date, asof_ts: datetime) -> Price:
    return Price(
        asof_dt=asof_dt,
        asof_ts=asof_ts,
        symbol=symbol,
        price_type="last",
        price=price_value,
        currency=currency,
        venue=BINANCE_PRICE_VENUE,
        source=BINANCE_PRICE_SOURCE,
        quality_score=QUALITY_SCORE_BINANCE,
    )


def _coingecko_fallback(symbols: List[str]) -> List[Price]:
    rows = fetch_simple_prices(symbols)
    prices: List[Price] = []
    for row in rows:
        try:
            prices.append(
                Price(
                    asof_dt=row["asof_dt"],
                    asof_ts=row.get("asof_ts"),
                    symbol=row["symbol"],
                    price_type="last",
                    price=row["price"],
                    currency=row["currency"],
                    venue=row["venue"],
                    source=row["source"],
                    quality_score=row["quality_score"],
                )
            )
        except Exception:
            continue
    return prices


def fetch_binance_prices(
    symbols: Iterable[str],
    base_url: str = BASE_URL,
    timeout: int = DEFAULT_TIMEOUT,
    allow_coingecko_fallback: bool = True,
) -> Tuple[List[Price], List[str]]:
    asof_dt = date.today()
    asof_ts = datetime.now(timezone.utc)
    prices: List[Price] = []
    missing: List[str] = []

    seen = set()
    for raw_symbol in symbols or []:
        if not raw_symbol:
            continue
        symbol = str(raw_symbol).strip().upper()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)

        if symbol in BINANCE_STABLECOINS:
            prices.append(_build_price(symbol, 1.0, "USDT", asof_dt, asof_ts))
            continue

        pair = f"{symbol}USDT"
        res = _ticker_price(pair, base_url, timeout)
        currency = "USDT"
        if res is None:
            pair = f"{symbol}USD"
            res = _ticker_price(pair, base_url, timeout)
            currency = "USD"

        if res is None:
            missing.append(symbol)
            continue

        price_value, detected_currency = res
        prices.append(_build_price(symbol, price_value, detected_currency or currency, asof_dt, asof_ts))

    if allow_coingecko_fallback and missing:
        fallback_candidates = [s for s in missing if s in ("BTC", "ETH")]
        if fallback_candidates:
            fallback_prices = _coingecko_fallback(fallback_candidates)
            prices.extend(fallback_prices)
            if fallback_prices:
                priced_symbols = {p.symbol for p in fallback_prices}
                missing = [sym for sym in missing if sym not in priced_symbols]

    return prices, missing
