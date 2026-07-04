from __future__ import annotations

from datetime import date, datetime, timezone
from math import nan
from typing import Any, Iterable, Optional

import pandas as pd

from backend.core.config import ACCOUNT_ID
from backend.valuation.models import Price

PPI_SOURCE = "ppi"
PPI_MARKET = "ppi"
PPI_PRICE_SOURCE = "ppi_api"
PPI_PRICE_QUALITY_SCORE = 95
PPI_CASH_PRICE_SOURCE = "ppi_cash"

CASH_SYMBOL_BY_CURRENCY = {
    "ARS": "CASH_ARS",
    "USD": "CASH_USD",
}

INSTRUMENT_TYPE_MAP = {
    "ACCIONES": "equity",
    "ACCIONES-USA": "equity",
    "BONOS": "bond",
    "CEDEARS": "cedear",
    "ETF": "etf",
    "FCI": "fci",
    "FCI-EXTERIOR": "fci",
}


def _safe_float(value: Any) -> Optional[float]:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return None if pd.isna(result) else result


def _clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _upper_text(value: Any) -> Optional[str]:
    cleaned = _clean_text(value)
    return cleaned.upper() if cleaned else None


def _currency_code(value: Any) -> Optional[str]:
    raw_value = _upper_text(value)
    if not raw_value:
        return None
    if raw_value in ("ARS", "PESO", "PESOS"):
        return "ARS"
    if raw_value.startswith("DOLAR") or raw_value.startswith("DOLARES") or raw_value.startswith("USD"):
        return "USD"
    return raw_value


def _asset_type(raw_type: Any) -> str:
    key = (_upper_text(raw_type) or "").replace(" ", "-")
    return INSTRUMENT_TYPE_MAP.get(key, "other")


def _cash_symbol(currency: str, settlement: Optional[str]) -> str:
    base = CASH_SYMBOL_BY_CURRENCY.get(currency, f"CASH_{currency}")
    if not settlement:
        return base
    settlement_key = str(settlement).strip().upper().replace(" ", "_").replace("-", "_")
    return f"{base}_{settlement_key}" if settlement_key else base


def _position_value(quantity: float, price: Optional[float], amount: Optional[float]) -> float:
    if amount is not None:
        return amount
    if price is not None:
        return quantity * price
    return nan


def positions_to_df(payload: dict[str, Any], position_columns: Optional[list[str]] = None) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []

    for group in payload.get("groupedAvailability") or []:
        if not isinstance(group, dict):
            continue
        for availability in group.get("availability") or []:
            if not isinstance(availability, dict):
                continue
            amount = _safe_float(availability.get("amount"))
            if amount is None or amount <= 0:
                continue

            currency = _currency_code(availability.get("symbol") or availability.get("simbol") or group.get("currency"))
            if not currency:
                continue
            settlement = _clean_text(availability.get("settlement"))
            rows.append(
                {
                    "symbol": _cash_symbol(currency, settlement),
                    "description": _clean_text(availability.get("name")) or f"{currency} cash",
                    "instrument_type": "cash",
                    "market": PPI_MARKET,
                    "source": PPI_SOURCE,
                    "account_id": ACCOUNT_ID,
                    "currency": currency,
                    "quantity": amount,
                    "price": 1.0,
                    "valuation": amount,
                }
            )

    for group in payload.get("groupedInstruments") or []:
        if not isinstance(group, dict):
            continue
        group_name = _clean_text(group.get("name"))
        instrument_type = _asset_type(group_name)
        for instrument in group.get("instruments") or []:
            if not isinstance(instrument, dict):
                continue
            ticker = _upper_text(instrument.get("ticker"))
            if not ticker:
                continue
            quantity = _safe_float(instrument.get("quantity") or instrument.get("amount"))
            if quantity is None or quantity <= 0:
                continue

            price = _safe_float(instrument.get("price"))
            amount = _safe_float(instrument.get("amount"))
            currency = _currency_code(instrument.get("currency") or instrument.get("settlementCurrency"))
            rows.append(
                {
                    "symbol": ticker,
                    "description": _clean_text(instrument.get("description")) or ticker,
                    "instrument_type": instrument_type,
                    "market": _clean_text(instrument.get("market")) or group_name or PPI_MARKET,
                    "source": PPI_SOURCE,
                    "account_id": ACCOUNT_ID,
                    "currency": currency,
                    "quantity": quantity,
                    "price": price if price is not None else nan,
                    "valuation": _position_value(quantity, price, amount),
                }
            )

    df = pd.DataFrame(rows)
    if df.empty:
        return df.reindex(columns=position_columns) if position_columns else df
    return df.reindex(columns=position_columns) if position_columns else df


def prices_from_positions(payload: dict[str, Any]) -> list[Price]:
    asof_dt = date.today()
    asof_ts = datetime.now(timezone.utc)
    prices: list[Price] = []

    for group in payload.get("groupedAvailability") or []:
        if not isinstance(group, dict):
            continue
        for availability in group.get("availability") or []:
            if not isinstance(availability, dict):
                continue
            amount = _safe_float(availability.get("amount"))
            currency = _currency_code(availability.get("symbol") or availability.get("simbol") or group.get("currency"))
            if amount is None or amount <= 0 or not currency:
                continue
            prices.append(
                Price(
                    asof_dt=asof_dt,
                    asof_ts=asof_ts,
                    symbol=_cash_symbol(currency, _clean_text(availability.get("settlement"))),
                    price_type="last",
                    price=1.0,
                    currency=currency,
                    venue=PPI_MARKET.upper(),
                    source=PPI_CASH_PRICE_SOURCE,
                    quality_score=100,
                )
            )

    for group in payload.get("groupedInstruments") or []:
        if not isinstance(group, dict):
            continue
        venue = _clean_text(group.get("name")) or PPI_MARKET.upper()
        for instrument in group.get("instruments") or []:
            if not isinstance(instrument, dict):
                continue
            ticker = _upper_text(instrument.get("ticker"))
            price = _safe_float(instrument.get("price"))
            currency = _currency_code(instrument.get("currency") or instrument.get("settlementCurrency"))
            if not ticker or price is None or price <= 0 or not currency:
                continue
            prices.append(
                Price(
                    asof_dt=asof_dt,
                    asof_ts=asof_ts,
                    symbol=ticker,
                    price_type="last",
                    price=price,
                    currency=currency,
                    venue=venue,
                    source=PPI_PRICE_SOURCE,
                    quality_score=PPI_PRICE_QUALITY_SCORE,
                )
            )

    return prices


def instruments_for_market_data(payload: dict[str, Any]) -> Iterable[tuple[str, str, str]]:
    for group in payload.get("groupedInstruments") or []:
        if not isinstance(group, dict):
            continue
        instrument_type = _clean_text(group.get("name"))
        if not instrument_type:
            continue
        for instrument in group.get("instruments") or []:
            if not isinstance(instrument, dict):
                continue
            ticker = _upper_text(instrument.get("ticker"))
            if not ticker:
                continue
            settlement = _clean_text(instrument.get("settlement")) or "A-48HS"
            yield ticker, instrument_type, settlement
