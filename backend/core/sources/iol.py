from __future__ import annotations

from math import nan
from typing import Any, Callable

import pandas as pd

from .models import SourcePositions


def fallback_positions_df(
    raw_items: list[dict[str, Any]], *, account_id: str, position_columns: list[str]
) -> pd.DataFrame:
    fallback_rows = []
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        symbol = item.get("simbolo") or item.get("ticker") or item.get("codigo")
        qty = item.get("cantidad") or item.get("cantidadNominal")
        try:
            qty_val = float(qty) if qty is not None else 0.0
        except (TypeError, ValueError):
            qty_val = 0.0
        if not symbol or qty_val <= 0:
            continue
        price_val = item.get("ultimoPrecio") or item.get("precio")
        try:
            price_coerced = float(price_val) if price_val is not None else nan
        except (TypeError, ValueError):
            price_coerced = nan
        fallback_rows.append(
            {
                "symbol": symbol,
                "description": item.get("descripcion"),
                "instrument_type": item.get("tipoInstrumento")
                or item.get("instrumento")
                or item.get("tipo"),
                "market": item.get("_market") or item.get("mercado"),
                "source": "iol",
                "account_id": account_id,
                "currency": None,
                "quantity": qty_val,
                "price": price_coerced,
                "valuation": nan,
            }
        )
    if not fallback_rows:
        return pd.DataFrame(columns=position_columns)
    return pd.DataFrame(fallback_rows).reindex(columns=position_columns)


def load_positions(
    *,
    username: str | None,
    password: str | None,
    account_id: str,
    position_columns: list[str],
    get_bearer_tokens: Callable[[str, str], tuple[str, str]],
    get_positions: Callable[[str], list[dict[str, Any]]],
    extract_positions_as_df: Callable[[list[dict[str, Any]]], pd.DataFrame],
) -> SourcePositions:
    if not (username and password):
        print("IOL credentials missing; skipping IOL positions.")
        return SourcePositions(df=pd.DataFrame(columns=position_columns))

    access_token, _ = get_bearer_tokens(username, password)
    raw_items = get_positions(access_token)
    iol_df = extract_positions_as_df(raw_items)
    if not iol_df.empty:
        iol_df["account_id"] = account_id
    elif raw_items:
        iol_df = fallback_positions_df(
            raw_items, account_id=account_id, position_columns=position_columns
        )
    return SourcePositions(df=iol_df, raw_items=raw_items, access_token=access_token)
