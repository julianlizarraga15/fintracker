from __future__ import annotations

from math import nan
from typing import Iterable, Optional

import pandas as pd

from backend.core.binance_common import BINANCE_MARKET, BINANCE_SOURCE, BINANCE_STABLECOINS
from backend.core.config import ACCOUNT_ID


def balances_to_df(balances: Iterable[dict], position_columns: Optional[list[str]] = None) -> pd.DataFrame:
    """Normalize Binance balances into the POSITION_COLUMNS layout."""
    rows = []
    for entry in balances or []:
        if not isinstance(entry, dict):
            continue
        asset = str(entry.get("asset") or "").strip().upper()
        if not asset:
            continue
        try:
            free_amt = float(entry.get("free") or 0.0)
        except (TypeError, ValueError):
            free_amt = 0.0
        try:
            locked_amt = float(entry.get("locked") or 0.0)
        except (TypeError, ValueError):
            locked_amt = 0.0

        quantity = free_amt + locked_amt
        if quantity <= 0:
            continue

        is_stable = asset in BINANCE_STABLECOINS
        rows.append(
            {
                "symbol": asset,
                "description": None,
                "instrument_type": "crypto",
                "market": BINANCE_MARKET,
                "source": BINANCE_SOURCE,
                "account_id": ACCOUNT_ID,
                "currency": "USD",
                "quantity": quantity,
                "price": 1.0 if is_stable else nan,
                "valuation": quantity if is_stable else nan,
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        return df.reindex(columns=position_columns) if position_columns else df

    df = (
        df.groupby(
            ["symbol", "currency", "instrument_type", "market", "source", "account_id"],
            as_index=False,
        )
        .agg(
            {
                "description": "last",
                "price": "last",
                "quantity": "sum",
                "valuation": "sum",
            }
        )
    )

    return df.reindex(columns=position_columns) if position_columns else df
