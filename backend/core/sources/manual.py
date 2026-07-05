from __future__ import annotations

from math import nan

import pandas as pd

from backend.core.crypto_holdings import CryptoHolding
from backend.core.santander_holdings import SantanderHolding


def santander_positions_df(
    holdings: list[SantanderHolding], *, position_columns: list[str]
) -> pd.DataFrame:
    rows = []
    for holding in holdings:
        price = holding.get("price")
        valuation = holding.get("valuation")
        rows.append(
            {
                "symbol": holding["symbol"],
                "description": holding.get("display_name"),
                "instrument_type": "fci",
                "market": holding["market"],
                "source": holding["source"],
                "account_id": holding["account_id"],
                "currency": holding["currency"],
                "quantity": holding["quantity"],
                "price": price if price is not None else nan,
                "valuation": valuation if valuation is not None else nan,
            }
        )
    df_manual = pd.DataFrame(rows)
    if df_manual.empty:
        return df_manual
    return df_manual.reindex(columns=position_columns)


def crypto_positions_df(
    holdings: list[CryptoHolding], *, position_columns: list[str]
) -> pd.DataFrame:
    rows = []
    for holding in holdings:
        rows.append(
            {
                "symbol": holding["symbol"],
                "description": holding.get("display_name"),
                "instrument_type": "crypto",
                "market": holding["market"],
                "source": holding["source"],
                "account_id": holding["account_id"],
                "currency": holding["currency"],
                "quantity": holding["quantity"],
                "price": nan,
                "valuation": nan,
            }
        )
    df_manual = pd.DataFrame(rows)
    if df_manual.empty:
        return df_manual
    return df_manual.reindex(columns=position_columns)
