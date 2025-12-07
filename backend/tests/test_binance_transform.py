from __future__ import annotations

import pandas as pd
import pytest

from backend.core import binance_transform
from backend.core import daily_snapshot


def test_balances_to_df_shapes_rows(monkeypatch):
    monkeypatch.setattr(binance_transform, "ACCOUNT_ID", "acct-binance")
    balances = [
        {"asset": "btc", "free": "1.2", "locked": "0.3"},
        {"asset": "usdt", "free": 10, "locked": 0},
        {"asset": "btc", "free": 0.5, "locked": 0},
        {"asset": "zero", "free": 0, "locked": 0},
    ]

    df = binance_transform.balances_to_df(balances, position_columns=daily_snapshot.POSITION_COLUMNS)

    assert set(df["symbol"]) == {"BTC", "USDT"}
    assert list(df.columns) == daily_snapshot.POSITION_COLUMNS

    btc_row = df[df["symbol"] == "BTC"].iloc[0]
    assert btc_row["quantity"] == pytest.approx(2.0)
    assert pd.isna(btc_row["price"])

    usdt_row = df[df["symbol"] == "USDT"].iloc[0]
    assert usdt_row["price"] == pytest.approx(1.0)
    assert usdt_row["valuation"] == pytest.approx(10)
    assert usdt_row["account_id"] == "acct-binance"
