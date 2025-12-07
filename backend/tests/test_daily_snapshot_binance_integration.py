from __future__ import annotations

from datetime import date, datetime, timezone

import pandas as pd

from backend.core import binance_transform, daily_snapshot, iol_transform
from backend.valuation.models import Price as PriceModel


def test_daily_snapshot_merges_and_prices_binance(monkeypatch):
    dt_str = "2024-01-01"
    saved = {}

    def fake_save_snapshot_files(df, resource_name="positions", source=None, account_id=None):
        saved[resource_name] = df.copy()
        return {"csv": f"/tmp/{resource_name}.csv", "parquet": None, "dt": dt_str}

    monkeypatch.setattr(daily_snapshot, "save_snapshot_files", fake_save_snapshot_files)
    monkeypatch.setattr(daily_snapshot, "maybe_upload_to_s3", lambda *args, **kwargs: None)
    monkeypatch.setattr(daily_snapshot, "IOL_USERNAME", "user")
    monkeypatch.setattr(daily_snapshot, "IOL_PASSWORD", "pass")
    monkeypatch.setattr(daily_snapshot, "ENABLE_BINANCE", True)
    monkeypatch.setattr(daily_snapshot, "BINANCE_API_KEY", "key")
    monkeypatch.setattr(daily_snapshot, "BINANCE_API_SECRET", "secret")
    monkeypatch.setattr(daily_snapshot, "ACCOUNT_ID", "acct-1")
    monkeypatch.setattr(iol_transform, "ACCOUNT_ID", "acct-1")
    monkeypatch.setattr(binance_transform, "ACCOUNT_ID", "acct-1")

    monkeypatch.setattr(daily_snapshot, "get_bearer_tokens", lambda u, p: ("token", "refresh"))
    monkeypatch.setattr(
        daily_snapshot,
        "get_positions",
        lambda access_token: [{"simbolo": "IOL1", "cantidad": 2, "precio": 10, "_market": "ar"}],
    )
    monkeypatch.setattr(daily_snapshot, "get_prices_for_positions", lambda items, token: [
        PriceModel(
            asof_dt=date.today(),
            asof_ts=datetime.now(timezone.utc),
            symbol="IOL1",
            price_type="last",
            price=10.0,
            currency="USD",
            venue="IOL",
            source="iol",
            quality_score=100,
        )
    ])
    monkeypatch.setattr(daily_snapshot, "load_holdings", lambda: [])
    monkeypatch.setattr(daily_snapshot, "load_crypto_holdings", lambda: [])
    monkeypatch.setattr(daily_snapshot, "fetch_crypto_prices", lambda symbols: [])
    monkeypatch.setattr(daily_snapshot, "fetch_santander_nav_values", lambda fund_ids: [])
    monkeypatch.setattr(daily_snapshot, "get_fx_rates", lambda: [])

    monkeypatch.setattr(
        daily_snapshot,
        "get_account_balances",
        lambda **kwargs: [
            {"asset": "BTC", "free": 0.5, "locked": 0.0},
            {"asset": "USDT", "free": 20.0, "locked": 0.0},
        ],
    )

    asof_dt = date.today()
    asof_ts = datetime.now(timezone.utc)
    monkeypatch.setattr(
        daily_snapshot,
        "fetch_binance_prices",
        lambda symbols: (
            [
                PriceModel(
                    asof_dt=asof_dt,
                    asof_ts=asof_ts,
                    symbol="BTC",
                    price_type="last",
                    price=30000.0,
                    currency="USDT",
                    venue="BINANCE",
                    source="binance_api",
                    quality_score=90,
                ),
                PriceModel(
                    asof_dt=asof_dt,
                    asof_ts=asof_ts,
                    symbol="USDT",
                    price_type="last",
                    price=1.0,
                    currency="USDT",
                    venue="BINANCE",
                    source="binance_api",
                    quality_score=90,
                ),
            ],
            [],
        ),
    )

    daily_snapshot.main()

    positions_df = saved["positions"]
    assert set(positions_df["symbol"]) == {"IOL1", "BTC", "USDT"}
    assert (positions_df[positions_df["source"] == "binance"]["quantity"] > 0).all()

    prices_df = saved["prices"]
    assert set(prices_df["symbol"]) == {"IOL1", "BTC", "USDT"}
    assert (prices_df["account_id"] == "acct-1").all()

    fx_df = saved["fx"]
    assert any(row["from_ccy"] == "USDT" and row["to_ccy"] == "USD" for _, row in fx_df.iterrows())

    valuations_df = saved["valuations"]
    btc_val = valuations_df[valuations_df["symbol"] == "BTC"].iloc[0]
    assert pd.notna(btc_val["value_base"])
    assert btc_val["status"] == "ok"
