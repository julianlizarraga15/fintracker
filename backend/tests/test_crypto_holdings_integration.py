from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from backend.core.crypto_holdings import load_holdings
from backend.core.daily_snapshot import _crypto_positions_df
from backend.valuation.models import Position, Price, compute_valuations


def test_crypto_holdings_are_loaded_and_valued(tmp_path: Path):
    holdings_file = tmp_path / "crypto_holdings.json"
    holdings_file.write_text(
        """
        [
          { "symbol": "BTC", "quantity": 0.01, "display_name": "BTC (Exodus)",  "market": "crypto", "source": "exodus",  "currency": "USD" },
          { "symbol": "BTC", "quantity": 0.02, "display_name": "BTC (Binance)", "market": "crypto", "source": "binance", "currency": "USD" },
          { "symbol": "ETH", "quantity": 1.5,  "display_name": "ETH (Metamask)", "market": "crypto", "source": "metamask", "currency": "USD" }
        ]
        """,
        encoding="utf-8",
    )

    holdings = load_holdings(str(holdings_file))
    assert len(holdings) == 3

    df = _crypto_positions_df(holdings)
    assert not df.empty
    assert set(df["instrument_type"]) == {"crypto"}

    snapshot_dt = date(2024, 1, 1)
    snapshot_ts = datetime(2024, 1, 1, tzinfo=timezone.utc)

    pos_models = [
        Position(
            snapshot_dt=snapshot_dt,
            snapshot_ts=snapshot_ts,
            account_id=row["account_id"],
            source=row["source"],
            market=row["market"],
            symbol=row["symbol"],
            quantity=float(row["quantity"]),
            currency=row["currency"],
        )
        for row in df.to_dict(orient="records")
    ]

    price_models = [
        Price(
            asof_dt=snapshot_dt,
            asof_ts=snapshot_ts,
            symbol="BTC",
            price_type="last",
            price=30000.0,
            currency="USD",
            venue="COINGECKO",
            source="test",
            quality_score=85,
        ),
        Price(
            asof_dt=snapshot_dt,
            asof_ts=snapshot_ts,
            symbol="ETH",
            price_type="last",
            price=2000.0,
            currency="USD",
            venue="COINGECKO",
            source="test",
            quality_score=85,
        ),
    ]

    valuations = compute_valuations(
        pos_models,
        price_models,
        fx_rates=[],
        base_currency="USD",
        snapshot_dt=snapshot_dt,
        computed_ts=snapshot_ts,
    )

    assert len(valuations) == 3
    totals = {(v.symbol, v.source): v.value_base for v in valuations}
    assert totals[("BTC", "exodus")] == pytest.approx(300.0)
    assert totals[("BTC", "binance")] == pytest.approx(600.0)
    assert totals[("ETH", "metamask")] == pytest.approx(3000.0)
