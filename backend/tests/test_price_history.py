from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest

from backend.app import prices_history


def _write_prices(prices_dir: Path, dt_value: date, rows: list[dict]) -> None:
    dt_dir = prices_dir / f"dt={dt_value.isoformat()}"
    dt_dir.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    df.to_csv(dt_dir / f"prices_{dt_value.isoformat()}.csv", index=False)


def _write_fx(fx_dir: Path, dt_value: date, rows: list[dict]) -> None:
    dt_dir = fx_dir / f"dt={dt_value.isoformat()}"
    dt_dir.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    df.to_csv(dt_dir / f"fx_{dt_value.isoformat()}.csv", index=False)


def test_price_history_prefers_best_quality_and_converts_fx(tmp_path, monkeypatch):
    prices_history.clear_cache()
    prices_dir = tmp_path / "prices"
    fx_dir = tmp_path / "fx"
    today = date.today()
    previous = today - timedelta(days=1)

    _write_prices(
        prices_dir,
        today,
        [
            {
                "asof_dt": today.isoformat(),
                "asof_ts": f"{today.isoformat()}T10:00:00Z",
                "symbol": "ABC",
                "price": 100.0,
                "currency": "USD",
                "venue": "TEST1",
                "source": "low_quality",
                "quality_score": 70,
            },
            {
                "asof_dt": today.isoformat(),
                "asof_ts": f"{today.isoformat()}T12:00:00Z",
                "symbol": "ABC",
                "price": 105.0,
                "currency": "USD",
                "venue": "TEST2",
                "source": "better",
                "quality_score": 90,
            },
        ],
    )

    _write_prices(
        prices_dir,
        previous,
        [
            {
                "asof_dt": previous.isoformat(),
                "symbol": "ABC",
                "price": 20000.0,
                "currency": "ARS",
                "venue": "TEST3",
                "source": "ars_feed",
                "quality_score": 60,
            }
        ],
    )

    _write_fx(
        fx_dir,
        previous,
        [
            {
                "asof_dt": previous.isoformat(),
                "from_ccy": "ARS",
                "to_ccy": "USD",
                "rate": 0.001,
                "source": "fx_test",
                "max_age_days": 5,
            }
        ],
    )

    monkeypatch.setattr(prices_history, "PRICES_DIR", prices_dir)
    monkeypatch.setattr(prices_history, "FX_DIR", fx_dir)

    response = prices_history.get_price_history("abc", days=5, base_currency="usd")

    assert response.base_currency == "USD"
    assert response.window_days == 5
    assert response.points == 2
    assert response.missing_fx is False
    assert response.prices[0].asof_dt == previous
    assert response.prices[0].price_base == pytest.approx(20.0)
    assert response.prices[1].price == pytest.approx(105.0)
    assert response.prices[1].source == "better"
    assert response.prices[1].quality_score == 90


def test_price_history_handles_missing_fx_and_caps_window(tmp_path, monkeypatch):
    prices_history.clear_cache()
    prices_dir = tmp_path / "prices"
    fx_dir = tmp_path / "fx"
    today = date.today()

    _write_prices(
        prices_dir,
        today,
        [
            {
                "asof_dt": today.isoformat(),
                "symbol": "NFX",
                "price": 5000.0,
                "currency": "ARS",
                "venue": "TEST",
                "source": "missing_fx",
                "quality_score": 50,
            }
        ],
    )

    monkeypatch.setattr(prices_history, "PRICES_DIR", prices_dir)
    monkeypatch.setattr(prices_history, "FX_DIR", fx_dir)

    response = prices_history.get_price_history("NFX", days=5000, base_currency="USD")

    assert response.window_days == prices_history.MAX_WINDOW_DAYS
    assert response.points == 1
    assert response.missing_fx is True
    assert response.prices[0].price_base is None
