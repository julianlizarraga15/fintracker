from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

from backend.app import valuations
from backend.app.valuations import SnapshotNotFound


def _write_snapshot(base_dir: Path, dt_str: str, account_id: str, rows: list[dict]) -> None:
    dt_dir = base_dir / f"dt={dt_str}"
    account_dir = dt_dir / f"account={account_id}"
    account_dir.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    df.to_csv(account_dir / f"valuations_{dt_str}.csv", index=False)


def test_get_latest_valuation_snapshot_builds_response(tmp_path, monkeypatch):
    valuations_dir = tmp_path / "valuations"
    older_rows = [
        {
            "snapshot_dt": "2024-10-01",
            "computed_ts": "2024-10-01T12:00:00Z",
            "symbol": "OLD",
            "quantity": 1,
            "value_base": 10,
            "status": "ok",
        }
    ]
    latest_rows = [
        {
            "snapshot_dt": "2024-11-15",
            "computed_ts": "2024-11-15T08:30:00Z",
            "symbol": "AAPL",
            "quantity": 3,
            "value_base": 1500.5,
            "status": "ok",
            "unit_price_base": 500.166,
            "price_quality_score": 90,
        },
        {
            "snapshot_dt": "2024-11-15",
            "computed_ts": "2024-11-15T08:30:00Z",
            "symbol": "BOND1",
            "quantity": 2,
            "value_base": 800,
            "status": "stale",
            "unit_price_base": 400,
            "price_quality_score": 75,
        },
    ]

    _write_snapshot(valuations_dir, "2024-10-01", "acc-123", older_rows)
    _write_snapshot(valuations_dir, "2024-11-15", "acc-123", latest_rows)
    monkeypatch.setattr(valuations, "VALUATIONS_DIR", valuations_dir)

    response = valuations.get_latest_valuation_snapshot("acc-123")

    assert response.snapshot_dt.isoformat() == "2024-11-15"
    assert response.computed_ts == datetime.fromisoformat("2024-11-15T08:30:00+00:00")
    assert response.totals.positions == 2
    assert response.totals.ok_positions == 1
    assert response.totals.total_value_base == pytest.approx(2300.5)
    assert [row.symbol for row in response.rows] == ["AAPL", "BOND1"]
    assert response.rows[0].price_quality_score == 90
    assert response.rows[1].status == "stale"
    assert response.rows[0].portfolio_share_pct == pytest.approx(65.22495, rel=1e-3)
    assert response.rows[1].portfolio_share_pct == pytest.approx(34.77505, rel=1e-3)
    assert response.source_file.endswith(".csv")


def test_get_latest_valuation_snapshot_missing_account(tmp_path, monkeypatch):
    valuations_dir = tmp_path / "valuations"
    (valuations_dir / "dt=2024-11-01").mkdir(parents=True)
    monkeypatch.setattr(valuations, "VALUATIONS_DIR", valuations_dir)

    with pytest.raises(SnapshotNotFound) as excinfo:
        valuations.get_latest_valuation_snapshot("missing")

    assert "missing" in str(excinfo.value)
