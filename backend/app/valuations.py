from __future__ import annotations

import os
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from backend.app.snapshot_parsing import parse_snapshot_date, parse_snapshot_datetime, safe_float, safe_int
from pydantic import BaseModel, Field


class SnapshotNotFound(RuntimeError):
    """Raised when no valuation snapshot can be located."""


SNAPSHOTS_ROOT = Path(os.getenv("SNAPSHOTS_LOCAL_DIR", "data/positions"))
VALUATIONS_DIR = SNAPSHOTS_ROOT / "valuations"
BASE_CURRENCY = os.getenv("VALUATIONS_BASE_CURRENCY", "USD")
PORTFOLIO_PCT_SCALE = 100.0


def _latest_dt_dirs(base_dir: Path):
    if not base_dir.exists():
        return []
    dirs = []
    for child in base_dir.iterdir():
        if child.is_dir() and child.name.startswith("dt="):
            _, _, dt_str = child.name.partition("=")
            try:
                dirs.append((date.fromisoformat(dt_str), child))
            except ValueError:
                continue
    return sorted(dirs, key=lambda item: item[0], reverse=True)


def _account_dir(dt_dir: Path, account_id: str) -> Path:
    candidate = dt_dir / f"account={account_id}"
    if not candidate.exists():
        raise SnapshotNotFound(f"No valuations for account '{account_id}' in {dt_dir.name}.")
    return candidate


def _pick_snapshot_file(account_dir: Path) -> Path:
    # prefer parquet, fall back to csv
    parquet_files = sorted(account_dir.glob("valuations_*.parquet"), key=lambda p: p.stat().st_mtime, reverse=True)
    if parquet_files:
        return parquet_files[0]
    csv_files = sorted(account_dir.glob("valuations_*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    if csv_files:
        return csv_files[0]
    raise SnapshotNotFound(f"No valuation files under {account_dir}.")


def _read_snapshot(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path) if path.suffix == ".parquet" else pd.read_csv(path)


def _parse_date(value) -> date:
    try:
        return parse_snapshot_date(value, error_message="snapshot_dt missing in valuation file.")
    except ValueError as exc:
        raise SnapshotNotFound(str(exc)) from exc


def _parse_datetime(value) -> datetime:
    try:
        return parse_snapshot_datetime(value, error_message="computed_ts missing in valuation file.")
    except ValueError as exc:
        if str(exc) == "computed_ts missing in valuation file.":
            raise SnapshotNotFound(str(exc)) from exc
        raise


def _compute_portfolio_share(value_base: Optional[float], total_value_base: Optional[float]) -> Optional[float]:
    if value_base is None or total_value_base is None:
        return None
    if total_value_base <= 0:
        return None
    return (value_base / total_value_base) * PORTFOLIO_PCT_SCALE


class ValuationRow(BaseModel):
    symbol: str
    quantity: float
    value_base: Optional[float] = None
    unit_price_base: Optional[float] = None
    unit_price_native: Optional[float] = None
    unit_price_native_ccy: Optional[str] = None
    fx_rate_to_base: Optional[float] = None
    account_id: Optional[str] = None
    source: Optional[str] = None
    market: Optional[str] = None
    asset_type: Optional[str] = None
    status: Optional[str] = None
    price_source: Optional[str] = None
    price_quality_score: Optional[int] = Field(None, ge=0, le=100)
    fx_source: Optional[str] = None
    portfolio_share_pct: Optional[float] = None


class ValuationTotals(BaseModel):
    positions: int
    ok_positions: int
    total_value_base: Optional[float] = None
    base_currency: str = Field(default=BASE_CURRENCY)


class SourceAllocation(BaseModel):
    source: str
    total_value_base: float
    portfolio_share_pct: Optional[float] = None
    positions: int
    ok_positions: int
    non_ok_positions: int
    markets: list[str] = Field(default_factory=list)
    asset_types: list[str] = Field(default_factory=list)
    top_symbols: list[str] = Field(default_factory=list)


class LatestValuationResponse(BaseModel):
    snapshot_dt: date
    computed_ts: datetime
    account_id: str
    base_currency: str = Field(default=BASE_CURRENCY)
    totals: ValuationTotals
    rows: list[ValuationRow]
    source_allocations: list[SourceAllocation] = Field(default_factory=list)
    source_file: str


def _clean_group_value(value, fallback: str = "unknown") -> str:
    if value is None or pd.isna(value):
        return fallback
    text = str(value).strip()
    return text or fallback


def _unique_sorted(values) -> list[str]:
    return sorted({_clean_group_value(value, "") for value in values if _clean_group_value(value, "")})


def _build_source_allocations(df: pd.DataFrame, total_value_base: Optional[float]) -> list[SourceAllocation]:
    required_columns = {"value_base", "source", "status", "symbol", "quantity", "asset_type"}
    if not required_columns.issubset(df.columns):
        return []

    working = df.copy()
    working["_source_key"] = working["source"].apply(_clean_group_value)
    working["_symbol_key"] = working.apply(
        lambda row: _clean_group_value(row.get("symbol") or row.get("raw_symbol"), ""), axis=1
    )
    working["_value_base"] = pd.to_numeric(working["value_base"], errors="coerce").fillna(0.0)

    allocations: list[SourceAllocation] = []
    for source, source_df in working.groupby("_source_key", dropna=False):
        symbol_totals = (
            source_df[source_df["_symbol_key"] != ""]
            .groupby("_symbol_key")["_value_base"]
            .sum()
            .sort_values(ascending=False)
        )
        total = safe_float(source_df["_value_base"].sum()) or 0.0
        ok_positions = int((source_df["status"] == "ok").sum())
        allocations.append(
            SourceAllocation(
                source=source,
                total_value_base=total,
                portfolio_share_pct=_compute_portfolio_share(total, total_value_base),
                positions=int(source_df["_symbol_key"].replace("", pd.NA).nunique()),
                ok_positions=ok_positions,
                non_ok_positions=int(len(source_df) - ok_positions),
                markets=_unique_sorted(source_df["market"]) if "market" in source_df else [],
                asset_types=_unique_sorted(source_df["asset_type"]),
                top_symbols=list(symbol_totals.head(5).index),
            )
        )
    allocations.sort(key=lambda allocation: allocation.total_value_base, reverse=True)
    return allocations


def _build_rows(df: pd.DataFrame, total_value_base: Optional[float]) -> list[ValuationRow]:
    rows: list[ValuationRow] = []
    for record in df.to_dict(orient="records"):
        symbol = record.get("symbol") or record.get("raw_symbol")
        if not symbol:
            continue
        value_base = safe_float(record.get("value_base"))
        rows.append(
            ValuationRow(
                symbol=symbol,
                quantity=safe_float(record.get("quantity")) or 0.0,
                value_base=value_base,
                unit_price_base=safe_float(record.get("unit_price_base")),
                unit_price_native=safe_float(record.get("unit_price_native")),
                unit_price_native_ccy=record.get("unit_price_native_ccy"),
                fx_rate_to_base=safe_float(record.get("fx_rate_to_base")),
                account_id=record.get("account_id"),
                source=record.get("source"),
                market=record.get("market"),
                asset_type=record.get("asset_type"),
                status=record.get("status"),
                price_source=record.get("price_source"),
                price_quality_score=safe_int(record.get("price_quality_score")),
                fx_source=record.get("fx_source"),
                portfolio_share_pct=_compute_portfolio_share(value_base, total_value_base),
            )
        )
    rows.sort(key=lambda row: (row.value_base or float("-inf")), reverse=True)
    return rows


def get_latest_valuation_snapshot(account_id: str) -> LatestValuationResponse:
    partitions = _latest_dt_dirs(VALUATIONS_DIR)
    if not partitions:
        raise SnapshotNotFound("No valuation snapshots found.")

    last_error: Optional[Exception] = None
    for dt_value, dt_path in partitions:
        try:
            account_dir = _account_dir(dt_path, account_id)
            snapshot_path = _pick_snapshot_file(account_dir)
            df = _read_snapshot(snapshot_path)
            if df.empty:
                raise SnapshotNotFound(f"Valuation file {snapshot_path} is empty.")

            snapshot_dt = _parse_date(df["snapshot_dt"].iloc[0] if "snapshot_dt" in df else dt_value)
            computed_ts = _parse_datetime(df["computed_ts"].iloc[0] if "computed_ts" in df else datetime.utcnow())
            totals = ValuationTotals(
                positions=len(df),
                ok_positions=int((df["status"] == "ok").sum()) if "status" in df else 0,
                total_value_base=safe_float(pd.to_numeric(df["value_base"], errors="coerce").sum()) if "value_base" in df else None,
            )
            rows = _build_rows(df, totals.total_value_base)
            source_allocations = _build_source_allocations(df, totals.total_value_base)
            return LatestValuationResponse(
                snapshot_dt=snapshot_dt,
                computed_ts=computed_ts,
                account_id=account_id,
                totals=totals,
                rows=rows,
                source_allocations=source_allocations,
                source_file=str(snapshot_path),
            )
        except SnapshotNotFound as exc:
            last_error = exc
            continue
    raise SnapshotNotFound(str(last_error) if last_error else "Unable to load valuation snapshot.")
