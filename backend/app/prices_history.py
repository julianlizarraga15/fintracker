from __future__ import annotations

import os
from datetime import date, datetime, timedelta
from pathlib import Path
from time import time
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd
from pydantic import BaseModel, Field

SNAPSHOTS_ROOT = Path(os.getenv("SNAPSHOTS_LOCAL_DIR", "data/positions"))
PRICES_DIR = SNAPSHOTS_ROOT / "prices"
FX_DIR = SNAPSHOTS_ROOT / "fx"

DEFAULT_WINDOW_DAYS = 30
MAX_WINDOW_DAYS = 180
CACHE_TTL_SECONDS = 45
FX_LOOKBACK_BUFFER_DAYS = 7
FX_DEFAULT_MAX_AGE_DAYS = 3
DEFAULT_BASE_CURRENCY = os.getenv("VALUATIONS_BASE_CURRENCY", "USD").upper()


class PriceHistoryPoint(BaseModel):
    asof_dt: date
    asof_ts: Optional[datetime] = None
    price: float
    currency: str
    price_base: Optional[float] = None
    source: Optional[str] = None
    venue: Optional[str] = None
    quality_score: Optional[int] = Field(None, ge=0, le=100)


class PriceHistoryResponse(BaseModel):
    base_currency: str
    window_days: int
    points: int
    missing_fx: bool = False
    prices: List[PriceHistoryPoint]


def _safe_float(value) -> Optional[float]:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return None if pd.isna(result) else result


def _safe_int(value) -> Optional[int]:
    try:
        result = int(value)
    except (TypeError, ValueError):
        return None
    return None if pd.isna(result) else result


def _parse_date(value) -> date:
    if isinstance(value, date):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        return date.fromisoformat(value.split(" ")[0])
    raise ValueError("Invalid date value.")


def _parse_datetime(value) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        sanitized = value.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(sanitized)
        except ValueError:
            return None
    return None


def _latest_dt_dirs(base_dir: Path) -> list[tuple[date, Path]]:
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


def _pick_snapshot_file(dt_dir: Path, prefix: str) -> Optional[Path]:
    parquet_files = sorted(dt_dir.glob(f"{prefix}*.parquet"), key=lambda p: p.stat().st_mtime, reverse=True)
    if parquet_files:
        return parquet_files[0]
    csv_files = sorted(dt_dir.glob(f"{prefix}*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    if csv_files:
        return csv_files[0]
    return None


def _read_snapshot(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path) if path.suffix == ".parquet" else pd.read_csv(path)


def _load_price_rows(symbol: str, window_start: date) -> list[dict]:
    symbol_upper = symbol.upper()
    records: list[dict] = []
    for dt_value, dt_path in _latest_dt_dirs(PRICES_DIR):
        if dt_value < window_start:
            break
        file_path = _pick_snapshot_file(dt_path, "prices_")
        if not file_path:
            continue
        df = _read_snapshot(file_path)
        if df.empty or "symbol" not in df:
            continue
        df_filtered = df[df["symbol"].astype(str).str.upper() == symbol_upper]
        if df_filtered.empty:
            continue
        records.extend(df_filtered.to_dict(orient="records"))
    return records


def _load_fx_rows(earliest_needed: date) -> list[dict]:
    rows: list[dict] = []
    for dt_value, dt_path in _latest_dt_dirs(FX_DIR):
        if dt_value < earliest_needed:
            break
        file_path = _pick_snapshot_file(dt_path, "fx_")
        if not file_path:
            continue
        df = _read_snapshot(file_path)
        if df.empty:
            continue
        rows.extend(df.to_dict(orient="records"))
    return rows


def _build_fx_lookup(fx_rows: Iterable[dict]) -> Dict[tuple[str, str], list[tuple[date, float, int]]]:
    lookup: Dict[tuple[str, str], list[tuple[date, float, int]]] = {}
    for row in fx_rows:
        try:
            asof_dt = _parse_date(row.get("asof_dt"))
        except Exception:
            continue
        from_ccy = (row.get("from_ccy") or "").upper()
        to_ccy = (row.get("to_ccy") or "").upper()
        rate = _safe_float(row.get("rate"))
        if not from_ccy or not to_ccy or rate is None:
            continue
        max_age_days = _safe_int(row.get("max_age_days"))
        age_limit = FX_DEFAULT_MAX_AGE_DAYS if max_age_days is None else max_age_days
        lookup.setdefault((from_ccy, to_ccy), []).append((asof_dt, rate, age_limit))

    for rates in lookup.values():
        rates.sort(key=lambda item: item[0], reverse=True)
    return lookup


def _resolve_fx_rate(
    from_ccy: str, to_ccy: str, asof_dt: date, lookup: Dict[tuple[str, str], list[tuple[date, float, int]]]
) -> Optional[float]:
    direct = lookup.get((from_ccy, to_ccy), [])
    for dt_value, rate, max_age in direct:
        if dt_value > asof_dt:
            continue
        if max_age and (asof_dt - dt_value).days > max_age:
            continue
        return rate

    inverse = lookup.get((to_ccy, from_ccy), [])
    for dt_value, rate, max_age in inverse:
        if dt_value > asof_dt:
            continue
        if max_age and (asof_dt - dt_value).days > max_age:
            continue
        if rate:
            return 1.0 / rate
    return None


def _pick_best_price_per_day(price_rows: Iterable[dict]) -> Dict[date, dict]:
    best: Dict[date, dict] = {}
    for row in price_rows:
        try:
            asof_dt = _parse_date(row.get("asof_dt"))
        except Exception:
            continue
        price_value = _safe_float(row.get("price"))
        currency = row.get("currency")
        if price_value is None or not currency:
            continue
        quality = _safe_int(row.get("quality_score")) or 0
        asof_ts = _parse_datetime(row.get("asof_ts") or row.get("valid_from_ts"))
        current = best.get(asof_dt)
        if current is None:
            best[asof_dt] = {
                "asof_dt": asof_dt,
                "asof_ts": asof_ts,
                "price": price_value,
                "currency": currency,
                "source": row.get("source"),
                "venue": row.get("venue"),
                "quality_score": quality,
            }
            continue

        current_quality = _safe_int(current.get("quality_score")) or 0
        if quality > current_quality:
            best[asof_dt] = {
                "asof_dt": asof_dt,
                "asof_ts": asof_ts,
                "price": price_value,
                "currency": currency,
                "source": row.get("source"),
                "venue": row.get("venue"),
                "quality_score": quality,
            }
        elif quality == current_quality:
            if asof_ts and current.get("asof_ts"):
                if asof_ts > current["asof_ts"]:
                    best[asof_dt] = {
                        "asof_dt": asof_dt,
                        "asof_ts": asof_ts,
                        "price": price_value,
                        "currency": currency,
                        "source": row.get("source"),
                        "venue": row.get("venue"),
                        "quality_score": quality,
                    }
            elif asof_ts and not current.get("asof_ts"):
                best[asof_dt] = {
                    "asof_dt": asof_dt,
                    "asof_ts": asof_ts,
                    "price": price_value,
                    "currency": currency,
                    "source": row.get("source"),
                    "venue": row.get("venue"),
                    "quality_score": quality,
                }
    return best


_CACHE: Dict[tuple[str, int, str], tuple[float, PriceHistoryResponse]] = {}


def _get_cached(key: tuple[str, int, str]) -> Optional[PriceHistoryResponse]:
    cached = _CACHE.get(key)
    if not cached:
        return None
    expires_at, response = cached
    if expires_at < time():
        _CACHE.pop(key, None)
        return None
    return response


def _set_cached(key: tuple[str, int, str], response: PriceHistoryResponse) -> None:
    if CACHE_TTL_SECONDS <= 0:
        return
    _CACHE[key] = (time() + CACHE_TTL_SECONDS, response)


def clear_cache() -> None:
    _CACHE.clear()


def get_price_history(symbol: str, days: int = DEFAULT_WINDOW_DAYS, base_currency: Optional[str] = None) -> PriceHistoryResponse:
    if not symbol or not symbol.strip():
        raise ValueError("symbol is required")

    window_days = max(1, min(days or DEFAULT_WINDOW_DAYS, MAX_WINDOW_DAYS))
    base_ccy = (base_currency or DEFAULT_BASE_CURRENCY).upper()
    cache_key = (symbol.upper(), window_days, base_ccy)
    cached = _get_cached(cache_key)
    if cached:
        return cached

    window_start = date.today() - timedelta(days=window_days - 1)
    price_rows = _load_price_rows(symbol, window_start)
    per_day = {dt: row for dt, row in _pick_best_price_per_day(price_rows).items() if dt >= window_start}

    if not per_day:
        response = PriceHistoryResponse(
            base_currency=base_ccy,
            window_days=window_days,
            points=0,
            missing_fx=False,
            prices=[],
        )
        _set_cached(cache_key, response)
        return response

    earliest_dt = min(per_day.keys())
    fx_rows = _load_fx_rows(earliest_dt - timedelta(days=FX_LOOKBACK_BUFFER_DAYS))
    fx_lookup = _build_fx_lookup(fx_rows)

    prices: list[PriceHistoryPoint] = []
    missing_fx = False
    for dt_value in sorted(per_day.keys()):
        row = per_day[dt_value]
        currency = (row.get("currency") or "").upper()
        price_value = row.get("price")
        price_base = None
        if currency == base_ccy:
            price_base = price_value
        else:
            rate = _resolve_fx_rate(currency, base_ccy, dt_value, fx_lookup)
            if rate is None:
                missing_fx = True
            else:
                price_base = price_value * rate

        prices.append(
            PriceHistoryPoint(
                asof_dt=dt_value,
                asof_ts=row.get("asof_ts"),
                price=price_value,
                currency=currency,
                price_base=price_base,
                source=row.get("source"),
                venue=row.get("venue"),
                quality_score=_safe_int(row.get("quality_score")),
            )
        )

    response = PriceHistoryResponse(
        base_currency=base_ccy,
        window_days=window_days,
        points=len(prices),
        missing_fx=missing_fx,
        prices=prices,
    )
    _set_cached(cache_key, response)
    return response
