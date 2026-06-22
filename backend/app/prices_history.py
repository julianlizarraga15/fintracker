from __future__ import annotations

import os
from datetime import date, datetime, timedelta
from pathlib import Path
from time import time
from typing import Dict, Iterable, List, Literal, Optional

import pandas as pd
from backend.app.corporate_actions import CorporateAction, get_corporate_actions, parse_cedear_ratio
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
OUTLIER_DAILY_CHANGE_THRESHOLD_PCT = 30.0


class RawPriceCandidate(BaseModel):
    asof_dt: date
    price: float
    currency: str
    source: Optional[str] = None
    venue: Optional[str] = None
    quality_score: Optional[int] = Field(None, ge=0, le=100)
    asof_ts: Optional[datetime] = None


class PriceHistoryPoint(BaseModel):
    asof_dt: date
    asof_ts: Optional[datetime] = None
    price: float
    currency: str
    price_base: Optional[float] = None
    price_raw: float
    price_base_raw: Optional[float] = None
    price_adjusted: float
    price_base_adjusted: Optional[float] = None
    is_known_event: bool = False
    known_event_reason: Optional[str] = None
    source: Optional[str] = None
    venue: Optional[str] = None
    quality_score: Optional[int] = Field(None, ge=0, le=100)
    daily_change_pct: Optional[float] = None
    is_outlier: bool = False
    outlier_reason: Optional[str] = None
    raw_candidates: list[RawPriceCandidate] = Field(default_factory=list)


class PriceHistoryResponse(BaseModel):
    base_currency: str
    window_days: int
    points: int
    missing_fx: bool = False
    has_adjustments: bool = False
    default_series: Literal["raw", "adjusted"] = "raw"
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


def _collect_snapshot_files(dt_dir: Path, prefix: str) -> list[Path]:
    parquet_files = sorted(dt_dir.rglob(f"{prefix}*.parquet"), key=lambda p: p.stat().st_mtime, reverse=True)
    if parquet_files:
        return parquet_files
    return sorted(dt_dir.rglob(f"{prefix}*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)


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
        files = _collect_snapshot_files(dt_path, "fx_")
        if not files:
            continue
        for file_path in files:
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


def _price_candidate_from_row(row: dict) -> Optional[dict]:
    try:
        asof_dt = _parse_date(row.get("asof_dt"))
    except Exception:
        return None
    price_value = _safe_float(row.get("price"))
    currency = row.get("currency")
    if price_value is None or not currency:
        return None
    return {
        "asof_dt": asof_dt,
        "asof_ts": _parse_datetime(row.get("asof_ts") or row.get("valid_from_ts")),
        "price": price_value,
        "currency": str(currency).upper(),
        "source": row.get("source"),
        "venue": row.get("venue"),
        "quality_score": _safe_int(row.get("quality_score")) or 0,
    }


def _is_better_price_candidate(candidate: dict, current: dict) -> bool:
    candidate_quality = _safe_int(candidate.get("quality_score")) or 0
    current_quality = _safe_int(current.get("quality_score")) or 0
    if candidate_quality != current_quality:
        return candidate_quality > current_quality

    candidate_ts = candidate.get("asof_ts")
    current_ts = current.get("asof_ts")
    if candidate_ts and current_ts:
        return candidate_ts > current_ts
    return bool(candidate_ts and not current_ts)


def _pick_best_price_per_day(price_rows: Iterable[dict]) -> tuple[Dict[date, dict], Dict[date, list[dict]]]:
    best: Dict[date, dict] = {}
    candidates_by_day: Dict[date, list[dict]] = {}
    for row in price_rows:
        candidate = _price_candidate_from_row(row)
        if candidate is None:
            continue

        asof_dt = candidate["asof_dt"]
        candidates_by_day.setdefault(asof_dt, []).append(candidate)
        current = best.get(asof_dt)
        if current is None or _is_better_price_candidate(candidate, current):
            best[asof_dt] = candidate

    return best, candidates_by_day


def _cedear_ratio_change_factor(action: CorporateAction, dt_value: date) -> Optional[float]:
    old_ratio = parse_cedear_ratio(action.old_ratio)
    new_ratio = parse_cedear_ratio(action.new_ratio)
    if old_ratio is None or new_ratio in (None, 0):
        return None
    return old_ratio / new_ratio if dt_value < action.effective_date else 1.0


def _find_ratio_action(actions: list[CorporateAction], dt_value: date) -> Optional[CorporateAction]:
    applicable = [
        action
        for action in actions
        if action.kind == "cedear_ratio_change" and parse_cedear_ratio(action.new_ratio) is not None
    ]
    if not applicable:
        return None
    past_actions = [action for action in applicable if dt_value >= action.effective_date]
    if past_actions:
        return max(past_actions, key=lambda action: action.effective_date)
    return min(applicable, key=lambda action: action.effective_date)


def _known_event_dates(actions: list[CorporateAction], available_dates: Iterable[date]) -> dict[date, str]:
    event_dates: dict[date, str] = {}
    sorted_dates = sorted(available_dates)
    for action in actions:
        if action.kind != "cedear_ratio_change":
            continue
        reason = action.description or (
            f"{action.symbol} CEDEAR ratio changed from {action.old_ratio} to {action.new_ratio}"
        )
        adjacent = {dt for dt in sorted_dates if abs((dt - action.effective_date).days) <= 1}
        previous_dates = [dt for dt in sorted_dates if dt < action.effective_date]
        next_dates = [dt for dt in sorted_dates if dt >= action.effective_date]
        if previous_dates:
            adjacent.add(max(previous_dates))
        if next_dates:
            adjacent.add(min(next_dates))
        for dt in adjacent:
            event_dates[dt] = reason
    return event_dates


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
    best_per_day, candidates_by_day = _pick_best_price_per_day(price_rows)
    per_day = {dt: row for dt, row in best_per_day.items() if dt >= window_start}
    candidates_by_day = {dt: rows for dt, rows in candidates_by_day.items() if dt >= window_start}

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

    actions = get_corporate_actions(symbol)
    known_event_dates = _known_event_dates(actions, per_day.keys())

    prices: list[PriceHistoryPoint] = []
    missing_fx = False
    previous_display_price: Optional[float] = None
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

        action = _find_ratio_action(actions, dt_value)
        adjustment_factor = _cedear_ratio_change_factor(action, dt_value) if action else None
        price_adjusted = price_value * adjustment_factor if adjustment_factor is not None else price_value
        price_base_adjusted = (
            price_base * adjustment_factor
            if adjustment_factor is not None and price_base is not None
            else price_base
        )
        display_price = price_base_adjusted if price_base_adjusted is not None else price_adjusted
        is_known_event = dt_value in known_event_dates
        known_event_reason = known_event_dates.get(dt_value)

        daily_change_pct = None
        is_outlier = False
        outlier_reason = None
        if previous_display_price not in (None, 0) and display_price is not None:
            daily_change_pct = ((display_price - previous_display_price) / previous_display_price) * 100
            if abs(daily_change_pct) > OUTLIER_DAILY_CHANGE_THRESHOLD_PCT and not is_known_event:
                is_outlier = True
                outlier_reason = "daily_change_exceeds_threshold"
        previous_display_price = display_price

        raw_candidates = [RawPriceCandidate(**candidate) for candidate in candidates_by_day.get(dt_value, [])]

        prices.append(
            PriceHistoryPoint(
                asof_dt=dt_value,
                asof_ts=row.get("asof_ts"),
                price=price_adjusted,
                currency=currency,
                price_base=price_base_adjusted,
                price_raw=price_value,
                price_base_raw=price_base,
                price_adjusted=price_adjusted,
                price_base_adjusted=price_base_adjusted,
                is_known_event=is_known_event,
                known_event_reason=known_event_reason,
                source=row.get("source"),
                venue=row.get("venue"),
                quality_score=_safe_int(row.get("quality_score")),
                daily_change_pct=daily_change_pct,
                is_outlier=is_outlier,
                outlier_reason=outlier_reason,
                raw_candidates=raw_candidates,
            )
        )

    has_adjustments = any(
        point.price_adjusted != point.price_raw
        or point.price_base_adjusted != point.price_base_raw
        for point in prices
    )
    response = PriceHistoryResponse(
        base_currency=base_ccy,
        window_days=window_days,
        points=len(prices),
        missing_fx=missing_fx,
        has_adjustments=has_adjustments,
        default_series="adjusted" if has_adjustments else "raw",
        prices=prices,
    )
    _set_cached(cache_key, response)
    return response
