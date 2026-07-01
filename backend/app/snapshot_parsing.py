from __future__ import annotations

from datetime import date, datetime
from typing import Optional

import pandas as pd


def safe_float(value) -> Optional[float]:
    if value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return None if pd.isna(result) else result


def safe_int(value) -> Optional[int]:
    if value is None:
        return None
    try:
        result = int(value)
    except (TypeError, ValueError):
        return None
    return None if pd.isna(result) else result


def parse_snapshot_date(value, *, error_message: str = "Invalid date value.") -> date:
    if isinstance(value, date):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        return date.fromisoformat(value.split(" ")[0])
    raise ValueError(error_message)


def parse_snapshot_datetime(value, *, error_message: str = "Invalid datetime value.") -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    raise ValueError(error_message)


def parse_optional_datetime(value) -> Optional[datetime]:
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
