from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from backend.valuation.models import Price


@dataclass
class SourcePositions:
    """Positions and metadata emitted by one daily snapshot source."""

    df: pd.DataFrame
    raw_items: list[dict[str, Any]] = field(default_factory=list)
    access_token: str | None = None
    prices: list[Price] = field(default_factory=list)
    symbols: set[str] = field(default_factory=set)
