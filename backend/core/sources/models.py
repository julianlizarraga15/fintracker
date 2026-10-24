from __future__ import annotations

from dataclasses import dataclass, field
import pandas as pd

from backend.valuation.models import Price


@dataclass
class SourcePositions:
    """Positions and metadata emitted by one daily snapshot source."""

    df: pd.DataFrame
    prices: list[Price] = field(default_factory=list)
    symbols: set[str] = field(default_factory=set)
