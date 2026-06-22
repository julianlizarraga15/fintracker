from __future__ import annotations

from datetime import date
from typing import Optional

from pydantic import BaseModel


class CorporateAction(BaseModel):
    symbol: str
    effective_date: date
    kind: str
    old_ratio: str
    new_ratio: str
    description: Optional[str] = None


CORPORATE_ACTIONS_BY_SYMBOL: dict[str, list[CorporateAction]] = {
    "SPY": [
        CorporateAction(
            symbol="SPY",
            effective_date=date(2026, 6, 1),
            kind="cedear_ratio_change",
            old_ratio="20:1",
            new_ratio="60:1",
            description="SPY CEDEAR ratio changed from 20:1 to 60:1",
        )
    ]
}


def get_corporate_actions(symbol: str) -> list[CorporateAction]:
    return CORPORATE_ACTIONS_BY_SYMBOL.get(symbol.upper(), [])


def parse_cedear_ratio(ratio: str) -> Optional[float]:
    """Return the CEDEAR count in a '<cedears>:<underlying>' ratio string."""
    try:
        cedears, underlying = ratio.split(":", maxsplit=1)
        underlying_value = float(underlying)
        if underlying_value == 0:
            return None
        return float(cedears) / underlying_value
    except (AttributeError, ValueError):
        return None
