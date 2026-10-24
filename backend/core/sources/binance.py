from __future__ import annotations

from typing import Any, Callable

import pandas as pd

from .models import SourcePositions


def load_positions(
    *,
    enabled: bool,
    balance_lambda: str | None,
    position_columns: list[str],
    fetch_from_lambda: Callable[[str], list[dict[str, Any]]],
    balances_to_df: Callable[..., pd.DataFrame],
) -> SourcePositions:
    if not enabled:
        return SourcePositions(df=pd.DataFrame(columns=position_columns))
    if not balance_lambda:
        raise RuntimeError("Binance source function is not configured")
    binance_balances = fetch_from_lambda(balance_lambda)
    if not binance_balances:
        return SourcePositions(df=pd.DataFrame(columns=position_columns))
    binance_df = balances_to_df(binance_balances, position_columns=position_columns)
    symbols = set(binance_df["symbol"]) if not binance_df.empty else set()
    if symbols:
        print(f"Loaded {len(symbols)} Binance assets.")
    return SourcePositions(df=binance_df, symbols=symbols)
