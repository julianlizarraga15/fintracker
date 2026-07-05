from __future__ import annotations

from typing import Any, Callable

import pandas as pd

from .models import SourcePositions


def load_positions(
    *,
    enabled: bool,
    balance_lambda: str | None,
    api_key: str | None,
    api_secret: str | None,
    base_url: str,
    recv_window_ms: int,
    position_columns: list[str],
    fetch_from_lambda: Callable[[str], list[dict[str, Any]]],
    get_account_balances: Callable[..., list[dict[str, Any]]],
    balances_to_df: Callable[..., pd.DataFrame],
) -> SourcePositions:
    if not enabled:
        return SourcePositions(df=pd.DataFrame(columns=position_columns))
    binance_balances: list[dict[str, Any]] = []
    if balance_lambda:
        binance_balances = fetch_from_lambda(balance_lambda)
    elif api_key and api_secret:
        binance_balances = get_account_balances(
            api_key=api_key,
            api_secret=api_secret,
            base_url=base_url,
            recv_window_ms=recv_window_ms,
        )
    else:
        print("ENABLE_BINANCE set but missing BINANCE_API_KEY or BINANCE_API_SECRET.")
    if not binance_balances:
        return SourcePositions(df=pd.DataFrame(columns=position_columns))
    binance_df = balances_to_df(binance_balances, position_columns=position_columns)
    symbols = set(binance_df["symbol"]) if not binance_df.empty else set()
    if symbols:
        print(f"Loaded {len(symbols)} Binance assets.")
    return SourcePositions(df=binance_df, symbols=symbols)
