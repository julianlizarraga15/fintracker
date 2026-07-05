from __future__ import annotations

from typing import Any, Callable

import pandas as pd

from .models import SourcePositions


def load_ethereum_positions(
    *,
    enabled: bool,
    addresses_csv: str,
    position_columns: list[str],
    get_ethereum_balances: Callable[..., list[dict[str, Any]]],
    ethereum_balances_to_df: Callable[..., pd.DataFrame],
) -> SourcePositions:
    if not enabled:
        return SourcePositions(df=pd.DataFrame(columns=position_columns))
    addresses = [a.strip() for a in addresses_csv.split(",") if a.strip()]
    if not addresses:
        print("ENABLE_ETHEREUM set but ETHEREUM_WALLET_ADDRESSES is empty.")
        return SourcePositions(df=pd.DataFrame(columns=position_columns))
    eth_balances = get_ethereum_balances(addresses)
    eth_df = (
        ethereum_balances_to_df(eth_balances, position_columns=position_columns)
        if eth_balances
        else pd.DataFrame(columns=position_columns)
    )
    symbols = set(eth_df["symbol"]) if not eth_df.empty else set()
    if symbols:
        print(f"Loaded {len(symbols)} Ethereum assets.")
    return SourcePositions(df=eth_df, symbols=symbols)
