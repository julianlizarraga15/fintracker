from __future__ import annotations

import pandas as pd
from typing import Any, Dict, List, Optional
from backend.core.config import ACCOUNT_ID

def ethereum_balances_to_df(balances: List[Dict[str, Any]], position_columns: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Convert normalized Ethereum balances to a DataFrame compatible with the daily snapshot.
    """
    if not balances:
        return pd.DataFrame(columns=position_columns) if position_columns else pd.DataFrame()

    rows = []
    for b in balances:
        rows.append({
            "account_id": ACCOUNT_ID,
            "source": b["source"],
            "market": b["market"],
            "symbol": b["symbol"],
            "quantity": b["quantity"],
            "currency": "USD",  # Default for crypto prices in this system
            "instrument_type": "crypto",
            "display_name": f"{b['symbol']} (MetaMask)",
        })

    df = pd.DataFrame(rows)
    if position_columns:
        # Ensure all required columns exist, fill with None if missing
        for col in position_columns:
            if col not in df.columns:
                df[col] = None
        df = df.reindex(columns=position_columns)
        
    return df
