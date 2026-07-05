from __future__ import annotations

from typing import Callable, Any

import pandas as pd

from .models import SourcePositions


def load_positions(
    *,
    enabled: bool,
    position_columns: list[str],
    login_ppi: Callable[[], Any],
    resolve_ppi_account_number: Callable[[Any], str],
    get_ppi_balance_and_positions: Callable[[Any, str], Any],
    ppi_positions_to_df: Callable[..., pd.DataFrame],
    ppi_prices_from_positions: Callable[[Any], list[Any]],
    log_job_detail: Callable[[dict[str, Any]], None],
) -> SourcePositions:
    if not enabled:
        return SourcePositions(df=pd.DataFrame(columns=position_columns))
    ppi = login_ppi()
    ppi_account_number = resolve_ppi_account_number(ppi)
    ppi_payload = get_ppi_balance_and_positions(ppi, ppi_account_number)
    ppi_df = ppi_positions_to_df(ppi_payload, position_columns=position_columns)
    ppi_price_models = ppi_prices_from_positions(ppi_payload)
    print(f"Loaded {len(ppi_df)} PPI positions.")
    print(f"Priced {len(ppi_price_models)} PPI positions from portfolio payload.")
    log_job_detail(
        {
            "event": "source_counts",
            "source": "ppi",
            "loaded": len(ppi_df),
            "priced": len(ppi_price_models),
            "missing": max(len(ppi_df) - len(ppi_price_models), 0),
        }
    )
    return SourcePositions(df=ppi_df, prices=ppi_price_models)
