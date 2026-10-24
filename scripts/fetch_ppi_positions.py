from __future__ import annotations

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from backend.core.broker_lambda import fetch_broker_positions
from backend.core.config import ACCOUNT_ID, BROKER_LAMBDA_REGION, PPI_SOURCE_LAMBDA


def main() -> int:
    result = fetch_broker_positions("ppi", PPI_SOURCE_LAMBDA, BROKER_LAMBDA_REGION, ACCOUNT_ID)
    positions_df = result.df
    prices = result.prices

    print(f"Positions loaded: {len(positions_df)}. Prices loaded from positions payload: {len(prices)}")

    if positions_df.empty:
        print("No positions returned.")
        return 0

    display_columns = [
        "symbol",
        "description",
        "instrument_type",
        "market",
        "currency",
        "quantity",
        "price",
        "valuation",
    ]
    printable_df = positions_df.reindex(columns=display_columns)
    with __import__("pandas").option_context(
        "display.max_columns",
        None,
        "display.width",
        160,
        "display.float_format",
        lambda val: f"{val:,.4f}",
    ):
        print(printable_df.to_string(index=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
