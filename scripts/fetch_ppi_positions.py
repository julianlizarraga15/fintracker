from __future__ import annotations

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from backend.core.ppi_client import (
    get_accounts,
    get_balance_and_positions,
    login,
    resolve_account_number,
)
from backend.core.ppi_transform import positions_to_df, prices_from_positions


def _mask_account(value: str) -> str:
    if len(value) <= 4:
        return "****"
    return f"{'*' * max(len(value) - 4, 0)}{value[-4:]}"


def main() -> int:
    ppi = login()
    accounts = get_accounts(ppi)
    account_number = resolve_account_number(ppi)
    payload = get_balance_and_positions(ppi, account_number)

    positions_df = positions_to_df(payload)
    prices = prices_from_positions(payload)

    print(f"PPI login ok. Accounts available: {len(accounts)}. Selected account: {_mask_account(account_number)}")
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
