#!/usr/bin/env python3
"""
Fetch Santander Argentina mutual-fund (valor de la cuotaparte) data via their
public SPA endpoint. Accepts one or more fund ids (e.g. `1`, `2`) and prints the
fund name, current share value, plus its timestamp.

Usage (CLI):
    python scripts/fetch_santander_nav.py 1 2

Usage (Google Colab / notebooks):
    from scripts.fetch_santander_nav import fetch_share_values
    fetch_share_values(["1", "2"])
"""

from __future__ import annotations

import argparse
import sys
from typing import Iterable, List

from backend.core.santander_nav import (
    FundShareValue,
    build_session,
    fetch_share_value,
)

ShareValue = FundShareValue


def fetch_share_values(fund_ids: Iterable[str]) -> List[ShareValue]:
    """Fetch share values for each fund id and return structured results."""
    session = build_session()
    return [fetch_share_value(session, fund_id) for fund_id in fund_ids]


def run(fund_ids: Iterable[str]) -> int:
    """CLI entry point that prints the fetched values."""
    session = build_session()
    exit_code = 0
    for fund_id in fund_ids:
        try:
            share_value = fetch_share_value(session, fund_id)
            label = share_value["fund_name"] or f"Fund {fund_id}"
            print(
                f"{label} (Fund {fund_id}): {share_value['current_share_value']:.6f} "
                f"(as of {share_value['current_share_value_date']})"
            )
        except RuntimeError as exc:
            exit_code = 1
            print(str(exc), file=sys.stderr)
    return exit_code


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch Santander fund share values.")
    parser.add_argument(
        "fund_ids",
        nargs="*",
        default=["1"],
        help="Fund ids from https://www.santander.com.ar/personas/inversiones/informacion-fondos#/detail/<id>",
    )
    return parser.parse_args()


def _running_in_colab() -> bool:
    return "google.colab" in sys.modules


def _prompt_fund_ids() -> Iterable[str]:
    raw = input("Enter fund ids separated by spaces (default: 1): ").strip()
    return raw.split() if raw else ["1"]


if __name__ == "__main__":
    if _running_in_colab() and len(sys.argv) == 1:
        fund_ids = _prompt_fund_ids()
    else:
        fund_ids = parse_args().fund_ids
    raise SystemExit(run(fund_ids))
