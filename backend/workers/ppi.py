from __future__ import annotations

import os
from typing import Any

from backend.core import ppi_client, ppi_transform
from backend.core.broker_contract import POSITION_COLUMNS
from backend.core.sources.models import SourcePositions
from backend.workers.runtime import load_parameters, run_broker


def _collect() -> SourcePositions:
    account_id = os.environ["ACCOUNT_ID"]
    credentials = load_parameters(
        {
            "public_key": os.environ["PPI_PUBLIC_KEY_PARAMETER"],
            "private_key": os.environ["PPI_PRIVATE_KEY_PARAMETER"],
        }
    )
    ppi = ppi_client.login(
        public_api_key=credentials["public_key"],
        private_api_key=credentials["private_key"],
        sandbox=False,
    )
    account_number = ppi_client.resolve_account_number(
        ppi, os.environ.get("PPI_ACCOUNT_NUMBER")
    )
    payload = ppi_client.get_balance_and_positions(ppi, account_number)
    positions = ppi_transform.positions_to_df(
        payload, account_id=account_id, position_columns=POSITION_COLUMNS
    )
    prices = ppi_transform.prices_from_positions(payload)
    return SourcePositions(df=positions, prices=prices)


def handler(event: Any, context: Any) -> dict[str, Any]:
    return run_broker(event, "ppi", _collect)
