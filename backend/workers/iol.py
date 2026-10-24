import os

from backend.core import iol_client, iol_transform
from backend.core.broker_contract import POSITION_COLUMNS
from backend.core.sources import iol as iol_source
from backend.core.sources.models import SourcePositions
from backend.workers.runtime import load_parameters, run_broker


def _collect() -> SourcePositions:
    account_id = os.environ["ACCOUNT_ID"]
    credentials = load_parameters({
        "username": os.environ["IOL_USERNAME_PARAMETER"],
        "password": os.environ["IOL_PASSWORD_PARAMETER"],
    })
    return iol_source.load_positions(
        username=credentials["username"],
        password=credentials["password"],
        account_id=account_id,
        position_columns=POSITION_COLUMNS,
        get_bearer_tokens=iol_client.get_bearer_tokens,
        get_positions=iol_client.get_positions,
        extract_positions_as_df=iol_transform.extract_positions_as_df,
        get_prices_for_positions=iol_client.get_prices_for_positions,
    )


def handler(event, context):
    return run_broker(event, "iol", _collect)
