from __future__ import annotations

import json

from pydantic import ValidationError

from backend.core.broker_contract import Broker, BrokerFailure, BrokerSnapshot, MAX_SOURCE_PAYLOAD_BYTES, POSITION_COLUMNS
from backend.core.sources.models import SourcePositions

LAMBDA_READ_TIMEOUT_SECONDS = 910
LAMBDA_CONNECT_TIMEOUT_SECONDS = 10


class BrokerSourceError(RuntimeError):
    pass


def invoke_json(function_name: str, region: str, *, client=None):
    if not function_name:
        raise BrokerSourceError("Broker function is not configured")
    try:
        if client is None:
            import boto3
            from botocore.config import Config

            client = boto3.client("lambda", region_name=region, config=Config(
                connect_timeout=LAMBDA_CONNECT_TIMEOUT_SECONDS,
                read_timeout=LAMBDA_READ_TIMEOUT_SECONDS,
                retries={"max_attempts": 0},
            ))
        response = client.invoke(FunctionName=function_name, InvocationType="RequestResponse", Payload=b"{}")
        stream = response.get("Payload")
        try:
            if response.get("StatusCode") != 200 or response.get("FunctionError"):
                raise BrokerSourceError("Broker function failed")
            if stream is None:
                raise BrokerSourceError("Broker function returned no data")
            payload = stream.read(MAX_SOURCE_PAYLOAD_BYTES + 1)
        finally:
            if stream is not None:
                stream.close()
        if len(payload) > MAX_SOURCE_PAYLOAD_BYTES:
            raise BrokerSourceError("Broker response exceeds the size limit")
        return json.loads(payload)
    except BrokerSourceError:
        raise
    except Exception:
        # AWS and JSON errors may contain request or response data.
        raise BrokerSourceError("Broker invocation failed") from None


def fetch_broker_positions(source: Broker, function_name: str, region: str, account_id: str, *, client=None) -> SourcePositions:
    import pandas as pd

    payload = invoke_json(function_name, region, client=client)
    try:
        if isinstance(payload, dict) and payload.get("status") == "error":
            failure = BrokerFailure.model_validate(payload)
            if failure.source != source:
                raise ValueError("Source mismatch")
            raise BrokerSourceError(f"{source.upper()} source unavailable ({failure.error_code})")
        snapshot = BrokerSnapshot.model_validate(payload)
        if snapshot.source != source or any(row.account_id != account_id for row in snapshot.positions):
            raise ValueError("Source or account mismatch")
    except BrokerSourceError:
        raise
    except (ValidationError, ValueError, TypeError):
        raise BrokerSourceError(f"{source.upper()} returned invalid source data") from None
    return SourcePositions(
        df=pd.DataFrame([row.model_dump() for row in snapshot.positions], columns=POSITION_COLUMNS),
        prices=list(snapshot.prices),
    )
