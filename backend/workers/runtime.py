from __future__ import annotations

from contextlib import redirect_stderr, redirect_stdout
import io
import logging
from typing import Callable

from backend.core.broker_contract import Broker, BrokerFailure, MAX_SOURCE_PAYLOAD_BYTES, broker_success
from backend.core.sources.models import SourcePositions


class _DiscardOutput(io.TextIOBase):
    def write(self, text):
        return len(text)


def load_parameters(names: dict[str, str]) -> dict[str, str]:
    import boto3

    if not names or any(not name.startswith("/") for name in names.values()):
        raise ValueError("Broker parameter names are not configured")
    response = boto3.client("ssm").get_parameters(Names=list(names.values()), WithDecryption=True)
    parameters = {item["Name"]: item for item in response.get("Parameters", [])}
    if response.get("InvalidParameters") or any(
        name not in parameters or parameters[name].get("Type") != "SecureString" or not parameters[name].get("Value")
        for name in names.values()
    ):
        raise ValueError("Broker credentials unavailable")
    return {label: parameters[name]["Value"] for label, name in names.items()}


def run_broker(event, source: Broker, collect: Callable[[], SourcePositions]) -> dict:
    if type(event) is not dict or event:
        return BrokerFailure(source=source, error_code="invalid_request").model_dump()
    disabled_before = logging.root.manager.disable
    sink = _DiscardOutput()
    try:
        # Invoke callers can request LogType=Tail, so dependencies cannot log raw data.
        logging.disable(logging.CRITICAL)
        with redirect_stdout(sink), redirect_stderr(sink):
            try:
                result = collect()
            except Exception:
                return BrokerFailure(source=source, error_code="source_unavailable").model_dump()
            try:
                response = broker_success(source, result.df.to_dict(orient="records"), result.prices)
                if len(response.model_dump_json().encode()) > MAX_SOURCE_PAYLOAD_BYTES:
                    raise ValueError("Broker response too large")
                return response.model_dump(mode="json")
            except Exception:
                return BrokerFailure(source=source, error_code="invalid_response").model_dump()
    finally:
        logging.disable(disabled_before)
