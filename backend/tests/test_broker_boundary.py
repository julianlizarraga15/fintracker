from __future__ import annotations

import io
import json
import logging
import os
import subprocess
import sys
from datetime import date

import pandas as pd
import pytest

from backend.core.broker_contract import POSITION_COLUMNS
from backend.core.broker_lambda import BrokerSourceError, fetch_broker_positions
from backend.core.sources.models import SourcePositions
from backend.valuation.models import Price
from backend.workers.runtime import run_broker


def source_result():
    return SourcePositions(
        df=pd.DataFrame([{
            "symbol": "AL30", "description": "Bond", "instrument_type": "bond",
            "market": "argentina", "source": "iol", "account_id": "acct-1",
            "currency": "USD", "quantity": 2, "price": float("nan"), "valuation": None,
        }], columns=POSITION_COLUMNS),
        prices=[Price(asof_dt=date(2026, 10, 24), symbol="AL30", price_type="last",
                      price=10, currency="USD", source="iol")],
    )


class LambdaClient:
    def __init__(self, payload, **metadata):
        self.payload = payload
        self.metadata = metadata
        self.calls = []

    def invoke(self, **kwargs):
        self.calls.append(kwargs)
        return {"StatusCode": 200, "Payload": io.BytesIO(json.dumps(self.payload).encode()), **self.metadata}


def test_worker_roundtrip_preserves_positions_and_prices_without_auth_material():
    response = run_broker({}, "iol", source_result)
    assert response["status"] == "ok"
    assert response["positions"][0]["price"] is None
    client = LambdaClient(response)
    result = fetch_broker_positions("iol", "fixed-function", "us-east-1", "acct-1", client=client)
    assert result.df.iloc[0]["quantity"] == 2
    assert result.prices[0].price == 10
    assert client.calls == [{"FunctionName": "fixed-function", "InvocationType": "RequestResponse", "Payload": b"{}"}]
    assert not hasattr(result, "access_token")
    assert not hasattr(result, "raw_items")


def test_worker_suppresses_dependency_output_and_returns_fixed_error(capsys, caplog):
    secret = "synthetic-private-credential"

    def broken_source():
        print(secret)
        logging.getLogger("broker-sdk").error(secret)
        raise RuntimeError(secret)

    response = run_broker({}, "iol", broken_source)
    assert response == {"schema_version": 1, "source": "iol", "status": "error", "error_code": "source_unavailable"}
    assert secret not in json.dumps(response)
    assert secret not in capsys.readouterr().out
    assert secret not in caplog.text


def test_worker_rejects_caller_overrides_before_fetching():
    called = []
    response = run_broker({"url": "https://attacker.invalid", "account": "another"}, "iol", lambda: called.append(True))
    assert response["error_code"] == "invalid_request"
    assert not called


@pytest.mark.parametrize("corruption", ["function_error", "unknown_field", "wrong_account", "wrong_source", "nonfinite", "error"])
def test_invocation_rejects_failure_and_invalid_wire_data_without_echoing(corruption):
    secret = "synthetic-sensitive-response"
    response = run_broker({}, "iol", source_result)
    metadata = {}
    if corruption == "function_error":
        response = {"errorMessage": secret}
        metadata["FunctionError"] = "Unhandled"
    elif corruption == "unknown_field":
        response["access_token"] = secret
    elif corruption == "wrong_account":
        response["positions"][0]["account_id"] = "other-account"
    elif corruption == "wrong_source":
        response["positions"][0]["source"] = "ppi"
    elif corruption == "nonfinite":
        response["positions"][0]["quantity"] = float("inf")
    else:
        response = {"schema_version": 1, "source": "iol", "status": "error", "error_code": "source_unavailable"}
    with pytest.raises(BrokerSourceError) as exc:
        fetch_broker_positions("iol", "fixed-function", "us-east-1", "acct-1", client=LambdaClient(response, **metadata))
    assert secret not in str(exc.value)


def test_worker_rejects_unexpected_position_fields_instead_of_serializing_them():
    def unexpected():
        result = source_result()
        result.df["access_token"] = "synthetic-token"
        return result

    response = run_broker({}, "iol", unexpected)
    assert response["status"] == "error"
    assert "synthetic-token" not in json.dumps(response)


def test_worker_imports_do_not_initialize_web_settings_or_environment_files():
    environment = dict(os.environ, SSM_ENV_PATH="/fintracker/prod", IOL_PASSWORD="synthetic-credential")
    environment.pop("ACCOUNT_EMAIL", None)
    result = subprocess.run([sys.executable, "-c", """
import sys
import backend.workers.iol
import backend.workers.ppi
assert 'backend.core.config' not in sys.modules
assert 'backend.core.settings' not in sys.modules
assert 'dotenv' not in sys.modules
"""], env=environment, capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("parameter", [None, {"Type": "String", "Value": "synthetic-secret"}, {"Type": "SecureString", "Value": ""}])
def test_worker_rejects_missing_or_nonsecure_credentials(monkeypatch, parameter):
    import boto3
    from backend.workers.runtime import load_parameters

    class SSM:
        def get_parameters(self, **kwargs):
            assert kwargs == {"Names": ["/fixed/credential"], "WithDecryption": True}
            return {"Parameters": [] if parameter is None else [{"Name": "/fixed/credential", **parameter}]}

    monkeypatch.setattr(boto3, "client", lambda _: SSM())
    response = run_broker({}, "iol", lambda: load_parameters({"key": "/fixed/credential"}))
    assert response["error_code"] == "source_unavailable"
    assert "synthetic-secret" not in json.dumps(response)
