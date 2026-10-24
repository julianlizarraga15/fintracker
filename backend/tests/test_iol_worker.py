import json
import logging

import boto3
import pandas as pd
import pytest
import requests

from backend.core import iol_client, iol_transform
from backend.core.sources import iol as iol_source


SECRET = "synthetic-secret-never-return"


class Response:
    def __init__(self, payload, status_code=200):
        self.payload = payload
        self.status_code = status_code
        self.text = SECRET

    def json(self):
        return self.payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(SECRET)


def test_iol_transform_uses_explicit_worker_account():
    result = iol_transform.extract_positions_as_df(
        [{"simbolo": "AL30", "cantidad": 2, "precio": 100,
          "tipo": "bond", "moneda": "ARS", "_market": "argentina"}],
        account_id="worker-account",
    )
    assert result.iloc[0]["account_id"] == "worker-account"
    assert result.iloc[0]["valuation"] == 200


def test_iol_source_returns_prices_and_preserves_flat_fallback():
    columns = ["symbol", "description", "instrument_type", "market", "source",
               "account_id", "currency", "quantity", "price", "valuation"]
    items = [{"simbolo": "AL30", "cantidad": "2", "ultimoPrecio": "101.5",
              "descripcion": "Bond", "tipoInstrumento": "bond", "mercado": "BCBA"}]
    price_calls = []
    result = iol_source.load_positions(
        username="user", password="pass", account_id="worker-account",
        position_columns=columns, get_bearer_tokens=lambda *_: (SECRET, SECRET),
        get_positions=lambda _: items,
        extract_positions_as_df=lambda *_args, **_kwargs: pd.DataFrame(),
        get_prices_for_positions=lambda raw, token: price_calls.append((raw, token)) or [],
    )
    assert price_calls == [(items, SECRET)]
    assert not hasattr(result, "access_token")
    assert not hasattr(result, "raw_items")
    assert list(result.df.columns) == columns
    row = result.df.iloc[0]
    assert row["symbol"] == "AL30"
    assert row["quantity"] == 2
    assert row["price"] == 101.5
    assert row["account_id"] == "worker-account"
    assert row["market"] == "BCBA"
    assert row["currency"] is None
    assert pd.isna(row["valuation"])


def test_iol_successful_empty_account_is_not_a_request_failure(monkeypatch):
    monkeypatch.setattr(iol_client.requests, "get", lambda *_args, **_kwargs: Response({"activos": []}))
    assert iol_client.get_positions(SECRET) == []


def test_iol_unrecognized_success_payload_is_a_request_failure(monkeypatch):
    monkeypatch.setattr(iol_client.requests, "get", lambda *_args, **_kwargs: Response({"access_token": SECRET}))
    with pytest.raises(RuntimeError, match="Unable to fetch positions from IOL"):
        iol_client.get_positions(SECRET)


def test_iol_preserves_successful_market_when_the_other_market_fails(monkeypatch):
    def get(_url, **kwargs):
        if kwargs["params"]["pais"] == "argentina":
            raise requests.RequestException(SECRET)
        return Response({"tenencias": [{"simbolo": "AAPL", "cantidad": 1}]})

    monkeypatch.setattr(iol_client.requests, "get", get)
    assert iol_client.get_positions(SECRET) == [{
        "simbolo": "AAPL", "cantidad": 1, "_market": "estados_unidos",
    }]


def test_iol_authenticated_failures_never_log_response_or_exception(monkeypatch, caplog):
    caplog.set_level(logging.DEBUG)
    monkeypatch.setattr(iol_client.requests, "get", lambda *_args, **_kwargs: Response({}, 401))
    with pytest.raises(RuntimeError) as failure:
        iol_client.get_positions(SECRET)
    assert SECRET not in str(failure.value)
    assert iol_client.get_price_for_symbol("AL30", "argentina", "RentaFija", SECRET) is None

    def network_failure(*_args, **_kwargs):
        raise requests.RequestException(SECRET)

    monkeypatch.setattr(iol_client.requests, "get", network_failure)
    assert iol_client.get_price_for_symbol("AL30", "argentina", "RentaFija", SECRET) is None
    assert iol_client.get_prices_for_positions([
        {"simbolo": "BAD", "precio": -1, "authentication": SECRET},
    ], SECRET) == []
    assert SECRET not in caplog.text


def test_iol_worker_fetches_both_markets_and_quotes_with_internal_credentials(monkeypatch, caplog, capsys):
    from backend.workers import iol as worker

    monkeypatch.setenv("ACCOUNT_ID", "worker-account")
    monkeypatch.setenv("IOL_USERNAME_PARAMETER", "/workers/iol/username")
    monkeypatch.setenv("IOL_PASSWORD_PARAMETER", "/workers/iol/password")
    parameter_requests = []

    class SSM:
        def get_parameters(self, **kwargs):
            parameter_requests.append(kwargs)
            return {"Parameters": [{"Name": name, "Value": SECRET, "Type": "SecureString"} for name in kwargs["Names"]]}

    monkeypatch.setattr(boto3, "client", lambda service, **_kwargs: SSM() if service == "ssm" else pytest.fail(service))
    requests_seen = []

    def post(url, **kwargs):
        assert url == iol_client.BASE_URL + "/token"
        assert kwargs["data"] == {"username": SECRET, "password": SECRET, "grant_type": "password"}
        assert kwargs["timeout"] > 0
        return Response({"access_token": SECRET, "refresh_token": SECRET})

    def get(url, **kwargs):
        assert kwargs["headers"] == {"Authorization": "Bearer " + SECRET}
        assert kwargs["timeout"] > 0
        requests_seen.append((url, kwargs.get("params")))
        if url.endswith("/portafolio"):
            market = kwargs["params"]["pais"]
            if market == "argentina":
                return Response({"activos": [{"titulo": {"simbolo": "AL30", "tipo": "bond", "moneda": "ARS"}, "cantidad": 2}]})
            return Response({"tenencias": [{"simbolo": "AAPL", "tipo": "equity", "moneda": "USD", "cantidad": 1, "precio": 200}]})
        assert url.endswith("/Cotizaciones/argentina/AL30/RentaFija")
        return Response({"cotizacion": {"ultimoPrecio": 100, "moneda": "ARS"}})

    monkeypatch.setattr(iol_client.requests, "post", post)
    monkeypatch.setattr(iol_client.requests, "get", get)
    result = worker.handler({}, None)
    assert result["source"] == "iol"
    assert {row["symbol"] for row in result["positions"]} == {"AL30", "AAPL"}
    assert all(row["account_id"] == "worker-account" for row in result["positions"])
    assert {row["symbol"]: row["price"] for row in result["prices"]} == {"AL30": 100, "AAPL": 200}
    assert [params["pais"] for _, params in requests_seen if params] == ["argentina", "estados_unidos"]
    assert parameter_requests == [{"Names": ["/workers/iol/username", "/workers/iol/password"], "WithDecryption": True}]
    assert SECRET not in json.dumps(result)
    assert SECRET not in caplog.text + capsys.readouterr().out


def test_iol_worker_sanitizes_authentication_failure(monkeypatch, caplog, capsys):
    from backend.workers import iol as worker

    monkeypatch.setenv("ACCOUNT_ID", "worker-account")
    monkeypatch.setenv("IOL_USERNAME_PARAMETER", "/workers/iol/username")
    monkeypatch.setenv("IOL_PASSWORD_PARAMETER", "/workers/iol/password")
    monkeypatch.setattr(worker, "load_parameters", lambda _: {"username": SECRET, "password": SECRET})
    monkeypatch.setattr(iol_client.requests, "post", lambda *_args, **_kwargs: Response({"password": SECRET}, 401))
    result = worker.handler({}, None)
    assert result == {"schema_version": 1, "source": "iol", "status": "error", "error_code": "source_unavailable"}
    captured = capsys.readouterr()
    assert SECRET not in json.dumps(result) + caplog.text + captured.out + captured.err


def test_iol_worker_rejects_request_overrides_before_reading_credentials(monkeypatch):
    from backend.workers import iol as worker

    monkeypatch.setattr(worker, "load_parameters", lambda _: pytest.fail("invalid request read credentials"))
    result = worker.handler({"account_id": "caller-account"}, None)
    assert result == {"schema_version": 1, "source": "iol", "status": "error", "error_code": "invalid_request"}
