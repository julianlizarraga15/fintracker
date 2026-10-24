from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
import requests

from backend.core import ppi_client


@pytest.mark.parametrize("sandbox", [False, True])
def test_sdk_requests_have_bounded_timeouts(monkeypatch, sandbox):
    from ppi_client.api.constants import (
        ACCOUNT_ACCOUNTS,
        ACCOUNT_BALANCE_POSITIONS,
        ACCOUNT_LOGIN_API,
    )

    positions = {"groupedAvailability": [], "groupedInstruments": []}
    payloads = [
        {"accessToken": "synthetic-token", "refreshToken": "synthetic-refresh"},
        [{"accountNumber": "synthetic-account"}],
        positions,
    ]
    calls = []

    def send(session, request, **kwargs):
        calls.append((request, kwargs))
        response = requests.Response()
        response.status_code = 200
        response._content = json.dumps(payloads[len(calls) - 1]).encode()
        return response

    monkeypatch.setattr(requests.Session, "send", send)

    ppi = ppi_client.login("synthetic-public", "synthetic-private", sandbox=sandbox)
    account_number = ppi_client.resolve_account_number(ppi)
    assert ppi_client.get_balance_and_positions(ppi, account_number) == positions

    host = "clientapisandbox" if sandbox else "clientapi"
    base_url = f"https://{host}.portfoliopersonal.com/api/"
    assert [(request.method, request.url) for request, _ in calls] == [
        ("POST", base_url + ACCOUNT_LOGIN_API),
        ("GET", base_url + ACCOUNT_ACCOUNTS),
        ("GET", base_url + ACCOUNT_BALANCE_POSITIONS.format(account_number)),
    ]
    assert [options["timeout"] for _, options in calls] == [(5, 30)] * 3
    assert calls[1][0].headers["Authorization"] == "Bearer synthetic-token"


@pytest.mark.parametrize(
    "payload",
    [
        {"accessToken": "synthetic-secret"},
        {},
        {"groupedAvailability": None},
        {"groupedInstruments": {}},
        {"groupedAvailability": [], "groupedInstruments": "synthetic-secret"},
    ],
)
def test_positions_reject_malformed_payload_without_echoing_it(payload):
    account = SimpleNamespace(get_balance_and_positions=lambda _: payload)
    with pytest.raises(ppi_client.PPIAPIError) as error:
        ppi_client.get_balance_and_positions(SimpleNamespace(account=account), "synthetic")
    assert "synthetic-secret" not in str(error.value)


@pytest.mark.parametrize(
    "payload",
    [
        {"groupedAvailability": []},
        {"groupedInstruments": []},
        {"groupedAvailability": [], "groupedInstruments": []},
    ],
)
def test_positions_accept_explicit_empty_portfolios(payload):
    account = SimpleNamespace(get_balance_and_positions=lambda _: payload)
    assert ppi_client.get_balance_and_positions(
        SimpleNamespace(account=account), "synthetic"
    ) == payload
