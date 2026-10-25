from datetime import date
import json

import pytest
import requests

from backend.core import fx_rates


@pytest.mark.parametrize("payload,rate,asof", [
    ({"venta": "1.234,50", "fechaActualizacion": "2026-10-24T12:00:00Z"}, 1234.5, date(2026, 10, 24)),
    ({"sell": 2.5, "last_update": "2026-10-23T12:00:00+00:00"}, 2.5, date(2026, 10, 23)),
    ({"value": "3.5", "fechaActualizacion": "invalid"}, 3.5, date.today()),
])
def test_public_fx_response_preserves_rate_currency_source_and_date(monkeypatch, payload, rate, asof):
    response = requests.Response()
    response.status_code = 200
    response._content = json.dumps(payload).encode()
    monkeypatch.setattr(fx_rates.requests, "get", lambda *args, **kwargs: response)

    result = fx_rates.get_fx_rates()

    assert [row.model_dump() for row in result] == [{
        "asof_dt": asof, "from_ccy": "USD", "to_ccy": "ARS",
        "rate": rate, "source": "dolarapi_blue_venta", "max_age_days": 3,
    }]


@pytest.mark.parametrize("status,body", [
    (503, '{"venta": 2}'),
    (200, "not JSON"),
    (200, "[]"),
    (200, "{}"),
    (200, '{"venta": 0}'),
    (200, '{"venta": -2}'),
])
def test_public_fx_unavailable_or_invalid_response_returns_no_rates(monkeypatch, status, body):
    response = requests.Response()
    response.status_code = status
    response._content = body.encode()
    monkeypatch.setattr(fx_rates.requests, "get", lambda *args, **kwargs: response)

    assert fx_rates.get_fx_rates() == []


def test_public_fx_transport_failure_returns_no_rates(monkeypatch):
    def timeout(*args, **kwargs):
        raise requests.Timeout("synthetic timeout")

    monkeypatch.setattr(fx_rates.requests, "get", timeout)

    assert fx_rates.get_fx_rates() == []
