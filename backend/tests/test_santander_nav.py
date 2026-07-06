from __future__ import annotations

import pytest

from backend.core import santander_nav


class DummyResponse:
    def __init__(self, json_data):
        self._json = json_data

    def raise_for_status(self):
        return None

    def json(self):
        return self._json


class DummySession:
    def __init__(self):
        self.headers = {}
        self.calls = []

    def get(self, url, headers=None, timeout=None):
        self.calls.append({"url": url, "headers": headers, "timeout": timeout})
        return DummyResponse(
            {
                "data": {
                    "name": "Super Fondo",
                    "currentShareValue": "123.45",
                    "currentShareValueDate": "2026-07-03",
                }
            }
        )


def test_fetch_share_value_uses_detail_endpoint_without_landing_page(monkeypatch):
    session = DummySession()
    monkeypatch.setattr(santander_nav.requests, "Session", lambda: session)

    result = santander_nav.fetch_share_value(santander_nav.build_session(), "1")

    assert result == {
        "fund_id": "1",
        "fund_name": "Super Fondo",
        "current_share_value": 123.45,
        "current_share_value_date": "2026-07-03",
    }
    assert [call["url"] for call in session.calls] == [
        santander_nav.DETAIL_URL.format(fund_id="1")
    ]
    assert all(call["url"] != santander_nav.LANDING_URL for call in session.calls)
    assert session.calls[0]["headers"]["channel-name"] == "webpublic"
    assert session.calls[0]["headers"]["referer"] == santander_nav.LANDING_URL
    assert session.headers == santander_nav.SESSION_HEADERS


def test_fetch_share_value_wraps_request_errors_with_fund_id():
    class FailingSession:
        def get(self, url, headers=None, timeout=None):
            raise santander_nav.requests.Timeout("timed out")

    with pytest.raises(RuntimeError, match="Failed to fetch fund 99: timed out"):
        santander_nav.fetch_share_value(FailingSession(), "99")
