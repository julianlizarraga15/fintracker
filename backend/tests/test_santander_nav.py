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
    assert session.calls[0]["timeout"] == santander_nav.REQUEST_TIMEOUT
    assert session.headers == {}


def test_fetch_share_value_retries_direct_request_after_session_timeout(monkeypatch):
    direct_calls = []

    class TimingOutSession:
        def get(self, url, headers=None, timeout=None):
            raise santander_nav.requests.ReadTimeout("session timed out")

    def fake_get(url, headers=None, timeout=None):
        direct_calls.append({"url": url, "headers": headers, "timeout": timeout})
        return DummyResponse(
            {
                "data": {
                    "shortDescription": "Direct Fund",
                    "currentShareValue": 5,
                    "currentShareValueDate": "2026-07-03",
                }
            }
        )

    monkeypatch.setattr(santander_nav.requests, "get", fake_get)

    result = santander_nav.fetch_share_value(TimingOutSession(), "1")

    assert result["fund_name"] == "Direct Fund"
    assert result["current_share_value"] == 5.0
    assert [call["url"] for call in direct_calls] == [
        santander_nav.DETAIL_URL.format(fund_id="1")
    ]
    assert all(call["url"] != santander_nav.LANDING_URL for call in direct_calls)
    assert direct_calls[0]["headers"]["channel-name"] == "webpublic"
    assert direct_calls[0]["timeout"] == santander_nav.REQUEST_TIMEOUT


def test_fetch_share_value_wraps_request_errors_with_fund_id():
    class FailingSession:
        def get(self, url, headers=None, timeout=None):
            raise santander_nav.requests.ConnectionError("connection failed")

    with pytest.raises(RuntimeError, match="Failed to fetch fund 99: connection failed"):
        santander_nav.fetch_share_value(FailingSession(), "99")


def test_fetch_share_value_reports_timeout_retry_failure(monkeypatch):
    class TimingOutSession:
        def get(self, url, headers=None, timeout=None):
            raise santander_nav.requests.ReadTimeout("session timed out")

    def fake_get(url, headers=None, timeout=None):
        raise santander_nav.requests.ReadTimeout("direct timed out")

    monkeypatch.setattr(santander_nav.requests, "get", fake_get)

    with pytest.raises(
        RuntimeError,
        match=(
            "Failed to fetch fund 99: session request timed out "
            r"\(session timed out\); direct retry failed: direct timed out"
        ),
    ):
        santander_nav.fetch_share_value(TimingOutSession(), "99")
