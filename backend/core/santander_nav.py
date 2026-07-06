"""Helpers to fetch Santander Argentina mutual-fund NAV data."""

from __future__ import annotations

import uuid
from typing import Dict, Iterable, List, TypedDict

import requests

LANDING_URL = "https://www.santander.com.ar/personas/inversiones/informacion-fondos"
DETAIL_URL = "https://www.santander.com.ar/fondosInformacion/funds/{fund_id}/detail"

API_HEADERS = {
    "accept": "application/json, text/plain, */*",
    "origin": "https://www.santander.com.ar",
    "referer": LANDING_URL,
    "channel-name": "webpublic",
    "x-ibm-client-id": "6pXM5mL8Gz8hQKZAo7kpTxjVpuVtNcIl",
    "x-san-segment-id": "2",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
}

CONNECT_TIMEOUT_SECONDS = 5
READ_TIMEOUT_SECONDS = 30
REQUEST_TIMEOUT = (CONNECT_TIMEOUT_SECONDS, READ_TIMEOUT_SECONDS)


class FundShareValue(TypedDict):
    fund_id: str
    fund_name: str
    current_share_value: float
    current_share_value_date: str


def build_session() -> requests.Session:
    return requests.Session()


def _api_headers() -> Dict[str, str]:
    headers = dict(API_HEADERS)
    headers["x-san-correlationid"] = str(uuid.uuid4())
    return headers


def fetch_share_value(session: requests.Session, fund_id: str) -> FundShareValue:
    """Return share value data (including fund name) for the chosen fund."""
    url = DETAIL_URL.format(fund_id=fund_id)
    headers = _api_headers()
    try:
        resp = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
    except requests.Timeout as exc:
        try:
            resp = requests.get(url, headers=_api_headers(), timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
        except requests.RequestException as retry_exc:
            raise RuntimeError(
                f"Failed to fetch fund {fund_id}: session request timed out ({exc}); "
                f"direct retry failed: {retry_exc}"
            ) from retry_exc
    except requests.RequestException as exc:
        raise RuntimeError(f"Failed to fetch fund {fund_id}: {exc}") from exc

    payload = resp.json()
    data = payload.get("data") or {}
    share_value = data.get("currentShareValue")
    share_date = data.get("currentShareValueDate") or ""
    raw_name = data.get("name") or data.get("shortDescription") or ""
    fund_name = raw_name.strip() if isinstance(raw_name, str) else ""
    if share_value is None:
        raise RuntimeError(f"Fund {fund_id} missing share value in response: {payload}")
    return {
        "fund_id": fund_id,
        "fund_name": fund_name,
        "current_share_value": float(share_value),
        "current_share_value_date": share_date,
    }


def fetch_share_values(fund_ids: Iterable[str]) -> List[FundShareValue]:
    session = build_session()
    return [fetch_share_value(session, f) for f in fund_ids]
