"""Helpers to fetch Santander Argentina mutual-fund NAV data."""

from __future__ import annotations

import uuid
from typing import Dict, Iterable, List, TypedDict

import requests

LANDING_URL = "https://www.santander.com.ar/personas/inversiones/informacion-fondos"
DETAIL_URL = "https://www.santander.com.ar/fondosInformacion/funds/{fund_id}/detail"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/142.0.0.0 Safari/537.36"
)
ACCEPT_LANGUAGE = "es-AR,es;q=0.9,en;q=0.8"
SEC_CH_UA = '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"'

SESSION_HEADERS = {
    "user-agent": USER_AGENT,
    "accept-language": ACCEPT_LANGUAGE,
    "sec-ch-ua": SEC_CH_UA,
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
}

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


class FundShareValue(TypedDict):
    fund_id: str
    fund_name: str
    current_share_value: float
    current_share_value_date: str


def _bootstrap_session(session: requests.Session) -> None:
    """Hit the landing page so Santander sets anti-bot cookies."""
    try:
        session.get(LANDING_URL, timeout=10)
    except requests.RequestException:
        pass  # harmless: subsequent JSON request will surface real error


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(SESSION_HEADERS)
    _bootstrap_session(session)
    return session


def _api_headers() -> Dict[str, str]:
    headers = dict(API_HEADERS)
    headers["x-san-correlationid"] = str(uuid.uuid4())
    return headers


def fetch_share_value(session: requests.Session, fund_id: str) -> FundShareValue:
    """Return share value data (including fund name) for the chosen fund."""
    url = DETAIL_URL.format(fund_id=fund_id)
    try:
        resp = session.get(url, headers=_api_headers(), timeout=10)
        resp.raise_for_status()
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

