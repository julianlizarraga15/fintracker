#!/usr/bin/env python3
"""
Fetch Santander Argentina mutual-fund (valor de la cuotaparte) data via their
public SPA endpoint. Accepts one or more fund ids (e.g. `1`, `2`) and prints the
current share value plus its timestamp.

Usage (CLI):
    python scripts/fetch_santander_nav.py 1 2

Usage (Google Colab / notebooks):
    from scripts.fetch_santander_nav import fetch_share_values
    fetch_share_values(["1", "2"])
"""

from __future__ import annotations

import argparse
import sys
import uuid
from typing import Iterable, List, Tuple, Dict, Union

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

# Minimal header set that the SPA sends before hitting the JSON endpoint.
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


def _bootstrap_session(session: requests.Session) -> None:
    """Hit the landing page so Santander sets anti-bot cookies."""
    try:
        session.get(LANDING_URL, timeout=10)
    except requests.RequestException:
        # Not fatal; if it fails the subsequent call will likely fail too,
        # but we let the detail request raise the real error.
        pass


def _build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(SESSION_HEADERS)
    _bootstrap_session(session)
    return session


def _api_headers() -> Dict[str, str]:
    headers = dict(API_HEADERS)
    headers["x-san-correlationid"] = str(uuid.uuid4())
    return headers


def fetch_share_value(session: requests.Session, fund_id: str) -> Tuple[float, str]:
    """Return (share_value, share_value_date) for the chosen fund."""
    url = DETAIL_URL.format(fund_id=fund_id)
    try:
        resp = session.get(url, headers=_api_headers(), timeout=10)
        resp.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(f"Failed to fetch fund {fund_id}: {exc}") from exc

    payload = resp.json()
    data = payload.get("data") or {}
    share_value = data.get("currentShareValue")
    share_date = data.get("currentShareValueDate")
    if share_value is None:
        raise RuntimeError(f"Fund {fund_id} missing share value in response: {payload}")
    return float(share_value), share_date


ShareValue = Dict[str, Union[str, float]]


def fetch_share_values(fund_ids: Iterable[str]) -> List[ShareValue]:
    """Fetch share values for each fund id and return structured results."""
    session = _build_session()

    results: List[Dict[str, str]] = []
    for fund_id in fund_ids:
        value, value_date = fetch_share_value(session, fund_id)
        results.append(
            {
                "fund_id": fund_id,
                "current_share_value": value,
                "current_share_value_date": value_date,
            }
        )
    return results


def run(fund_ids: Iterable[str]) -> int:
    """CLI entry point that prints the fetched values."""
    session = _build_session()
    exit_code = 0
    for fund_id in fund_ids:
        try:
            value, value_date = fetch_share_value(session, fund_id)
            print(f"Fund {fund_id}: {value:.6f} (as of {value_date})")
        except RuntimeError as exc:
            exit_code = 1
            print(str(exc), file=sys.stderr)
    return exit_code


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch Santander fund share values.")
    parser.add_argument(
        "fund_ids",
        nargs="*",
        default=["1"],
        help="Fund ids from https://www.santander.com.ar/personas/inversiones/informacion-fondos#/detail/<id>",
    )
    return parser.parse_args()


def _running_in_colab() -> bool:
    return "google.colab" in sys.modules


def _prompt_fund_ids() -> Iterable[str]:
    raw = input("Enter fund ids separated by spaces (default: 1): ").strip()
    return raw.split() if raw else ["1"]


if __name__ == "__main__":
    if _running_in_colab() and len(sys.argv) == 1:
        fund_ids = _prompt_fund_ids()
    else:
        fund_ids = parse_args().fund_ids
    raise SystemExit(run(fund_ids))
