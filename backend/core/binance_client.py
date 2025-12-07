from __future__ import annotations

import hashlib
import hmac
import logging
import time
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

import requests

from backend.core.binance_common import BINANCE_MARKET, BINANCE_SOURCE
from backend.core.config import (
    BINANCE_API_KEY,
    BINANCE_API_SECRET,
    BINANCE_BASE_URL,
    BINANCE_RECV_WINDOW_MS,
    DEFAULT_BINANCE_BASE_URL,
    DEFAULT_BINANCE_RECV_WINDOW_MS,
)

LOG = logging.getLogger(__name__)

ACCOUNT_ENDPOINT = "/api/v3/account"
TIME_ENDPOINT = "/api/v3/time"
DEFAULT_TIMEOUT = 15
BASE_URL = BINANCE_BASE_URL or DEFAULT_BINANCE_BASE_URL
DEFAULT_RECV_WINDOW_MS = BINANCE_RECV_WINDOW_MS or DEFAULT_BINANCE_RECV_WINDOW_MS


class BinanceAPIError(RuntimeError):
    def __init__(self, message: str, status_code: Optional[int] = None, payload: Any = None):
        super().__init__(message)
        self.status_code = status_code
        self.payload = payload


def _safe_text(resp: requests.Response) -> str:
    try:
        return resp.text[:300].replace("\n", " ")
    except Exception:
        return "<no body>"


def _timestamp_ms() -> int:
    return int(time.time() * 1000)


def _build_signed_params(api_secret: str, recv_window_ms: Optional[int], timestamp_ms: Optional[int]) -> Dict[str, Any]:
    params: Dict[str, Any] = {"timestamp": timestamp_ms or _timestamp_ms()}
    if recv_window_ms:
        params["recvWindow"] = recv_window_ms

    query_string = urlencode(params)
    signature = hmac.new(api_secret.encode("utf-8"), query_string.encode("utf-8"), hashlib.sha256).hexdigest()
    params["signature"] = signature
    return params


def _signed_get(
    path: str,
    api_key: str,
    api_secret: str,
    base_url: str,
    recv_window_ms: Optional[int],
    timeout: int,
    timestamp_ms: Optional[int] = None,
) -> Any:
    if not api_key or not api_secret:
        raise BinanceAPIError("Binance API key/secret missing.")

    params = _build_signed_params(api_secret, recv_window_ms, timestamp_ms)
    headers = {"X-MBX-APIKEY": api_key}
    url = f"{base_url.rstrip('/')}{path}"

    try:
        resp = requests.get(url, params=params, headers=headers, timeout=timeout)
    except requests.RequestException as exc:
        raise BinanceAPIError(f"Binance request error at {path}: {exc}") from exc

    payload: Any
    try:
        payload = resp.json()
    except ValueError:
        payload = None

    if resp.status_code in (401, 403):
        raise BinanceAPIError(
            f"Binance authentication failed (status={resp.status_code}).",
            status_code=resp.status_code,
            payload=payload if payload is not None else _safe_text(resp),
        )
    if resp.status_code == 429:
        raise BinanceAPIError(
            "Binance rate limit hit (429).",
            status_code=resp.status_code,
            payload=payload if payload is not None else _safe_text(resp),
        )
    if resp.status_code >= 400:
        raise BinanceAPIError(
            f"Binance error {resp.status_code}: {payload if payload is not None else _safe_text(resp)}",
            status_code=resp.status_code,
            payload=payload,
        )

    if payload is None:
        raise BinanceAPIError(f"Binance returned a non-JSON payload (status={resp.status_code}).", status_code=resp.status_code)

    return payload


def _is_timestamp_error(payload: Any) -> bool:
    return isinstance(payload, dict) and payload.get("code") == -1021


def get_server_time(base_url: str = BASE_URL, timeout: int = DEFAULT_TIMEOUT) -> Optional[int]:
    url = f"{base_url.rstrip('/')}{TIME_ENDPOINT}"
    try:
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
        server_time = data.get("serverTime")
        return int(server_time) if server_time is not None else None
    except (requests.RequestException, ValueError, TypeError) as exc:
        LOG.debug("Binance server time fetch failed: %s", exc)
        return None


def get_account_balances(
    api_key: Optional[str] = None,
    api_secret: Optional[str] = None,
    base_url: str = BASE_URL,
    recv_window_ms: Optional[int] = DEFAULT_RECV_WINDOW_MS,
    timeout: int = DEFAULT_TIMEOUT,
) -> List[Dict[str, Any]]:
    key = api_key or BINANCE_API_KEY
    secret = api_secret or BINANCE_API_SECRET

    def _fetch(ts_ms: Optional[int] = None) -> Any:
        return _signed_get(
            ACCOUNT_ENDPOINT,
            key,
            secret,
            base_url,
            recv_window_ms,
            timeout,
            ts_ms,
        )

    try:
        payload = _fetch()
    except BinanceAPIError as err:
        if err.status_code == 400 and _is_timestamp_error(err.payload):
            server_time = get_server_time(base_url=base_url, timeout=timeout)
            if server_time:
                payload = _fetch(server_time)
            else:
                raise
        else:
            raise

    balances_raw = []
    if isinstance(payload, dict):
        balances_raw = payload.get("balances") or []

    normalized: List[Dict[str, Any]] = []
    for entry in balances_raw or []:
        asset = str(entry.get("asset") or "").strip().upper()
        if not asset:
            continue

        try:
            free_amt = float(entry.get("free") or 0.0)
        except (TypeError, ValueError):
            free_amt = 0.0
        try:
            locked_amt = float(entry.get("locked") or 0.0)
        except (TypeError, ValueError):
            locked_amt = 0.0

        quantity = free_amt + locked_amt
        if quantity <= 0:
            continue

        normalized.append(
            {
                "asset": asset,
                "free": free_amt,
                "locked": locked_amt,
                "quantity": quantity,
                "market": BINANCE_MARKET,
                "source": BINANCE_SOURCE,
            }
        )

    return normalized
