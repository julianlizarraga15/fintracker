from __future__ import annotations

from functools import partial
from types import SimpleNamespace
from typing import Any, Optional


PPI_HTTP_CONNECT_TIMEOUT_SECONDS = 5
PPI_HTTP_READ_TIMEOUT_SECONDS = 30


class PPIAPIError(RuntimeError):
    """Raised when PPI credentials or API calls fail."""


def _build_ppi(sandbox: bool = False):
    try:
        from ppi_client.api.accounts import AccountsApi
        from ppi_client.api.marketdata import MarketDataApi
        from ppi_client.ppi import PPI
        from ppi_client.ppi_api_client import PPIClient
    except ImportError as exc:
        raise PPIAPIError(
            "ppi-client is not installed. Run `pip install -r backend/requirements-broker-lambda.txt` first."
        ) from exc
    client = PPIClient(PPI.version, sandbox)
    rest_client = client.get_rest_client()
    sdk_get_session = rest_client.get_session

    def get_session():
        session = sdk_get_session()
        # Keep the SDK's TLS adapter and scope the timeout to this client only.
        session.request = partial(
            session.request,
            timeout=(PPI_HTTP_CONNECT_TIMEOUT_SECONDS, PPI_HTTP_READ_TIMEOUT_SECONDS),
        )
        return session

    rest_client.get_session = get_session
    return SimpleNamespace(account=AccountsApi(client), marketdata=MarketDataApi(client))


def login(
    public_api_key: str,
    private_api_key: str,
    sandbox: bool = False,
):
    if not public_api_key or not private_api_key:
        raise PPIAPIError("Missing PPI_PUBLIC_API_KEY or PPI_PRIVATE_API_KEY.")

    ppi = _build_ppi(sandbox=sandbox)
    ppi.account.login_api(public_api_key, private_api_key)
    return ppi


def get_accounts(ppi) -> list[dict[str, Any]]:
    accounts = ppi.account.get_accounts()
    return accounts if isinstance(accounts, list) else []


def resolve_account_number(ppi, configured_account_number: Optional[str] = None) -> str:
    if configured_account_number:
        return str(configured_account_number)

    accounts = get_accounts(ppi)
    if not accounts:
        raise PPIAPIError("PPI login succeeded, but no accounts were returned.")

    account_number = accounts[0].get("accountNumber")
    if not account_number:
        raise PPIAPIError("PPI account response did not include accountNumber.")
    return str(account_number)


def get_balance_and_positions(ppi, account_number: str) -> dict[str, Any]:
    payload = ppi.account.get_balance_and_positions(account_number)
    if not isinstance(payload, dict):
        raise PPIAPIError(f"Unexpected PPI balances/positions payload: {type(payload).__name__}")
    group_names = ("groupedAvailability", "groupedInstruments")
    present_groups = [name for name in group_names if name in payload]
    if not present_groups or any(not isinstance(payload[name], list) for name in present_groups):
        raise PPIAPIError("Unexpected PPI balances/positions payload shape.")
    return payload


def get_current_market_data(ppi, ticker: str, instrument_type: str, settlement: str) -> Optional[dict[str, Any]]:
    try:
        payload = ppi.marketdata.current(ticker, instrument_type, settlement)
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None
