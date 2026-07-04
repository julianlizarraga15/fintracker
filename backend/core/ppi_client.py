from __future__ import annotations

from typing import Any, Optional

from backend.core.config import (
    PPI_ACCOUNT_NUMBER,
    PPI_PRIVATE_API_KEY,
    PPI_PUBLIC_API_KEY,
    PPI_SANDBOX,
)


class PPIAPIError(RuntimeError):
    """Raised when PPI credentials or API calls fail."""


def _build_ppi(sandbox: bool = PPI_SANDBOX):
    try:
        from ppi_client.ppi import PPI
    except ImportError as exc:
        raise PPIAPIError(
            "ppi-client is not installed. Run `pip install -r backend/requirements.txt` first."
        ) from exc
    return PPI(sandbox=sandbox)


def login(
    public_api_key: Optional[str] = None,
    private_api_key: Optional[str] = None,
    sandbox: bool = PPI_SANDBOX,
):
    public_key = public_api_key or PPI_PUBLIC_API_KEY
    private_key = private_api_key or PPI_PRIVATE_API_KEY
    if not public_key or not private_key:
        raise PPIAPIError("Missing PPI_PUBLIC_API_KEY or PPI_PRIVATE_API_KEY.")

    ppi = _build_ppi(sandbox=sandbox)
    ppi.account.login_api(public_key, private_key)
    return ppi


def get_accounts(ppi) -> list[dict[str, Any]]:
    accounts = ppi.account.get_accounts()
    return accounts if isinstance(accounts, list) else []


def resolve_account_number(ppi, configured_account_number: Optional[str] = PPI_ACCOUNT_NUMBER) -> str:
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
    return payload


def get_current_market_data(ppi, ticker: str, instrument_type: str, settlement: str) -> Optional[dict[str, Any]]:
    try:
        payload = ppi.marketdata.current(ticker, instrument_type, settlement)
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None
