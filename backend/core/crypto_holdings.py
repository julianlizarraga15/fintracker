"""Load manual crypto holdings to merge into valuations."""

from __future__ import annotations

import json
from pathlib import Path
from typing import List, Optional, TypedDict

from .config import ACCOUNT_ID, CRYPTO_HOLDINGS_FILE


class CryptoHolding(TypedDict):
    symbol: str
    quantity: float
    account_id: str
    currency: str
    market: str
    source: str
    display_name: Optional[str]


def _normalize_entry(entry: dict) -> Optional[CryptoHolding]:
    symbol_raw = entry.get("symbol")
    if not symbol_raw:
        return None

    symbol = str(symbol_raw).strip().upper()
    if not symbol:
        return None

    try:
        quantity = float(entry.get("quantity", 0.0))
    except (TypeError, ValueError):
        return None
    if quantity <= 0:
        return None

    display_name = entry.get("display_name") or entry.get("name")

    return CryptoHolding(
        symbol=symbol,
        quantity=quantity,
        account_id=str(entry.get("account_id") or ACCOUNT_ID),
        currency=str(entry.get("currency") or "USD"),
        market=str(entry.get("market") or "crypto"),
        source=str(entry.get("source") or "manual"),
        display_name=str(display_name).strip() if display_name else None,
    )


def load_holdings(path: Optional[str] = None) -> List[CryptoHolding]:
    """Return sanitized holdings from disk (empty list if file absent/invalid)."""

    file_path = Path(path or CRYPTO_HOLDINGS_FILE)
    if not file_path.exists():
        return []

    try:
        raw_text = file_path.read_text(encoding="utf-8").strip()
    except OSError as exc:
        print(f"[warn] Unable to read {file_path}: {exc}")
        return []

    if not raw_text:
        return []

    try:
        payload = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        print(f"[warn] Invalid JSON in {file_path}: {exc}")
        return []

    if isinstance(payload, dict):
        entries = payload.get("positions") or payload.get("holdings") or []
    elif isinstance(payload, list):
        entries = payload
    else:
        return []

    holdings: List[CryptoHolding] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        normalized = _normalize_entry(entry)
        if normalized:
            holdings.append(normalized)

    return holdings
