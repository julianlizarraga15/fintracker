"""Load manual Santander mutual-fund holdings to merge into valuations."""

from __future__ import annotations

import json
from pathlib import Path
from typing import List, Optional, TypedDict

from .config import ACCOUNT_ID, SANTANDER_HOLDINGS_FILE


class SantanderHolding(TypedDict):
    fund_id: str
    symbol: str
    quantity: float
    account_id: str
    currency: str
    market: str
    source: str
    display_name: Optional[str]


def _normalize_entry(entry: dict) -> Optional[SantanderHolding]:
    fund_id_raw = entry.get("fund_id") or entry.get("id")
    if fund_id_raw is None:
        return None
    fund_id = str(fund_id_raw).strip()
    if not fund_id:
        return None

    try:
        quantity = float(entry.get("quantity", 0.0))
    except (TypeError, ValueError):
        return None
    if quantity <= 0:
        return None

    symbol = entry.get("symbol") or f"SANTANDER_{fund_id}"
    symbol = str(symbol).strip().upper()
    display_name = entry.get("display_name") or entry.get("name")

    return SantanderHolding(
        fund_id=fund_id,
        symbol=symbol,
        quantity=quantity,
        account_id=str(entry.get("account_id") or ACCOUNT_ID),
        currency=str(entry.get("currency") or "ARS"),
        market=str(entry.get("market") or "santander"),
        source=str(entry.get("source") or "santander"),
        display_name=str(display_name).strip() if display_name else None,
    )


def load_holdings(path: Optional[str] = None) -> List[SantanderHolding]:
    """Return sanitized holdings from disk (empty list if file absent/invalid)."""

    file_path = Path(path or SANTANDER_HOLDINGS_FILE)
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
        entries = payload.get("positions") or payload.get("funds") or []
    elif isinstance(payload, list):
        entries = payload
    else:
        return []

    holdings: List[SantanderHolding] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        normalized = _normalize_entry(entry)
        if normalized:
            holdings.append(normalized)

    return holdings

