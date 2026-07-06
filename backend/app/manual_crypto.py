from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field, field_validator

from backend.core import config

JSON_INDENT = 2


class ManualCryptoHolding(BaseModel):
    symbol: str = Field(min_length=1)
    quantity: float = Field(gt=0)
    display_name: Optional[str] = None
    currency: str = "USD"
    market: str = "crypto"
    source: str = "manual"
    account_id: Optional[str] = None

    @field_validator("symbol", "currency", mode="before")
    @classmethod
    def _uppercase_required_text(cls, value: object) -> str:
        text = str(value or "").strip().upper()
        if not text:
            raise ValueError("Value is required.")
        return text

    @field_validator("source", "market", mode="before")
    @classmethod
    def _strip_required_text(cls, value: object) -> str:
        text = str(value or "").strip()
        if not text:
            raise ValueError("Value is required.")
        return text

    @field_validator("display_name", "account_id", mode="before")
    @classmethod
    def _strip_optional_text(cls, value: object) -> Optional[str]:
        if value is None:
            return None
        text = str(value).strip()
        return text or None


class ManualCryptoHoldingsResponse(BaseModel):
    holdings: list[ManualCryptoHolding]


class ManualCryptoHoldingsUpdate(BaseModel):
    holdings: list[ManualCryptoHolding]


def _holdings_file(path: str | None = None) -> Path:
    return Path(path or config.CRYPTO_HOLDINGS_FILE)


def load_manual_crypto_holdings(path: str | None = None) -> ManualCryptoHoldingsResponse:
    file_path = _holdings_file(path)
    if not file_path.exists():
        return ManualCryptoHoldingsResponse(holdings=[])

    try:
        payload = json.loads(file_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"Unable to load manual crypto holdings from {file_path}.") from exc

    if isinstance(payload, dict):
        entries = payload.get("holdings") or payload.get("positions") or []
    elif isinstance(payload, list):
        entries = payload
    else:
        entries = []

    return ManualCryptoHoldingsResponse(
        holdings=[ManualCryptoHolding.model_validate(entry) for entry in entries if isinstance(entry, dict)]
    )


def save_manual_crypto_holdings(
    update: ManualCryptoHoldingsUpdate,
    path: str | None = None,
) -> ManualCryptoHoldingsResponse:
    file_path = _holdings_file(path)
    file_path.parent.mkdir(parents=True, exist_ok=True)

    rows = []
    for holding in update.holdings:
        row = holding.model_dump(exclude_none=True)
        if not row.get("account_id"):
            row.pop("account_id", None)
        rows.append(row)

    serialized = json.dumps(rows, indent=JSON_INDENT, sort_keys=True)
    temp_path = file_path.with_name(f".{file_path.name}.tmp")
    temp_path.write_text(f"{serialized}\n", encoding="utf-8")
    os.replace(temp_path, file_path)

    return load_manual_crypto_holdings(str(file_path))
