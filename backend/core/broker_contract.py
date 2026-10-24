from __future__ import annotations

import math
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.valuation.models import Price

Broker = Literal["iol", "ppi"]
MAX_SOURCE_ROWS = 10_000
MAX_SOURCE_PAYLOAD_BYTES = 4 * 1024 * 1024


class BrokerPosition(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)

    symbol: str = Field(min_length=1, max_length=128)
    description: str | None = Field(default=None, max_length=1024)
    instrument_type: str | None = Field(default=None, max_length=128)
    market: str | None = Field(default=None, max_length=128)
    source: Broker
    account_id: str = Field(min_length=1, max_length=128)
    currency: str | None = Field(default=None, max_length=32)
    quantity: float
    price: float | None = None
    valuation: float | None = None


POSITION_COLUMNS = list(BrokerPosition.model_fields)


class BrokerPrice(Price):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)


class BrokerSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1]
    source: Broker
    status: Literal["ok"]
    positions: list[BrokerPosition] = Field(max_length=MAX_SOURCE_ROWS)
    prices: list[BrokerPrice] = Field(max_length=MAX_SOURCE_ROWS)

    @model_validator(mode="after")
    def match_source(self):
        if any(row.source != self.source for row in self.positions):
            raise ValueError("Position source mismatch")
        allowed = {"iol"} if self.source == "iol" else {"ppi_api", "ppi_cash"}
        if any(price.source not in allowed for price in self.prices):
            raise ValueError("Price source mismatch")
        return self


class BrokerFailure(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = 1
    source: Broker
    status: Literal["error"] = "error"
    error_code: Literal["invalid_request", "source_unavailable", "invalid_response"]


def broker_success(source: Broker, positions: list[dict], prices: list[Price]) -> BrokerSnapshot:
    normalized = []
    for row in positions:
        row = dict(row)
        for name in ("price", "valuation"):
            value = row.get(name)
            if isinstance(value, float) and not math.isfinite(value):
                row[name] = None
        normalized.append(row)
    return BrokerSnapshot(
        schema_version=1, source=source, status="ok", positions=normalized,
        prices=[price.model_dump(mode="json") for price in prices],
    )
