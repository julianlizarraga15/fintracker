from __future__ import annotations
from datetime import date, datetime
from typing import Optional, Literal, List, Dict, Iterable, Tuple
from pydantic import BaseModel, Field

AssetType = Literal[
    "equity", "cedear", "etf", "bond", "crypto", "fci", "cash", "other"
]
PriceType = Literal["close", "last", "nav", "midpoint"]
# Source is free-form (e.g., "iol", "binance", "santander", "manual", "yahoo", "iol_api")
CurrencyCode = str


class Asset(BaseModel):
    """
    Canonical asset dictionary row.

    One row per canonical symbol (your internal symbol).
    """
    symbol: str = Field(..., description="Canonical internal symbol (unique key).")
    display_name: Optional[str] = Field(None, description="Human-friendly name.")
    asset_type: AssetType = Field(..., description="Asset class/type.")
    native_currency: CurrencyCode = Field(..., description="Currency in which the asset is natively quoted.")
    venues: Optional[List[str]] = Field(default=None, description="Acceptable trading venues/markets.")
    alt_symbols: Optional[List[Dict[str, str]]] = Field(
        default=None,
        description='Alt ids like [{"source": "iol", "raw_symbol": "BRKB"}].'
    )
    status: Literal["active", "inactive"] = Field(default="active")


class Position(BaseModel):
    """
    Holdings snapshot row (what you own), independent of price.
    Partition by snapshot_dt when writing to storage.
    """
    snapshot_dt: date = Field(..., description="Partition date for the snapshot (UTC date).")
    snapshot_ts: datetime = Field(..., description="Exact timestamp the snapshot was taken (UTC).")

    account_id: str = Field(..., description="Your internal account key/id.")
    source: str = Field(..., description="Origin of this row: iol, binance, santander, manual, etc.")
    market: Optional[str] = Field(None, description="e.g., argentina, us, binance, santander")

    # Symbols
    symbol: str = Field(..., description="Canonical symbol (join key to Asset).")
    raw_symbol: Optional[str] = Field(None, description="Symbol as reported by the source.")
    asset_type: Optional[AssetType] = Field(None, description="Optional redundancy for convenience.")
    currency: Optional[CurrencyCode] = Field(None, description="Currency that the position is notionally held in.")

    # Quantities & optional costs
    quantity: float = Field(..., ge=0.0)
    cost_basis_ccy: Optional[CurrencyCode] = None
    cost_basis_per_unit: Optional[float] = Field(None, ge=0.0)

    # Optional lot identifier if your source has it
    position_id: Optional[str] = None
    notes: Optional[str] = None


class Price(BaseModel):
    """
    Market price row (unit price in native currency).
    Partition by asof_dt for daily; keep asof_ts if you want higher frequency.
    """
    asof_dt: date = Field(..., description="Partition date (UTC date).")
    asof_ts: Optional[datetime] = Field(None, description="Timestamp of quote if available (UTC).")

    symbol: str = Field(..., description="Canonical symbol (join key to Asset).")
    price_type: PriceType = Field(..., description="close, last, nav, midpoint.")
    price: float = Field(..., ge=0.0, description="Unit price in 'currency'.")
    currency: CurrencyCode = Field(..., description="Quote currency of the price (e.g., ARS, USD, USDT).")

    venue: Optional[str] = Field(None, description="Exchange/venue, e.g., BCBA, NYSE, BINANCE, SANTANDER.")
    source: str = Field(..., description="e.g., iol_api, binance_api, yahoo, manual.")
    quality_score: int = Field(100, ge=0, le=100, description="Conflict resolution helper (higher = better).")

    valid_from_ts: Optional[datetime] = None
    valid_to_ts: Optional[datetime] = None


class FXRate(BaseModel):
    """
    FX conversion rate row.
    rate converts from_ccy -> to_ccy by multiplication (amount * rate).
    """
    asof_dt: date = Field(..., description="Partition date (UTC date).")
    from_ccy: CurrencyCode = Field(..., description="Source currency (e.g., ARS).")
    to_ccy: CurrencyCode = Field(..., description="Target/base currency (e.g., USD_MEP).")
    rate: float = Field(..., gt=0.0, description="Multiply amount_in_from by rate to get amount_in_to.")
    source: str = Field(..., description="e.g., mep_scraper, bna_api, manual.")
    max_age_days: int = Field(3, ge=0, description="Carry-forward tolerance for staleness.")


class Valuation(BaseModel):
    """
    Derived row for analytics: quantity * unit_price_native * fx_rate_to_base.
    Partition by snapshot_dt.
    """
    snapshot_dt: date = Field(..., description="Partition date (UTC date).")
    computed_ts: datetime = Field(..., description="When the valuation row was computed (UTC).")

    account_id: str
    source: str
    market: Optional[str] = None

    symbol: str
    asset_type: Optional[AssetType] = None

    quantity: float = Field(..., ge=0.0)

    unit_price_native: Optional[float] = Field(None, ge=0.0)
    unit_price_native_ccy: Optional[CurrencyCode] = None
    fx_rate_to_base: Optional[float] = Field(None, ge=0.0)

    unit_price_base: Optional[float] = Field(None, ge=0.0)
    value_base: Optional[float] = Field(None, ge=0.0)

    price_source: Optional[str] = None
    price_quality_score: Optional[int] = Field(None, ge=0, le=100)
    fx_source: Optional[str] = None

    status: Literal["ok", "missing_input", "stale_price", "stale_fx", "anomaly"] = "ok"


def compute_valuations(
    positions: Iterable[Position],
    prices: Iterable[Price],
    fx_rates: Iterable[FXRate],
    base_currency: CurrencyCode,
    snapshot_dt: Optional[date] = None,
    computed_ts: Optional[datetime] = None,
) -> List[Valuation]:
    """
    Build valuation rows from positions, prices and FX data.
    Keeps the logic intentionally lightweight and easy to follow.
    """
    computed_ts = computed_ts or datetime.utcnow()
    snapshot_dt = snapshot_dt or date.today()

    price_lookup: Dict[str, Price] = {}
    for price in prices:
        current = price_lookup.get(price.symbol)
        if current is None:
            price_lookup[price.symbol] = price
            continue
        # prefer better quality, otherwise keep the first seen
        if price.quality_score >= current.quality_score:
            price_lookup[price.symbol] = price

    fx_lookup: Dict[Tuple[CurrencyCode, CurrencyCode], FXRate] = {}
    for fx in fx_rates:
        fx_lookup[(fx.from_ccy, fx.to_ccy)] = fx

    valuations: List[Valuation] = []

    for pos in positions:
        price = price_lookup.get(pos.symbol)
        base_kwargs = dict(
            snapshot_dt=snapshot_dt,
            computed_ts=computed_ts,
            account_id=pos.account_id,
            source=pos.source,
            market=pos.market,
            symbol=pos.symbol,
            asset_type=pos.asset_type,
            quantity=pos.quantity,
        )

        if price is None:
            valuations.append(Valuation(status="missing_input", **base_kwargs))
            continue

        fx_rate = 1.0
        fx_source: Optional[str] = None
        native_ccy = price.currency

        if native_ccy != base_currency:
            direct = fx_lookup.get((native_ccy, base_currency))
            if direct:
                fx_rate = direct.rate
                fx_source = direct.source
            else:
                inverse = fx_lookup.get((base_currency, native_ccy))
                if inverse and inverse.rate:
                    fx_rate = 1.0 / inverse.rate
                    fx_source = inverse.source
                else:
                    valuations.append(
                        Valuation(
                            unit_price_native=price.price,
                            unit_price_native_ccy=native_ccy,
                            price_source=price.source,
                            price_quality_score=price.quality_score,
                            status="missing_input",
                            **base_kwargs,
                        )
                    )
                    continue

        unit_price_base = price.price * fx_rate if fx_rate is not None else None
        value_base = unit_price_base * pos.quantity if unit_price_base is not None else None

        valuations.append(
            Valuation(
                unit_price_native=price.price,
                unit_price_native_ccy=native_ccy,
                fx_rate_to_base=fx_rate,
                unit_price_base=unit_price_base,
                value_base=value_base,
                price_source=price.source,
                price_quality_score=price.quality_score,
                fx_source=fx_source,
                status="ok" if unit_price_base is not None else "missing_input",
                **base_kwargs,
            )
        )

    return valuations
