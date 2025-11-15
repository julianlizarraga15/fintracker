from __future__ import annotations

from datetime import datetime, date

from backend.core.config import IOL_USERNAME, IOL_PASSWORD, ACCOUNT_ID
from backend.core.iol_client import get_bearer_tokens, get_positions
from backend.core.iol_transform import extract_positions_as_df
from backend.core.storage import save_snapshot_files, maybe_upload_to_s3
from backend.core.iol_client import get_prices_for_positions, get_fx_rates
from backend.core.santander_holdings import SantanderHolding, load_holdings
from backend.core.santander_nav import fetch_share_values as fetch_santander_nav_values
from backend.valuation.models import (
    Position as PositionModel,
    Price as PriceModel,
    compute_valuations,
)
import pandas as pd

BASE_CURRENCY = "USD"

POSITION_COLUMNS = [
    "symbol",
    "description",
    "instrument_type",
    "market",
    "source",
    "account_id",
    "currency",
    "quantity",
    "price",
    "valuation",
]


def _parse_nav_timestamp(raw_value: str | None) -> tuple[date, datetime | None]:
    if not raw_value:
        return date.today(), None
    sanitized = raw_value.replace("Z", "+00:00")
    try:
        ts = datetime.fromisoformat(sanitized)
    except ValueError:
        return date.today(), None
    return ts.date(), ts


def _manual_positions_df(holdings: list[SantanderHolding]) -> pd.DataFrame:
    rows = []
    for holding in holdings:
        rows.append(
            {
                "symbol": holding["symbol"],
                "description": holding.get("display_name"),
                "instrument_type": "fci",
                "market": holding["market"],
                "source": holding["source"],
                "account_id": holding["account_id"],
                "currency": holding["currency"],
                "quantity": holding["quantity"],
                "price": None,
                "valuation": None,
            }
        )

    df_manual = pd.DataFrame(rows)
    if df_manual.empty:
        return df_manual
    return df_manual.reindex(columns=POSITION_COLUMNS)


def main():
    if not IOL_USERNAME or not IOL_PASSWORD:
        print("Missing IOL_USERNAME or IOL_PASSWORD in .env")
        return

    access_token, _ = get_bearer_tokens(IOL_USERNAME, IOL_PASSWORD)

    try:
        raw_items = get_positions(access_token)
    except Exception as e:
        print(f"Error fetching positions: {e}")
        return

    df = extract_positions_as_df(raw_items)
    # set account_id column if present
    if not df.empty:
        df["account_id"] = ACCOUNT_ID

    santander_holdings = load_holdings()
    holdings_by_fund_id: dict[str, list[SantanderHolding]] = {}
    if santander_holdings:
        manual_df = _manual_positions_df(santander_holdings)
        if not manual_df.empty:
            df = pd.concat([df, manual_df], ignore_index=True) if not df.empty else manual_df
        for holding in santander_holdings:
            holdings_by_fund_id.setdefault(holding["fund_id"], []).append(holding)
        if santander_holdings:
            print(f"Loaded {len(santander_holdings)} Santander manual holdings.")

    if df.empty:
        print("No positions found or unexpected API format.")
        try:
            preview = {"items_len": len(raw_items), "first_item": raw_items[0] if raw_items else None}
            print("Raw preview:", str(preview)[:500])
        except Exception:
            pass
        return

    print("df shape:", df.shape)
    print(df)

    # Save snapshot files (partitioned by date/source/account)
    info = save_snapshot_files(df)
    print(f"\nSaved -> {info['csv']}")
    if info.get("parquet"):
        print(f"Saved -> {info['parquet']}")

    # Optional: upload to S3 if configured
    upload_csv = False
    if upload_csv:
        maybe_upload_to_s3([info.get("csv"), info.get("parquet")], info.get("dt"))
    else:
        maybe_upload_to_s3([info.get("parquet")], info.get("dt"))

    # Quick totals by currency
    print("\nTotals by currency:")
    try:
        print(df.groupby("currency")["valuation"].sum())
    except Exception:
        pass

    # Fetch and persist prices for the current positions (simple inline behavior)
    prices: list[PriceModel] = []
    try:
        prices = get_prices_for_positions(raw_items, access_token)
    except Exception as e:
        print(f"Error fetching prices from IOL: {e}")

    nav_display_names: dict[str, str] = {}
    santander_price_models: list[PriceModel] = []
    if holdings_by_fund_id:
        try:
            fund_ids = list(holdings_by_fund_id.keys())
            nav_rows = fetch_santander_nav_values(fund_ids)
            for nav_row in nav_rows:
                fund_id = nav_row["fund_id"]
                holdings = holdings_by_fund_id.get(fund_id) or []
                asof_dt, asof_ts = _parse_nav_timestamp(nav_row.get("current_share_value_date"))
                for holding in holdings:
                    santander_price_models.append(
                        PriceModel(
                            asof_dt=asof_dt,
                            asof_ts=asof_ts,
                            symbol=holding["symbol"],
                            price_type="nav",
                            price=nav_row["current_share_value"],
                            currency=holding["currency"],
                            venue="SANTANDER",
                            source="santander_nav",
                            quality_score=95,
                        )
                    )
                    fund_name = nav_row.get("fund_name")
                    if fund_name:
                        nav_display_names[holding["symbol"]] = fund_name
        except Exception as e:
            print(f"Error fetching Santander NAV: {e}")

    if nav_display_names and not df.empty:
        for symbol, display_name in nav_display_names.items():
            df.loc[df["symbol"] == symbol, "description"] = display_name

    if santander_price_models:
        prices.extend(santander_price_models)

    if prices:
        try:
            rows = [p.model_dump() for p in prices]
            df_prices = pd.DataFrame(rows)
            if not df_prices.empty:
                df_prices["account_id"] = ACCOUNT_ID
            info_prices = save_snapshot_files(df_prices, resource_name="prices")
            print(f"\nSaved prices -> {info_prices['csv']}")
            if info_prices.get("parquet"):
                print(f"Saved prices -> {info_prices['parquet']}")
            if upload_csv:
                maybe_upload_to_s3(
                    [info_prices.get("csv"), info_prices.get("parquet")],
                    info_prices.get("dt"),
                    resource_name="prices",
                )
            else:
                maybe_upload_to_s3(
                    [info_prices.get("parquet")],
                    info_prices.get("dt"),
                    resource_name="prices",
                )
        except Exception as e:
            print(f"Error saving prices snapshot: {e}")
    else:
        print("No prices fetched from IOL or Santander.")

    fx_rates = []
    try:
        fx_rates = get_fx_rates()
        if fx_rates:
            df_fx = pd.DataFrame([r.model_dump() for r in fx_rates])
            info_fx = save_snapshot_files(
                df_fx,
                resource_name="fx",
                source="dolarapi_blue_venta",
            )
            print(f"\nSaved FX rates -> {info_fx['csv']}")
            if info_fx.get("parquet"):
                print(f"Saved FX rates -> {info_fx['parquet']}")
            if upload_csv:
                maybe_upload_to_s3(
                    [info_fx.get("csv"), info_fx.get("parquet")],
                    info_fx.get("dt"),
                    resource_name="fx",
                    source="dolarapi_blue_venta",
                )
            else:
                maybe_upload_to_s3(
                    [info_fx.get("parquet")],
                    info_fx.get("dt"),
                    resource_name="fx",
                    source="dolarapi_blue_venta",
                )
        else:
            print("No FX rates fetched.")
    except Exception as e:
        print(f"Error fetching/saving FX rates: {e}")

    try:
        snapshot_dt_str = info.get("dt")
        try:
            snapshot_dt = datetime.strptime(snapshot_dt_str, "%Y-%m-%d").date() if snapshot_dt_str else date.today()
        except ValueError:
            snapshot_dt = date.today()

        pos_models = []
        snapshot_ts = datetime.utcnow()
        for row in df.to_dict(orient="records"):
            symbol = row.get("symbol")
            if not symbol:
                continue
            try:
                quantity = float(row.get("quantity") or 0.0)
            except (TypeError, ValueError):
                quantity = 0.0
            if quantity <= 0:
                continue
            pos_models.append(
                PositionModel(
                    snapshot_dt=snapshot_dt,
                    snapshot_ts=snapshot_ts,
                    account_id=row.get("account_id") or ACCOUNT_ID,
                    source=row.get("source") or "unknown",
                    market=row.get("market"),
                    symbol=symbol,
                    quantity=quantity,
                    currency=row.get("currency"),
                )
            )

        if not pos_models:
            print("No positions to value.")
            return

        valuations = compute_valuations(
            pos_models,
            prices,
            fx_rates,
            base_currency=BASE_CURRENCY,
            snapshot_dt=snapshot_dt,
        )

        if valuations:
            df_valuations = pd.DataFrame([v.model_dump() for v in valuations])
            info_val = save_snapshot_files(
                df_valuations,
                resource_name="valuations",
                account_id=ACCOUNT_ID,
            )
            print(f"\nSaved valuations -> {info_val['csv']}")
            if info_val.get("parquet"):
                print(f"Saved valuations -> {info_val['parquet']}")
            if upload_csv:
                maybe_upload_to_s3(
                    [info_val.get("csv"), info_val.get("parquet")],
                    info_val.get("dt"),
                    resource_name="valuations",
                    account_id=ACCOUNT_ID,
                )
            else:
                maybe_upload_to_s3(
                    [info_val.get("parquet")],
                    info_val.get("dt"),
                    resource_name="valuations",
                    account_id=ACCOUNT_ID,
                )
        else:
            print("No valuations generated.")
    except Exception as e:
        print(f"Error computing/saving valuations: {e}")


if __name__ == "__main__":
    main()
