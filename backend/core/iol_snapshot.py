from datetime import datetime, date

from backend.core.config import IOL_USERNAME, IOL_PASSWORD, ACCOUNT_ID
from backend.core.iol_client import get_bearer_tokens, get_positions
from backend.core.iol_transform import extract_positions_as_df
from backend.core.storage import save_snapshot_files, maybe_upload_to_s3
from backend.core.iol_client import get_prices_for_positions, get_fx_rates
from backend.valuation.models import Position as PositionModel, compute_valuations
import pandas as pd

BASE_CURRENCY = "USD"


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
    prices = []
    try:
        prices = get_prices_for_positions(raw_items, access_token)
        if prices:
            # Convert list of Pydantic models to DataFrame
            # Use `model_dump()` for Pydantic v2 instead of deprecated `dict()`
            rows = [p.model_dump() for p in prices]
            df_prices = pd.DataFrame(rows)
            # attach account id for consistency with positions
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
        else:
            print("No prices fetched from IOL.")
    except Exception as e:
        print(f"Error fetching/saving prices: {e}")

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
