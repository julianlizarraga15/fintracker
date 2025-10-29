from backend.core.config import IOL_USERNAME, IOL_PASSWORD, ACCOUNT_ID
from backend.core.iol_client import get_bearer_tokens, get_positions
from backend.core.iol_transform import extract_positions_as_df
from backend.core.storage import save_snapshot_files, maybe_upload_to_s3


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
    maybe_upload_to_s3([info.get("csv"), info.get("parquet")], info.get("dt"))

    # Quick totals by currency
    print("\nTotals by currency:")
    try:
        print(df.groupby("currency")["valuation"].sum())
    except Exception:
        pass


if __name__ == "__main__":
    main()
