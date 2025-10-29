from pathlib import Path
from datetime import datetime
from .config import OUTPUT_DIR, S3_BUCKET, S3_PREFIX, ACCOUNT_ID


def _save_snapshot_files(df) -> dict:
    """Save CSV and Parquet under data/positions/dt=YYYY-MM-DD/source=iol/account=.../."""
    ts = datetime.utcnow()
    dt = ts.strftime("%Y-%m-%d")
    out_dir = Path(OUTPUT_DIR) / f"dt={dt}" / "source=iol" / f"account={ACCOUNT_ID}"
    out_dir.mkdir(parents=True, exist_ok=True)

    # filenames
    stamp = ts.strftime("%H%M%S")
    csv_path = out_dir / f"positions_{dt}_{stamp}.csv"
    pq_path = out_dir / f"positions_{dt}_{stamp}.parquet"

    # attach timestamp column for convenience
    df_out = df.copy()
    df_out["snapshot_ts"] = ts.isoformat(timespec="seconds") + "Z"

    df_out.to_csv(csv_path, index=False)
    try:
        df_out.to_parquet(pq_path, index=False)
    except Exception as e:
        # Parquet is optional; keep CSV always
        print(f"Parquet save failed (will keep CSV only): {e}")
        pq_path = None

    return {"csv": str(csv_path), "parquet": str(pq_path) if pq_path else None, "dt": dt}


def _maybe_upload_to_s3(paths: list[str], dt: str):
    if not S3_BUCKET:
        print("S3 bucket not set; skipping upload.")
        return
    try:
        import boto3
        s3 = boto3.client("s3")
        for p in paths:
            if not p:
                continue
            key = f"{S3_PREFIX}dt={dt}/source=iol/account={ACCOUNT_ID}/{Path(p).name}"
            s3.upload_file(p, S3_BUCKET, key)
            print(f"Uploaded -> s3://{S3_BUCKET}/{key}")
    except Exception as e:
        print(f"S3 upload failed: {e}")


# Public API (stable names without leading underscore)
def save_snapshot_files(df) -> dict:
    return _save_snapshot_files(df)


def maybe_upload_to_s3(paths: list[str], dt: str):
    return _maybe_upload_to_s3(paths, dt)
