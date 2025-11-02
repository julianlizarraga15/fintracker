from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Optional, Sequence

from .config import OUTPUT_DIR, S3_BUCKET, S3_PREFIX, ACCOUNT_ID


# --------------------------- helpers ---------------------------

def _parts_for(resource: str, dt: str, source: Optional[str], account_id: Optional[str]) -> list[str]:
    """Return path/key parts according to your convention."""
    parts = [resource, f"dt={dt}"]

    if resource == "positions":
        if source:
            parts.append(f"source={source}")
        parts.append(f"account={(account_id or ACCOUNT_ID)}")

    elif resource in ("prices", "fx"):
        if source:
            parts.append(f"source={source}")

    elif resource == "valuations":
        parts.append(f"account={(account_id or ACCOUNT_ID)}")

    else:  # generic
        if source:
            parts.append(f"source={source}")
        if account_id:
            parts.append(f"account={account_id}")

    return parts


# --------------------------- disk I/O ---------------------------

def save_snapshot_files(
    df,
    resource_name: str = "positions",
    source: Optional[str] = None,
    account_id: Optional[str] = None,
) -> dict:
    """
    Save CSV (always) and Parquet (best-effort) under:
      {OUTPUT_DIR}/{resource}/dt=YYYY-MM-DD/[source=...]/[account=...]/

    Returns {"csv": str|None, "parquet": str|None, "dt": "YYYY-MM-DD"}.
    """
    ts = datetime.utcnow()
    dt = ts.strftime("%Y-%m-%d")

    out_dir = Path(OUTPUT_DIR)
    for part in _parts_for(resource_name, dt, source, account_id):
        out_dir /= part
    out_dir.mkdir(parents=True, exist_ok=True)

    stamp = ts.strftime("%H%M%S")
    base = f"{resource_name}_{dt}_{stamp}"
    csv_path = out_dir / f"{base}.csv"
    pq_path = out_dir / f"{base}.parquet"

    # attach timestamp column
    df_out = df.copy()
    df_out["snapshot_ts"] = ts.isoformat(timespec="seconds") + "Z"

    df_out.to_csv(csv_path, index=False)

    parquet_path_str: Optional[str] = None
    try:
        df_out.to_parquet(pq_path, index=False)  # requires pyarrow or fastparquet
        parquet_path_str = str(pq_path)
    except Exception as e:
        print(f"[warn] Parquet save failed; keeping CSV only: {e}")

    return {"csv": str(csv_path), "parquet": parquet_path_str, "dt": dt}


# --------------------------- S3 upload ---------------------------

def maybe_upload_to_s3(
    paths: Sequence[str],
    dt: str,
    resource_name: str = "positions",
    source: Optional[str] = None,
    account_id: Optional[str] = None,
) -> None:
    """
    If S3_BUCKET is set, upload given local file paths to:
      s3://{S3_BUCKET}/{S3_PREFIX}{resource}/dt=.../[...]/filename
    """
    if not S3_BUCKET:
        print("[info] S3 bucket not set; skipping upload.")
        return

    try:
        import boto3  # lazy import
        s3 = boto3.client("s3")

        key_prefix = "/".join(_parts_for(resource_name, dt, source, account_id))
        if S3_PREFIX:
            key_prefix = f"{S3_PREFIX.rstrip('/')}/{key_prefix}"

        for p in paths:
            if not p:
                continue
            pth = Path(p)
            key = f"{key_prefix}/{pth.name}"
            s3.upload_file(str(pth), S3_BUCKET, key)
            print(f"[ok] Uploaded -> s3://{S3_BUCKET}/{key}")

    except Exception as e:
        print(f"[error] S3 upload failed: {e}")
