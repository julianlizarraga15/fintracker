from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Sequence
import os

from . import config
from .config import Settings

_BUCKET_NOTICE_EMITTED = False
_CREDENTIAL_NOTICE_EMITTED = False


# --------------------------- helpers ---------------------------

def _parts_for(
    resource: str,
    dt: str,
    source: Optional[str],
    account_id: Optional[str],
    settings: Settings | None = None,
) -> list[str]:
    """Return path/key parts according to your convention."""
    default_account_id = settings.ACCOUNT_ID if settings is not None else config.ACCOUNT_ID
    parts = [resource, f"dt={dt}"]

    if resource == "positions":
        if source:
            parts.append(f"source={source}")
        parts.append(f"account={(account_id or default_account_id)}")

    elif resource in ("prices", "fx"):
        if source:
            parts.append(f"source={source}")

    elif resource == "valuations":
        parts.append(f"account={(account_id or default_account_id)}")

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
    settings: Settings | None = None,
) -> dict:
    """
    Save CSV (always) and Parquet (best-effort) under:
      {OUTPUT_DIR}/{resource}/dt=YYYY-MM-DD/[source=...]/[account=...]/

    Returns {"csv": str|None, "parquet": str|None, "dt": "YYYY-MM-DD"}.
    """
    ts = datetime.now(timezone.utc)
    dt = ts.strftime("%Y-%m-%d")

    output_dir = settings.OUTPUT_DIR if settings is not None else config.OUTPUT_DIR
    out_dir = Path(output_dir)
    for part in _parts_for(resource_name, dt, source, account_id, settings):
        out_dir /= part
    out_dir.mkdir(parents=True, exist_ok=True)

    stamp = ts.strftime("%H%M%S")
    base = f"{resource_name}_{dt}_{stamp}"
    csv_path = out_dir / f"{base}.csv"
    pq_path = out_dir / f"{base}.parquet"

    # attach timestamp column
    df_out = df.copy()
    df_out["snapshot_ts"] = ts.isoformat(timespec="seconds").replace("+00:00", "Z")

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
    settings: Settings | None = None,
) -> None:
    """
    If S3_BUCKET is set, upload given local file paths to:
      s3://{S3_BUCKET}/{S3_PREFIX}{resource}/dt=.../[...]/filename
    """
    global _BUCKET_NOTICE_EMITTED, _CREDENTIAL_NOTICE_EMITTED

    s3_bucket = settings.S3_BUCKET if settings is not None else config.S3_BUCKET
    s3_prefix = settings.S3_PREFIX if settings is not None else config.S3_PREFIX

    if not s3_bucket:
        if not _BUCKET_NOTICE_EMITTED:
            print("[info] S3 bucket not set; skipping upload.")
            _BUCKET_NOTICE_EMITTED = True
        return

    try:
        import boto3  # lazy import
        # Allow explicit credentials via env variables for local runs or CI.
        # Respect standard AWS env vars (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN / AWS_DEFAULT_REGION)
        aws_key = os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("SNAPSHOTS_AWS_ACCESS_KEY_ID")
        aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY") or os.getenv("SNAPSHOTS_AWS_SECRET_ACCESS_KEY")
        aws_token = os.getenv("AWS_SESSION_TOKEN")
        aws_region = os.getenv("AWS_DEFAULT_REGION") or os.getenv("AWS_REGION")

        if aws_key and aws_secret:
            session = boto3.Session(
                aws_access_key_id=aws_key,
                aws_secret_access_key=aws_secret,
                aws_session_token=aws_token,
                region_name=aws_region,
            )
        else:
            # Let boto3 fall back to its normal credential resolution chain
            session = boto3.Session()

        credentials = session.get_credentials()
        if credentials is None or not credentials.access_key:
            if not _CREDENTIAL_NOTICE_EMITTED:
                print("[info] AWS credentials not configured; skipping S3 upload.")
                _CREDENTIAL_NOTICE_EMITTED = True
            return

        s3 = session.client("s3")

        key_prefix = "/".join(_parts_for(resource_name, dt, source, account_id, settings))
        if s3_prefix:
            key_prefix = f"{s3_prefix.rstrip('/')}/{key_prefix}"

        for p in paths:
            if not p:
                continue
            pth = Path(p)
            key = f"{key_prefix}/{pth.name}"
            s3.upload_file(str(pth), s3_bucket, key)
            print(f"[ok] Uploaded -> s3://{s3_bucket}/{key}")

    except Exception as e:
        # Try to provide a clearer hint for missing credentials (botocore raises NoCredentialsError)
        try:
            from botocore.exceptions import NoCredentialsError

            if isinstance(e, NoCredentialsError) or "Unable to locate credentials" in str(e):
                print("[error] S3 upload failed: Unable to locate credentials.\n"
                      "Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY (or run 'aws configure'),\n"
                      "or provide credentials via IAM role / AWS_PROFILE in the environment.")
                return
        except Exception:
            # ignore import/check failures and fall back to generic message
            pass

        print(f"[error] S3 upload failed: {e}")
