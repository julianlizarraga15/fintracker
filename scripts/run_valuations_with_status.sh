#!/usr/bin/env bash
set -uo pipefail

cd /app

if [[ -f .env ]]; then
  set -o allexport
  # shellcheck disable=SC1091
  source .env
  set +o allexport
fi

JOB_NAME="valuations"
RUN_STAMP="$(date -u +"%Y-%m-%d_%H%M%S")"
STARTED_AT="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
RUNS_ROOT="${JOB_RUNS_LOCAL_DIR:-data/job_runs}"
RUNS_DIR="${RUNS_ROOT}/${JOB_NAME}"
LOG_FILE="${RUNS_DIR}/${RUN_STAMP}.log"

mkdir -p "${RUNS_DIR}"

set +e
python -m backend.core.daily_snapshot 2>&1 | tee "${LOG_FILE}"
EXIT_CODE=${PIPESTATUS[0]}
set -e

FINISHED_AT="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

export JOB_NAME RUN_STAMP STARTED_AT FINISHED_AT EXIT_CODE RUNS_DIR LOG_FILE
python - <<'PY' || true
from __future__ import annotations

import json
import os
import re
from datetime import datetime
from pathlib import Path


def parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def slugify(label: str) -> str:
    label = label.strip().lower()
    replacements = {
        "santander manual holdings": "santander_holdings",
        "crypto manual holdings": "crypto_manual_holdings",
        "binance assets": "binance_assets",
        "ethereum assets": "ethereum_assets",
        "exodus ethereum assets": "exodus_eth_assets",
        "exodus bitcoin assets": "exodus_btc_assets",
        "metamask bitcoin assets": "metamask_btc_assets",
    }
    if label in replacements:
        return replacements[label]
    return re.sub(r"[^a-z0-9]+", "_", label).strip("_")


job_name = os.environ["JOB_NAME"]
run_id = os.environ["RUN_STAMP"]
started_at = os.environ["STARTED_AT"]
finished_at = os.environ["FINISHED_AT"]
exit_code = int(os.environ.get("EXIT_CODE", "1"))
runs_dir = Path(os.environ["RUNS_DIR"])
log_file = Path(os.environ["LOG_FILE"])
log_text = log_file.read_text(encoding="utf-8", errors="replace") if log_file.exists() else ""
lines = [line.strip() for line in log_text.splitlines() if line.strip()]

loaded = {}
pricing = {}
outputs = {}
uploads = []
warnings = []
errors = []
positions_count = None

for line in lines:
    loaded_match = re.search(r"Loaded\s+(\d+)\s+(.+?)\.?$", line)
    if loaded_match:
        loaded[slugify(loaded_match.group(2))] = int(loaded_match.group(1))

    positions_match = re.search(r"Positions\s+\((\d+)\s+rows", line)
    if positions_match:
        positions_count = int(positions_match.group(1))

    binance_price_match = re.search(r"Priced\s+(\d+)\s+Binance assets\s+\(missing\s+(\d+)\)", line)
    if binance_price_match:
        pricing["binance_priced"] = int(binance_price_match.group(1))
        pricing["binance_missing"] = int(binance_price_match.group(2))

    generic_price_match = re.search(r"Priced\s+(\d+)\s+(.+?) assets\.?$", line)
    if generic_price_match:
        pricing[f"{slugify(generic_price_match.group(2))}_priced"] = int(generic_price_match.group(1))

    saved_match = re.search(r"\[(positions|prices|fx|valuations)\]\s+(CSV|Parquet) saved ->\s+(.+)$", line)
    if saved_match:
        resource, file_type, path = saved_match.groups()
        outputs.setdefault(resource, {})[file_type.lower()] = path

    upload_match = re.search(r"\[ok\]\s+Uploaded ->\s+(.+)$", line)
    if upload_match:
        uploads.append(upload_match.group(1))

    lowered = line.lower()
    if "error" in lowered or "traceback" in lowered or "exception" in lowered:
        errors.append(line)
    elif "missing" in lowered or lowered.startswith("no "):
        warnings.append(line)

status = "success" if exit_code == 0 and "valuations" in outputs else "failed"
try:
    duration_seconds = (parse_ts(finished_at) - parse_ts(started_at)).total_seconds()
except Exception:
    duration_seconds = None

payload = {
    "job": job_name,
    "run_id": run_id,
    "status": status,
    "started_at": started_at,
    "finished_at": finished_at,
    "duration_seconds": duration_seconds,
    "exit_code": exit_code,
    "positions_count": positions_count,
    "loaded": loaded,
    "pricing": pricing,
    "outputs": outputs,
    "uploads": uploads,
    "s3_uploaded": bool(uploads),
    "warnings": warnings,
    "errors": errors,
    "log_file": str(log_file),
}

runs_dir.mkdir(parents=True, exist_ok=True)
for name in (f"{run_id}.json", "latest.json"):
    target = runs_dir / name
    tmp = runs_dir / f".{name}.tmp"
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(target)
print(f"[job-run] Recorded status -> {runs_dir / f'{run_id}.json'}")
PY

exit "${EXIT_CODE}"
