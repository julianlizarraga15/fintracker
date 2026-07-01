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
from pathlib import Path

from backend.app.job_runs import build_job_run_payload


job_name = os.environ["JOB_NAME"]
run_id = os.environ["RUN_STAMP"]
started_at = os.environ["STARTED_AT"]
finished_at = os.environ["FINISHED_AT"]
exit_code = int(os.environ.get("EXIT_CODE", "1"))
runs_dir = Path(os.environ["RUNS_DIR"])
log_file = Path(os.environ["LOG_FILE"])
payload = build_job_run_payload(
    job_name=job_name,
    run_id=run_id,
    started_at=started_at,
    finished_at=finished_at,
    exit_code=exit_code,
    log_file=log_file,
)

runs_dir.mkdir(parents=True, exist_ok=True)
for name in (f"{run_id}.json", "latest.json"):
    target = runs_dir / name
    tmp = runs_dir / f".{name}.tmp"
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(target)
print(f"[job-run] Recorded status -> {runs_dir / f'{run_id}.json'}")
PY

exit "${EXIT_CODE}"
