"""Start the API with an isolated synthetic portfolio for browser tests."""
import csv
import hashlib
import json
import os
import shutil
from pathlib import Path

storage = Path("artifacts/e2e/data").resolve()
shutil.rmtree(storage, ignore_errors=True)
storage.mkdir(parents=True)
os.environ["JOB_RUNS_LOCAL_DIR"] = str(storage / "jobs")
os.environ.update(
    {
        "PYTHON_DOTENV_DISABLED": "1",
        "ACCOUNT_EMAIL": "e2e@example.invalid",
        "DEMO_AUTH_USERNAME": "demo",
        "DEMO_AUTH_PASSWORD": "test-password",
        "JWT_SECRET": "e2e-only-signing-secret",
        "SNAPSHOTS_LOCAL_DIR": str(storage),
        "CRYPTO_HOLDINGS_FILE": str(storage / "manual" / "crypto.json"),
        "ENABLE_BINANCE": "0",
        "ENABLE_PPI": "0",
        "ENABLE_ETHEREUM": "0",
        "ENABLE_EXODUS": "0",
        "ENABLE_METAMASK_BTC": "0",
        "SSM_ENV_PATH": "",
    }
)

account_id = hashlib.sha1(b"e2e@example.invalid").hexdigest()[:8]
snapshot_dir = storage / "valuations" / "dt=2026-09-30" / f"account={account_id}"
snapshot_dir.mkdir(parents=True)
with (snapshot_dir / "valuations_2026-09-30.csv").open("w", newline="", encoding="utf-8") as handle:
    writer = csv.DictWriter(
        handle,
        fieldnames=["snapshot_dt", "computed_ts", "symbol", "quantity", "value_base", "status", "source"],
    )
    writer.writeheader()
    writer.writerow(
        {
            "snapshot_dt": "2026-09-30",
            "computed_ts": "2026-09-30T12:00:00Z",
            "symbol": "BTC",
            "quantity": 0.5,
            "value_base": 30000,
            "status": "ok",
            "source": "binance",
        }
    )

run_id = "2026-09-30_120000"
started_at = "2026-09-30T12:00:00Z"
run_dir = storage / "jobs" / "valuations"
run_dir.mkdir(parents=True)
run = {
    "job": "valuations",
    "run_id": run_id,
    "status": "success",
    "started_at": started_at,
    "finished_at": "2026-09-30T12:00:05Z",
    "duration_seconds": 5,
    "exit_code": 0,
    "positions_count": 1,
    "loaded": {},
    "pricing": {},
    "counts_by_source": {},
    "pricing_issues": [],
    "outputs": {"valuations": {"csv": "synthetic.csv"}},
    "uploads": [],
    "warnings": [],
    "errors": [],
    "log_file": "synthetic.log",
}
(run_dir / f"{run_id}.json").write_text(json.dumps(run), encoding="utf-8")
(run_dir / "latest.json").write_text(json.dumps(run), encoding="utf-8")
lock_dir = run_dir / ".running.lock"
lock_dir.mkdir()
(lock_dir / "pid").write_text(f"{os.getpid()}\n", encoding="utf-8")

import uvicorn
uvicorn.run("backend.app.main:app", host="127.0.0.1", port=4174, log_level="warning")
