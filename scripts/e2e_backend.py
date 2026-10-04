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
        fieldnames=["snapshot_dt", "computed_ts", "symbol", "quantity", "unit_price_base", "value_base", "status", "source", "asset_type", "market"],
    )
    writer.writeheader()
    for row in [
        {"symbol": "BTC", "quantity": 0.225, "unit_price_base": 80000, "value_base": 18000, "status": "ok", "source": "binance", "asset_type": "crypto", "market": "crypto"},
        {"symbol": "BTC", "quantity": 0.05, "unit_price_base": "", "value_base": "", "status": "missing_price", "source": "binance", "asset_type": "crypto", "market": "crypto"},
        {"symbol": "BTC", "quantity": 0.1, "unit_price_base": 80000, "value_base": 8000, "status": "ok", "source": "exodus", "asset_type": "crypto", "market": "crypto"},
        {"symbol": "GGAL", "quantity": 0.5, "unit_price_base": 8000, "value_base": 4000, "status": "ok", "source": "iol", "asset_type": "cedear", "market": "equity"},
        {"symbol": "ZERO", "quantity": 12.75, "unit_price_base": 0, "value_base": 0, "status": "ok", "source": "iol", "asset_type": "other", "market": "equity"},
        {"symbol": "UNPRICED", "quantity": 2.5, "unit_price_base": "", "value_base": "", "status": "missing_price", "source": "unknown", "asset_type": "", "market": ""},
    ]:
        writer.writerow({"snapshot_dt": "2026-09-30", "computed_ts": "2026-09-30T12:00:00Z", **row})

other_account = "other-account"
other_dir = storage / "valuations" / "dt=2026-09-30" / f"account={other_account}"
other_dir.mkdir(parents=True)
with (other_dir / "valuations_2026-09-30.csv").open("w", newline="", encoding="utf-8") as handle:
    writer = csv.DictWriter(handle, fieldnames=["snapshot_dt", "computed_ts", "symbol", "quantity", "value_base", "status", "source", "asset_type"])
    writer.writeheader()
    writer.writerow({"snapshot_dt": "2026-09-30", "computed_ts": "2026-09-30T12:00:00Z", "symbol": "ALT", "quantity": 1, "value_base": 5000, "status": "ok", "source": "ppi", "asset_type": "fci"})

manual_file = storage / "manual" / "crypto.json"
manual_file.parent.mkdir(parents=True, exist_ok=True)
manual_file.write_text(json.dumps([{"symbol": "XRP", "quantity": 12.5, "source": "manual", "currency": "USD", "market": "crypto", "account_id": other_account}]), encoding="utf-8")

price_dir = storage / "prices" / "dt=2026-10-01"
price_dir.mkdir(parents=True)
with (price_dir / "prices_2026-10-01.csv").open("w", newline="", encoding="utf-8") as handle:
    writer = csv.DictWriter(handle, fieldnames=["asof_dt", "symbol", "price", "currency", "source", "venue", "quality_score"])
    writer.writeheader()
    writer.writerow({"asof_dt": "2026-10-01", "symbol": "BTC", "price": 80500, "currency": "USD", "source": "e2e", "venue": "fixture", "quality_score": 100})
    writer.writerow({"asof_dt": "2026-10-02", "symbol": "BTC", "price": 81000, "currency": "USD", "source": "e2e", "venue": "fixture", "quality_score": 100})

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
