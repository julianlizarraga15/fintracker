from __future__ import annotations

import argparse
import os
import subprocess
from pathlib import Path
from typing import Sequence

from backend.app.job_trigger import app_root, release_job_lock, valuation_job_script


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--lock-dir", required=True)
    args = parser.parse_args(argv)

    env = os.environ.copy()
    env["RUN_STAMP"] = args.run_id
    env["FINTRACKER_SKIP_JOB_LOCK"] = "1"
    lock_dir = Path(args.lock_dir)

    try:
        return subprocess.call([str(valuation_job_script())], cwd=str(app_root()), env=env)
    finally:
        release_job_lock(lock_dir)


if __name__ == "__main__":
    raise SystemExit(main())
