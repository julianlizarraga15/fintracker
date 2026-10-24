from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys


def test_scheduled_job_uses_ssm_run_history_and_existing_lock(tmp_path):
    repo_root = Path(__file__).resolve().parents[2]
    jobs_root = tmp_path / "configured-jobs"
    values = {
        "JOB_RUNS_LOCAL_DIR": str(jobs_root),
        "SANTANDER_HOLDINGS_FILE": str(tmp_path / "missing-santander.json"),
        "CRYPTO_HOLDINGS_FILE": str(tmp_path / "missing-crypto.json"),
    }
    (tmp_path / "sitecustomize.py").write_text(
        f"""
import socket
import sys
import types

values = {values!r}

def deny_network(*args, **kwargs):
    raise AssertionError("Unexpected network access in scheduled job test")

socket.socket.connect = deny_network
socket.create_connection = deny_network

class SSM:
    def get_parameters(self, *, Names, WithDecryption):
        assert WithDecryption
        assert all(name.startswith("/fintracker/web/") for name in Names)
        return {{"Parameters": [
            {{"Name": name, "Value": values[name.rsplit("/", 1)[-1]]}}
            for name in Names if name.rsplit("/", 1)[-1] in values
        ]}}

def client(service_name, *args, **kwargs):
    assert service_name == "ssm", "Unexpected AWS service: " + service_name
    return SSM()

sys.modules["boto3"] = types.SimpleNamespace(client=client)
""",
        encoding="utf-8",
    )
    env = {
        "PATH": os.pathsep.join((str(Path(sys.executable).parent), os.defpath)),
        "PYTHONPATH": os.pathsep.join((str(tmp_path), str(repo_root))),
        "PYTHON_DOTENV_DISABLED": "1",
        "AWS_EC2_METADATA_DISABLED": "true",
        "FINTRACKER_APP_ROOT": str(tmp_path),
        "ACCOUNT_EMAIL": "scheduled-test@example.invalid",
        "SSM_ENV_PATH": "/fintracker/web",
        "SSM_ENV_OVERRIDE": "false",
        "RUN_STAMP": "scheduled-ssm-test",
        **{name: "false" for name in (
            "ENABLE_IOL", "ENABLE_PPI", "ENABLE_BINANCE", "ENABLE_ETHEREUM",
            "ENABLE_EXODUS", "ENABLE_METAMASK_BTC",
        )},
    }
    command = ["bash", str(repo_root / "scripts/run_valuations_with_status.sh")]
    result = subprocess.run(
        command, cwd=tmp_path, env=env, capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    latest = jobs_root / "valuations/latest.json"
    assert latest.is_file(), f"Scheduled run did not save history at the configured SSM path: {result.stdout}"
    payload = json.loads(latest.read_text(encoding="utf-8"))
    assert payload["run_id"] == env["RUN_STAMP"]
    assert payload["exit_code"] == 0
    assert "No positions found" in result.stdout
    assert not (tmp_path / "data/job_runs").exists()

    lock_dir = jobs_root / "valuations/.running.lock"
    lock_dir.mkdir()
    env["RUN_STAMP"] = "scheduled-conflict-test"
    conflict = subprocess.run(
        command, cwd=tmp_path, env=env, capture_output=True, text=True, timeout=30,
    )
    assert conflict.returncode == 75, conflict.stdout + conflict.stderr
    assert "already running" in conflict.stdout
    assert lock_dir.is_dir()
    assert not (jobs_root / "valuations/scheduled-conflict-test.log").exists()
    assert json.loads(latest.read_text(encoding="utf-8"))["run_id"] == payload["run_id"]
    assert not (tmp_path / "data/job_runs").exists()
