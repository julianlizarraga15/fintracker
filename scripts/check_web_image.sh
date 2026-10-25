#!/usr/bin/env bash
set -euo pipefail

image="${1:?Usage: scripts/check_web_image.sh IMAGE}"
docker run --rm --network none --read-only --entrypoint python \
  -e PYTHON_DOTENV_DISABLED=1 -e PYTHONDONTWRITEBYTECODE=1 \
  -e AWS_EC2_METADATA_DISABLED=true -e SSM_ENV_PATH= \
  -e ACCOUNT_EMAIL=runtime-check@example.invalid "$image" -c '
import importlib.metadata
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import time
import urllib.error
import urllib.request

WORKER_ONLY_MODULES = (
    "backend.workers", "backend.tests", "backend.core.iol_client",
    "backend.core.ppi_client", "backend.core.iol_transform", "backend.core.ppi_transform",
    "backend.core.iol_utils", "backend.core.sources.iol", "backend.core.binance_client",
    "ppi_client",
)
RUNTIME_SCRIPTS = {
    "run_valuations.sh", "run_valuations_with_status.sh", "check_broker_web_config.py",
}
HEALTH_ATTEMPTS = 50
HEALTH_POLL_SECONDS = 0.1
HEALTH_REQUEST_TIMEOUT_SECONDS = 1
SERVER_STOP_TIMEOUT_SECONDS = 5

for module in WORKER_ONLY_MODULES:
    assert importlib.util.find_spec(module) is None, "Worker-only module in web image: " + module
try:
    importlib.metadata.version("ppi-client")
except importlib.metadata.PackageNotFoundError:
    pass
else:
    raise AssertionError("Worker SDK installed in web image")
assert {path.name for path in Path("/app/scripts").iterdir()} == RUNTIME_SCRIPTS

from backend.core.daily_snapshot import main
from backend.core.fx_rates import get_fx_rates
assert callable(main) and callable(get_fx_rates)

server = subprocess.Popen(
    [sys.executable, "-m", "uvicorn", "backend.app.main:app", "--host", "127.0.0.1", "--port", "8000"],
    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
)
try:
    for _ in range(HEALTH_ATTEMPTS):
        assert server.poll() is None, "Web server failed to start"
        try:
            with urllib.request.urlopen("http://127.0.0.1:8000/health", timeout=HEALTH_REQUEST_TIMEOUT_SECONDS) as response:
                assert json.load(response) == {"ok": True}
            break
        except (urllib.error.URLError, OSError):
            time.sleep(HEALTH_POLL_SECONDS)
    else:
        raise AssertionError("Web health endpoint unavailable")
finally:
    server.terminate()
    server.wait(timeout=SERVER_STOP_TIMEOUT_SECONDS)
print("web image boundary and HTTP smoke check passed")
'
