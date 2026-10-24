#!/usr/bin/env bash
set -euo pipefail

image="${1:?Usage: scripts/check_broker_image.sh IMAGE}"
docker run --rm --network none --entrypoint python "$image" -c '
import importlib.metadata
import sys
from backend.workers.iol import handler as iol
from backend.workers.ppi import handler as ppi
from backend.core.ppi_client import _build_ppi

assert importlib.metadata.version("ppi-client") == "1.2.4"
_build_ppi(sandbox=False)
for handler in (iol, ppi):
    assert handler({"account": "forbidden"}, None)["error_code"] == "invalid_request"
    assert handler({}, None)["error_code"] == "source_unavailable"
assert "backend.core.config" not in sys.modules
assert "backend.core.settings" not in sys.modules
print("broker image smoke check passed")
'
