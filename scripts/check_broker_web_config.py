"""Check a recreated web container's startup configuration without printing secrets."""

from __future__ import annotations

import json
import os
from pathlib import Path
import sys

os.environ["PYTHON_DOTENV_DISABLED"] = "1"
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from backend.core.settings import BROKER_CREDENTIAL_NAMES, WEB_SSM_PATH, load_settings


def main() -> int:
    present = sorted(name for name in BROKER_CREDENTIAL_NAMES if os.getenv(name))
    path_valid = os.getenv("SSM_ENV_PATH", "").rstrip("/") == WEB_SSM_PATH
    if present or not path_valid:
        print(json.dumps({"configuration_valid": False, "broker_names_present": present, "web_ssm_path_valid": path_valid}))
        return 1
    try:
        settings = load_settings()
    except Exception:
        print(json.dumps({"configuration_valid": False, "error": "web_configuration_rejected"}))
        return 1
    print(json.dumps({
        "configuration_valid": True,
        "broker_credentials_present": any(os.getenv(name) for name in BROKER_CREDENTIAL_NAMES),
        "web_ssm_path_valid": path_valid,
        "enabled_sources": {"iol": settings.ENABLE_IOL, "ppi": settings.ENABLE_PPI, "binance": settings.ENABLE_BINANCE},
    }))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
