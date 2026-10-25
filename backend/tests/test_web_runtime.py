import os
from pathlib import Path
import subprocess
import sys


def test_web_and_valuation_import_without_worker_modules():
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run([sys.executable, "-c", """
import sys

class WorkerImportBlocker:
    def find_spec(self, fullname, path=None, target=None):
        blocked = (
            'backend.workers', 'backend.core.iol_client', 'backend.core.ppi_client',
            'backend.core.iol_transform', 'backend.core.ppi_transform',
            'backend.core.iol_utils', 'backend.core.sources.iol',
            'backend.core.binance_client', 'ppi_client',
        )
        if any(fullname == name or fullname.startswith(name + '.') for name in blocked):
            raise RuntimeError('Web runtime imported worker-only module ' + fullname)

sys.meta_path.insert(0, WorkerImportBlocker())
import backend.app.main
import backend.core.daily_snapshot
"""], cwd=root, env={
        "PATH": os.defpath,
        "PYTHONPATH": str(root),
        "PYTHON_DOTENV_DISABLED": "1",
        "AWS_EC2_METADATA_DISABLED": "true",
        "SSM_ENV_PATH": "",
        "ACCOUNT_EMAIL": "runtime-check@example.invalid",
    }, capture_output=True, text=True, timeout=30)

    assert result.returncode == 0, result.stderr
