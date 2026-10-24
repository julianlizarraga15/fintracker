from __future__ import annotations

import sys
import types
import subprocess

import pytest

from backend.core.settings import BROKER_CREDENTIAL_NAMES, WEB_SSM_NAMES, load_settings


class FakeSsmClient:
    def __init__(self, calls):
        self.calls = calls

    def get_parameters(self, **kwargs):
        self.calls.append(kwargs)
        values = {"JWT_SECRET": "from-ssm", "DEMO_AUTH_USERNAME": "demo"}
        return {"Parameters": [
            {"Name": name, "Value": values[name.rsplit("/", 1)[-1]]}
            for name in kwargs["Names"] if name.rsplit("/", 1)[-1] in values
        ]}


def install_fake_boto(monkeypatch, calls):
    fake_boto3 = types.ModuleType("boto3")
    fake_boto3.client = lambda service_name: FakeSsmClient(calls)
    monkeypatch.setitem(sys.modules, "boto3", fake_boto3)


def prepare_env(monkeypatch):
    for name in BROKER_CREDENTIAL_NAMES:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("ACCOUNT_EMAIL", "test@example.invalid")
    monkeypatch.setenv("PYTHON_DOTENV_DISABLED", "1")
    monkeypatch.setenv("SSM_ENV_PATH", "/fintracker/web")
    monkeypatch.delenv("SSM_ENV_OVERRIDE", raising=False)


def test_web_loads_only_explicit_settings_without_requesting_broker_secrets(monkeypatch):
    prepare_env(monkeypatch)
    monkeypatch.setenv("JWT_SECRET", "from-env")
    monkeypatch.delenv("DEMO_AUTH_USERNAME", raising=False)
    calls = []
    install_fake_boto(monkeypatch, calls)
    settings = load_settings()
    assert settings.JWT_SECRET == "from-env"
    assert settings.DEMO_AUTH_USERNAME == "demo"
    requested = {name for call in calls for name in call["Names"]}
    assert requested == {f"/fintracker/web/{name}" for name in WEB_SSM_NAMES}
    assert not any(name.rsplit("/", 1)[-1] in BROKER_CREDENTIAL_NAMES for name in requested)
    assert all(call["WithDecryption"] and len(call["Names"]) <= 10 for call in calls)


def test_ssm_override_replaces_web_settings(monkeypatch):
    prepare_env(monkeypatch)
    monkeypatch.setenv("SSM_ENV_OVERRIDE", "true")
    monkeypatch.setenv("JWT_SECRET", "from-env")
    install_fake_boto(monkeypatch, [])
    assert load_settings().JWT_SECRET == "from-ssm"


def test_old_recursive_production_path_is_rejected_before_any_aws_call(monkeypatch):
    prepare_env(monkeypatch)
    monkeypatch.setenv("SSM_ENV_PATH", "/fintracker/prod")
    calls = []
    install_fake_boto(monkeypatch, calls)
    with pytest.raises(RuntimeError, match="/fintracker/web"):
        load_settings()
    assert not calls


@pytest.mark.parametrize("name", sorted(BROKER_CREDENTIAL_NAMES))
def test_web_refuses_broker_credentials_without_echoing_their_values(monkeypatch, name):
    prepare_env(monkeypatch)
    monkeypatch.setenv(name, "synthetic-sensitive-credential")
    calls = []
    install_fake_boto(monkeypatch, calls)
    with pytest.raises(RuntimeError) as exc:
        load_settings()
    assert name in str(exc.value)
    assert "synthetic-sensitive-credential" not in str(exc.value)
    assert not calls


def test_http_app_initializes_ssm_paths_before_loading_file_readers(monkeypatch, tmp_path):
    prepare_env(monkeypatch)
    snippet = f'''
import sys, types
from pathlib import Path
values = {{"SNAPSHOTS_LOCAL_DIR": {str(tmp_path / 'snapshots')!r},
           "JOB_RUNS_LOCAL_DIR": {str(tmp_path / 'jobs')!r}}}
class SSM:
    def get_parameters(self, **kwargs):
        return {{"Parameters": [{{"Name": name, "Value": values[name.rsplit("/", 1)[-1]]}}
                for name in kwargs["Names"] if name.rsplit("/", 1)[-1] in values]}}
sys.modules["boto3"] = types.SimpleNamespace(client=lambda service: SSM())
import backend.app.main
from backend.app import prices_history, job_runs
assert prices_history.SNAPSHOTS_ROOT == Path(values["SNAPSHOTS_LOCAL_DIR"])
assert job_runs.JOB_RUNS_ROOT == Path(values["JOB_RUNS_LOCAL_DIR"])
'''
    result = subprocess.run([sys.executable, "-c", snippet], capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
