from __future__ import annotations

import importlib
import sys
import types


REQUIRED_ENV = {
    "ACCOUNT_EMAIL": "test@example.com",
    "IOL_USERNAME": "user",
    "IOL_PASSWORD": "pass",
}


class FakePaginator:
    def __init__(self, parameters=None):
        self.parameters = parameters or [
            {"Name": "/fintracker/test/JWT_SECRET", "Value": "from-ssm"},
            {"Name": "/fintracker/test/DEMO_AUTH_USERNAME", "Value": "demo"},
        ]

    def paginate(self, **kwargs):
        assert kwargs == {
            "Path": "/fintracker/test",
            "WithDecryption": True,
            "Recursive": True,
        }
        return [{"Parameters": self.parameters}]


class FakeSsmClient:
    def __init__(self, parameters=None):
        self.parameters = parameters

    def get_paginator(self, name: str):
        assert name == "get_parameters_by_path"
        return FakePaginator(self.parameters)


def install_fake_boto(monkeypatch, parameters=None):
    class FakeBotoCoreError(Exception):
        pass

    class FakeClientError(Exception):
        pass

    fake_boto3 = types.ModuleType("boto3")
    fake_boto3.client = lambda service_name: FakeSsmClient(parameters)
    fake_botocore = types.ModuleType("botocore")
    fake_botocore.__path__ = []
    fake_botocore_exceptions = types.ModuleType("botocore.exceptions")
    fake_botocore_exceptions.BotoCoreError = FakeBotoCoreError
    fake_botocore_exceptions.ClientError = FakeClientError
    monkeypatch.setitem(sys.modules, "boto3", fake_boto3)
    monkeypatch.setitem(sys.modules, "botocore", fake_botocore)
    monkeypatch.setitem(sys.modules, "botocore.exceptions", fake_botocore_exceptions)


def import_fresh_config():
    sys.modules.pop("backend.core.config", None)
    return importlib.import_module("backend.core.config")


def prepare_required_env(monkeypatch):
    for name, value in REQUIRED_ENV.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("PYTHON_DOTENV_DISABLED", "1")
    monkeypatch.setenv("SSM_ENV_PATH", "/fintracker/test")


def test_config_loads_missing_values_from_ssm(monkeypatch):
    prepare_required_env(monkeypatch)
    monkeypatch.setenv("JWT_SECRET", "from-env")
    monkeypatch.delenv("DEMO_AUTH_USERNAME", raising=False)
    install_fake_boto(monkeypatch)

    config = import_fresh_config()

    assert config.JWT_SECRET == "from-env"
    assert config.DEMO_AUTH_USERNAME == "demo"


def test_config_ssm_override_replaces_existing_values_when_truthy(monkeypatch):
    prepare_required_env(monkeypatch)
    monkeypatch.setenv("SSM_ENV_OVERRIDE", "true")
    monkeypatch.setenv("JWT_SECRET", "from-env")
    monkeypatch.setenv("DEMO_AUTH_USERNAME", "env-demo")
    install_fake_boto(monkeypatch)

    config = import_fresh_config()

    assert config.JWT_SECRET == "from-ssm"
    assert config.DEMO_AUTH_USERNAME == "demo"
