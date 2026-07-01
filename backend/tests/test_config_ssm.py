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
    def paginate(self, **kwargs):
        assert kwargs == {
            "Path": "/fintracker/test",
            "WithDecryption": True,
            "Recursive": True,
        }
        return [
            {
                "Parameters": [
                    {"Name": "/fintracker/test/JWT_SECRET", "Value": "from-ssm"},
                    {"Name": "/fintracker/test/DEMO_AUTH_USERNAME", "Value": "demo"},
                ]
            }
        ]


class FakeSsmClient:
    def get_paginator(self, name: str):
        assert name == "get_parameters_by_path"
        return FakePaginator()


def test_config_loads_missing_values_from_ssm(monkeypatch):
    for name, value in REQUIRED_ENV.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("PYTHON_DOTENV_DISABLED", "1")
    monkeypatch.setenv("SSM_ENV_PATH", "/fintracker/test")
    monkeypatch.setenv("JWT_SECRET", "from-env")
    monkeypatch.delenv("DEMO_AUTH_USERNAME", raising=False)

    class FakeBotoCoreError(Exception):
        pass

    class FakeClientError(Exception):
        pass

    fake_boto3 = types.ModuleType("boto3")
    fake_boto3.client = lambda service_name: FakeSsmClient()
    fake_botocore = types.ModuleType("botocore")
    fake_botocore.__path__ = []
    fake_botocore_exceptions = types.ModuleType("botocore.exceptions")
    fake_botocore_exceptions.BotoCoreError = FakeBotoCoreError
    fake_botocore_exceptions.ClientError = FakeClientError
    monkeypatch.setitem(sys.modules, "boto3", fake_boto3)
    monkeypatch.setitem(sys.modules, "botocore", fake_botocore)
    monkeypatch.setitem(sys.modules, "botocore.exceptions", fake_botocore_exceptions)
    sys.modules.pop("backend.core.config", None)

    config = importlib.import_module("backend.core.config")

    assert config.JWT_SECRET == "from-env"
    assert config.DEMO_AUTH_USERNAME == "demo"
