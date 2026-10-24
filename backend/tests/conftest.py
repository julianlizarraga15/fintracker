import os

from backend.core.settings import BROKER_CREDENTIAL_NAMES

os.environ["PYTHON_DOTENV_DISABLED"] = "1"
os.environ["SSM_ENV_PATH"] = ""
os.environ["AWS_EC2_METADATA_DISABLED"] = "true"

for name in BROKER_CREDENTIAL_NAMES:
    os.environ.pop(name, None)
os.environ.setdefault("ACCOUNT_EMAIL", "pytest@example.invalid")
os.environ.setdefault("DEMO_AUTH_USERNAME", "demo")
os.environ.setdefault("DEMO_AUTH_PASSWORD", "test-password")
os.environ.setdefault("JWT_SECRET", "pytest-only-signing-secret")
