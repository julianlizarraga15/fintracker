import os

os.environ["PYTHON_DOTENV_DISABLED"] = "1"
os.environ.setdefault("ACCOUNT_EMAIL", "pytest@example.invalid")
os.environ.setdefault("DEMO_AUTH_USERNAME", "demo")
os.environ.setdefault("DEMO_AUTH_PASSWORD", "test-password")
os.environ.setdefault("JWT_SECRET", "pytest-only-signing-secret")
