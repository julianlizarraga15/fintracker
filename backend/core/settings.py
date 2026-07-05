import hashlib
import os
import re
from dataclasses import dataclass
from dotenv import load_dotenv

SSM_ENV_PATH_VAR = "SSM_ENV_PATH"
SSM_ENV_OVERRIDE_VAR = "SSM_ENV_OVERRIDE"
SSM_ENV_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _env_as_bool(value: str | None) -> bool:
    if value is None:
        return False
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def _load_ssm_environment() -> None:
    ssm_path = os.getenv(SSM_ENV_PATH_VAR)
    if not ssm_path:
        return

    try:
        import boto3
        from botocore.exceptions import BotoCoreError, ClientError
    except Exception as exc:
        raise RuntimeError("boto3 is required when SSM_ENV_PATH is configured.") from exc

    override_existing = _env_as_bool(os.getenv(SSM_ENV_OVERRIDE_VAR))
    client = boto3.client("ssm")
    paginator = client.get_paginator("get_parameters_by_path")

    try:
        pages = paginator.paginate(Path=ssm_path, WithDecryption=True, Recursive=True)
        for page in pages:
            for parameter in page.get("Parameters", []):
                env_name = parameter["Name"].rstrip("/").rsplit("/", 1)[-1]
                if not SSM_ENV_NAME_PATTERN.fullmatch(env_name):
                    raise ValueError(f"Invalid environment variable name from SSM parameter: {parameter['Name']}")
                if override_existing or env_name not in os.environ:
                    os.environ[env_name] = parameter.get("Value", "")
    except (BotoCoreError, ClientError) as exc:
        raise RuntimeError(f"Failed to load parameters from SSM path {ssm_path!r}.") from exc


def short_account_id(email: str) -> str:
    """Return a short, deterministic hash of the email for safe use in paths."""
    if not email:
        raise ValueError("ACCOUNT_EMAIL not set in .env")
    return hashlib.sha1(email.encode()).hexdigest()[:8]


@dataclass(frozen=True)
class Settings:
    DEFAULT_BINANCE_BASE_URL: str
    DEFAULT_BINANCE_RECV_WINDOW_MS: int
    DEFAULT_JWT_EXPIRES_MINUTES: int
    DEFAULT_PPI_API_VERSION: str

    OUTPUT_DIR: str
    S3_BUCKET: str | None
    S3_PREFIX: str

    IOL_USERNAME: str | None
    IOL_PASSWORD: str | None

    PPI_PUBLIC_API_KEY: str | None
    PPI_PRIVATE_API_KEY: str | None
    PPI_ACCOUNT_NUMBER: str | None
    PPI_API_VERSION: str
    ENABLE_PPI: bool
    PPI_SANDBOX: bool

    ACCOUNT_EMAIL: str | None
    ACCOUNT_ID: str

    SANTANDER_HOLDINGS_FILE: str
    CRYPTO_HOLDINGS_FILE: str

    BINANCE_API_KEY: str | None
    BINANCE_API_SECRET: str | None
    BINANCE_BASE_URL: str
    BINANCE_RECV_WINDOW_MS: int
    ENABLE_BINANCE: bool
    BINANCE_BALANCE_LAMBDA: str

    ETHERSCAN_API_KEY: str | None
    ETHEREUM_WALLET_ADDRESSES: str
    ETHEREUM_TOKEN_CONTRACTS: str
    ENABLE_ETHEREUM: bool

    EXODUS_ETH_ADDRESSES: str
    EXODUS_BTC_ADDRESSES: str
    ENABLE_EXODUS: bool

    METAMASK_BTC_ADDRESSES: str
    ENABLE_METAMASK_BTC: bool

    BLOCKCYPHER_API_KEY: str | None

    JWT_SECRET: str | None
    DEMO_AUTH_USERNAME: str | None
    DEMO_AUTH_PASSWORD: str | None
    JWT_EXPIRES_MINUTES: int


def load_settings() -> Settings:
    # Load local .env first for development, then fill missing values from SSM in production.
    load_dotenv()
    _load_ssm_environment()

    default_binance_base_url = "https://api.binance.com"
    default_binance_recv_window_ms = 5000
    default_jwt_expires_minutes = 15
    default_ppi_api_version = "1.0"

    account_email = os.getenv("ACCOUNT_EMAIL")

    try:
        binance_recv_window_ms = int(os.getenv("BINANCE_RECV_WINDOW_MS", str(default_binance_recv_window_ms)))
    except ValueError:
        binance_recv_window_ms = default_binance_recv_window_ms

    try:
        jwt_expires_minutes = int(os.getenv("JWT_EXPIRES_MINUTES", str(default_jwt_expires_minutes)))
    except ValueError:
        jwt_expires_minutes = default_jwt_expires_minutes
    if jwt_expires_minutes <= 0:
        jwt_expires_minutes = default_jwt_expires_minutes

    return Settings(
        DEFAULT_BINANCE_BASE_URL=default_binance_base_url,
        DEFAULT_BINANCE_RECV_WINDOW_MS=default_binance_recv_window_ms,
        DEFAULT_JWT_EXPIRES_MINUTES=default_jwt_expires_minutes,
        DEFAULT_PPI_API_VERSION=default_ppi_api_version,
        OUTPUT_DIR=os.getenv("SNAPSHOTS_LOCAL_DIR", "data/positions"),
        S3_BUCKET=os.getenv("SNAPSHOTS_S3_BUCKET"),
        S3_PREFIX=os.getenv("SNAPSHOTS_S3_PREFIX", "positions/"),
        IOL_USERNAME=os.getenv("IOL_USERNAME"),
        IOL_PASSWORD=os.getenv("IOL_PASSWORD"),
        PPI_PUBLIC_API_KEY=os.getenv("PPI_PUBLIC_API_KEY"),
        PPI_PRIVATE_API_KEY=os.getenv("PPI_PRIVATE_API_KEY"),
        PPI_ACCOUNT_NUMBER=os.getenv("PPI_ACCOUNT_NUMBER"),
        PPI_API_VERSION=os.getenv("PPI_API_VERSION", default_ppi_api_version),
        ENABLE_PPI=_env_as_bool(os.getenv("ENABLE_PPI")),
        PPI_SANDBOX=_env_as_bool(os.getenv("PPI_SANDBOX")),
        ACCOUNT_EMAIL=account_email,
        ACCOUNT_ID=short_account_id(account_email),
        SANTANDER_HOLDINGS_FILE=os.getenv(
            "SANTANDER_HOLDINGS_FILE",
            os.path.join("data", "manual", "santander_holdings.json"),
        ),
        CRYPTO_HOLDINGS_FILE=os.getenv(
            "CRYPTO_HOLDINGS_FILE",
            os.path.join("data", "manual", "crypto_holdings.json"),
        ),
        BINANCE_API_KEY=os.getenv("BINANCE_API_KEY"),
        BINANCE_API_SECRET=os.getenv("BINANCE_API_SECRET"),
        BINANCE_BASE_URL=os.getenv("BINANCE_BASE_URL", default_binance_base_url),
        BINANCE_RECV_WINDOW_MS=binance_recv_window_ms,
        ENABLE_BINANCE=_env_as_bool(os.getenv("ENABLE_BINANCE")),
        BINANCE_BALANCE_LAMBDA=os.getenv("BINANCE_BALANCE_LAMBDA", "fintracker-fetch-binance-balance"),
        ETHERSCAN_API_KEY=os.getenv("ETHERSCAN_API_KEY"),
        ETHEREUM_WALLET_ADDRESSES=os.getenv("ETHEREUM_WALLET_ADDRESSES", ""),
        ETHEREUM_TOKEN_CONTRACTS=os.getenv("ETHEREUM_TOKEN_CONTRACTS", ""),
        ENABLE_ETHEREUM=_env_as_bool(os.getenv("ENABLE_ETHEREUM")),
        EXODUS_ETH_ADDRESSES=os.getenv("EXODUS_ETH_ADDRESSES", ""),
        EXODUS_BTC_ADDRESSES=os.getenv("EXODUS_BTC_ADDRESSES", ""),
        ENABLE_EXODUS=_env_as_bool(os.getenv("ENABLE_EXODUS")),
        METAMASK_BTC_ADDRESSES=os.getenv("METAMASK_BTC_ADDRESSES", ""),
        ENABLE_METAMASK_BTC=_env_as_bool(os.getenv("ENABLE_METAMASK_BTC")),
        BLOCKCYPHER_API_KEY=os.getenv("BLOCKCYPHER_API_KEY"),
        JWT_SECRET=os.getenv("JWT_SECRET"),
        DEMO_AUTH_USERNAME=os.getenv("DEMO_AUTH_USERNAME"),
        DEMO_AUTH_PASSWORD=os.getenv("DEMO_AUTH_PASSWORD"),
        JWT_EXPIRES_MINUTES=jwt_expires_minutes,
    )
