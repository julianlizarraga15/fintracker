import hashlib
import os
from dataclasses import dataclass
from dotenv import load_dotenv

SSM_ENV_PATH_VAR = "SSM_ENV_PATH"
SSM_ENV_OVERRIDE_VAR = "SSM_ENV_OVERRIDE"
WEB_SSM_PATH = "/fintracker/web"
SSM_BATCH_SIZE = 10
BROKER_CREDENTIAL_NAMES = frozenset({
    "IOL_USERNAME", "IOL_PASSWORD", "IOL_ACCESS_TOKEN", "IOL_REFRESH_TOKEN",
    "PPI_PUBLIC_API_KEY", "PPI_PRIVATE_API_KEY", "BINANCE_API_KEY", "BINANCE_API_SECRET",
})
WEB_SSM_NAMES = (
    "ACCOUNT_EMAIL", "SNAPSHOTS_LOCAL_DIR", "SNAPSHOTS_S3_BUCKET", "SNAPSHOTS_S3_PREFIX",
    "JOB_RUNS_LOCAL_DIR", "VALUATIONS_BASE_CURRENCY",
    "SANTANDER_HOLDINGS_FILE", "CRYPTO_HOLDINGS_FILE",
    "ENABLE_IOL", "IOL_SOURCE_LAMBDA", "ENABLE_PPI", "PPI_SOURCE_LAMBDA", "BROKER_LAMBDA_REGION",
    "ENABLE_BINANCE", "BINANCE_BALANCE_LAMBDA", "BINANCE_BASE_URL",
    "ETHERSCAN_API_KEY", "ETHEREUM_WALLET_ADDRESSES", "ETHEREUM_TOKEN_CONTRACTS", "ENABLE_ETHEREUM",
    "EXODUS_ETH_ADDRESSES", "EXODUS_BTC_ADDRESSES", "ENABLE_EXODUS",
    "METAMASK_BTC_ADDRESSES", "ENABLE_METAMASK_BTC", "BLOCKCYPHER_API_KEY",
    "JWT_SECRET", "DEMO_AUTH_USERNAME", "DEMO_AUTH_PASSWORD", "JWT_EXPIRES_MINUTES",
)


def _env_as_bool(value: str | None) -> bool:
    if value is None:
        return False
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def _load_ssm_environment() -> None:
    ssm_path = os.getenv(SSM_ENV_PATH_VAR)
    if not ssm_path:
        return
    if ssm_path.rstrip("/") != WEB_SSM_PATH:
        raise RuntimeError(f"Web configuration must use SSM_ENV_PATH={WEB_SSM_PATH}")

    try:
        import boto3
        from botocore.exceptions import BotoCoreError, ClientError
    except Exception as exc:
        raise RuntimeError("boto3 is required when SSM_ENV_PATH is configured.") from exc

    override_existing = _env_as_bool(os.getenv(SSM_ENV_OVERRIDE_VAR))
    client = boto3.client("ssm")
    try:
        names = [f"{WEB_SSM_PATH}/{name}" for name in WEB_SSM_NAMES]
        for offset in range(0, len(names), SSM_BATCH_SIZE):
            requested = names[offset:offset + SSM_BATCH_SIZE]
            response = client.get_parameters(Names=requested, WithDecryption=True)
            for parameter in response.get("Parameters", []):
                if parameter["Name"] not in requested:
                    raise RuntimeError("Unexpected web configuration parameter")
                env_name = parameter["Name"].rstrip("/").rsplit("/", 1)[-1]
                if override_existing or env_name not in os.environ:
                    os.environ[env_name] = parameter.get("Value", "")
    except (BotoCoreError, ClientError):
        raise RuntimeError("Failed to load approved web configuration parameters") from None


def short_account_id(email: str) -> str:
    """Return a short, deterministic hash of the email for safe use in paths."""
    if not email:
        raise ValueError("ACCOUNT_EMAIL not set in .env")
    return hashlib.sha1(email.encode()).hexdigest()[:8]


@dataclass(frozen=True)
class Settings:
    DEFAULT_BINANCE_BASE_URL: str
    DEFAULT_JWT_EXPIRES_MINUTES: int

    OUTPUT_DIR: str
    S3_BUCKET: str | None
    S3_PREFIX: str

    ENABLE_IOL: bool
    IOL_SOURCE_LAMBDA: str
    PPI_SOURCE_LAMBDA: str
    BROKER_LAMBDA_REGION: str
    ENABLE_PPI: bool

    ACCOUNT_EMAIL: str | None
    ACCOUNT_ID: str

    SANTANDER_HOLDINGS_FILE: str
    CRYPTO_HOLDINGS_FILE: str

    BINANCE_BASE_URL: str
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
    load_dotenv()
    present = sorted(name for name in BROKER_CREDENTIAL_NAMES if os.getenv(name))
    if present:
        raise RuntimeError("Broker credentials are forbidden in the web runtime: " + ", ".join(present))
    _load_ssm_environment()

    default_binance_base_url = "https://api.binance.com"
    default_jwt_expires_minutes = 15

    account_email = os.getenv("ACCOUNT_EMAIL")

    try:
        jwt_expires_minutes = int(os.getenv("JWT_EXPIRES_MINUTES", str(default_jwt_expires_minutes)))
    except ValueError:
        jwt_expires_minutes = default_jwt_expires_minutes
    if jwt_expires_minutes <= 0:
        jwt_expires_minutes = default_jwt_expires_minutes

    return Settings(
        DEFAULT_BINANCE_BASE_URL=default_binance_base_url,
        DEFAULT_JWT_EXPIRES_MINUTES=default_jwt_expires_minutes,
        OUTPUT_DIR=os.getenv("SNAPSHOTS_LOCAL_DIR", "data/positions"),
        S3_BUCKET=os.getenv("SNAPSHOTS_S3_BUCKET"),
        S3_PREFIX=os.getenv("SNAPSHOTS_S3_PREFIX", "positions/"),
        ENABLE_IOL=_env_as_bool(os.getenv("ENABLE_IOL")),
        IOL_SOURCE_LAMBDA=os.getenv("IOL_SOURCE_LAMBDA", "fintracker-fetch-iol-data"),
        PPI_SOURCE_LAMBDA=os.getenv("PPI_SOURCE_LAMBDA", "fintracker-fetch-ppi-data"),
        BROKER_LAMBDA_REGION=os.getenv("BROKER_LAMBDA_REGION", "us-east-1"),
        ENABLE_PPI=_env_as_bool(os.getenv("ENABLE_PPI")),
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
        BINANCE_BASE_URL=os.getenv("BINANCE_BASE_URL", default_binance_base_url),
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
