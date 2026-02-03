import os
import hashlib
from dotenv import load_dotenv

# Load environment variables from .env when this module is imported
load_dotenv()


def _env_as_bool(value: str | None) -> bool:
    if value is None:
        return False
    return str(value).strip().lower() in ("1", "true", "yes", "on")


DEFAULT_BINANCE_BASE_URL = "https://api.binance.com"
DEFAULT_BINANCE_RECV_WINDOW_MS = 5000

OUTPUT_DIR = os.getenv("SNAPSHOTS_LOCAL_DIR", "data/positions")
S3_BUCKET = os.getenv("SNAPSHOTS_S3_BUCKET")
S3_PREFIX = os.getenv("SNAPSHOTS_S3_PREFIX", "positions/")

IOL_USERNAME = os.getenv("IOL_USERNAME")
IOL_PASSWORD = os.getenv("IOL_PASSWORD")

ACCOUNT_EMAIL = os.getenv("ACCOUNT_EMAIL")

def short_account_id(email: str) -> str:
    """Return a short, deterministic hash of the email for safe use in paths."""
    if not email:
        raise ValueError("ACCOUNT_EMAIL not set in .env")
    return hashlib.sha1(email.encode()).hexdigest()[:8]

ACCOUNT_ID = short_account_id(ACCOUNT_EMAIL)

SANTANDER_HOLDINGS_FILE = os.getenv(
    "SANTANDER_HOLDINGS_FILE",
    os.path.join("data", "manual", "santander_holdings.json"),
)

CRYPTO_HOLDINGS_FILE = os.getenv(
    "CRYPTO_HOLDINGS_FILE",
    os.path.join("data", "manual", "crypto_holdings.json"),
)

BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET")
BINANCE_BASE_URL = os.getenv("BINANCE_BASE_URL", DEFAULT_BINANCE_BASE_URL)
try:
    BINANCE_RECV_WINDOW_MS = int(os.getenv("BINANCE_RECV_WINDOW_MS", str(DEFAULT_BINANCE_RECV_WINDOW_MS)))
except ValueError:
    BINANCE_RECV_WINDOW_MS = DEFAULT_BINANCE_RECV_WINDOW_MS
ENABLE_BINANCE = _env_as_bool(os.getenv("ENABLE_BINANCE"))
BINANCE_BALANCE_LAMBDA = os.getenv("BINANCE_BALANCE_LAMBDA", "fintracker-fetch-binance-balance")

# Ethereum / MetaMask
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY")
ETHEREUM_WALLET_ADDRESSES = os.getenv("ETHEREUM_WALLET_ADDRESSES", "")
ETHEREUM_TOKEN_CONTRACTS = os.getenv("ETHEREUM_TOKEN_CONTRACTS", "")
ENABLE_ETHEREUM = _env_as_bool(os.getenv("ENABLE_ETHEREUM"))
