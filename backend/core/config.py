import os
import hashlib
from dotenv import load_dotenv

# Load environment variables from .env when this module is imported
load_dotenv()

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
