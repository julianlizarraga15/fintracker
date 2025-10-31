import os
from dotenv import load_dotenv

# Load environment variables from .env when this module is imported
load_dotenv()

OUTPUT_DIR = os.getenv("SNAPSHOTS_LOCAL_DIR", "data/positions")
S3_BUCKET = os.getenv("SNAPSHOTS_S3_BUCKET")
S3_PREFIX = os.getenv("SNAPSHOTS_S3_PREFIX", "positions/")
ACCOUNT_ID = os.getenv("IOL_ACCOUNT_ID", "unknown")

IOL_USERNAME = os.getenv("IOL_USERNAME")
IOL_PASSWORD = os.getenv("IOL_PASSWORD")
