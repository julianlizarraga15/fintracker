from fastapi import FastAPI

from backend.app.job_runs import (
    DEFAULT_HISTORY_LIMIT,
    JobRunNotFound,
    JobRunResponse,
    JobRunSummary,
    get_job_run_history,
    get_latest_job_run,
)
from backend.app.prices_history import (
    DEFAULT_WINDOW_DAYS,
    MAX_WINDOW_DAYS,
    PriceHistoryResponse,
    get_price_history,
)
from backend.app.routers.auth import (
    LoginRequest,
    LoginResponse,
    _ensure_auth_configured,
    login,
    router as auth_router,
)
from backend.app.routers.jobs import job_run_history, latest_job_run, router as jobs_router
from backend.app.routers.prices import price_history, router as prices_router
from backend.app.routers.valuations import latest_valuations, router as valuations_router
from backend.app.security import (
    JWT_ALGORITHM,
    JWT_EXPIRES_SECONDS,
    _auth_scheme,
    _detect_account_id,
    _issue_token,
    require_jwt,
)
from backend.app.valuations import (
    LatestValuationResponse,
    SnapshotNotFound,
    get_latest_valuation_snapshot,
)
from backend.core.config import (
    ACCOUNT_ID,
    DEMO_AUTH_PASSWORD,
    DEMO_AUTH_USERNAME,
    JWT_EXPIRES_MINUTES,
    JWT_SECRET,
)

app = FastAPI(title="Fintracker API")


@app.get("/health")
def health():
    return {"ok": True}


app.include_router(auth_router)
app.include_router(valuations_router)
app.include_router(jobs_router)
app.include_router(prices_router)
