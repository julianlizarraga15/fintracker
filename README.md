# Fintracker

Personal finance tracker for pulling brokerage positions, computing valuations, and surfacing them through containerized services.

## Architecture
- **Backend (`backend/`)**: FastAPI app plus snapshot utilities (`backend/core`) that fetch positions/prices/FX, compute valuations, and write CSV/Parquet files via `python -m backend.core.daily_snapshot`. The API exposes `GET /valuations/latest?account_id=<hash>`, which streams the freshest on-disk valuation snapshot along with totals for the frontend.
- **Frontend (`frontend/`)**: Static site served through Nginx that queries `/api/valuations/latest` and renders the latest snapshot (status chip, totals, per-position table, etc.) for a provided account id.
- **Docker Compose (`docker-compose.yml`)**: Builds/runs backend, frontend, and Nginx reverse proxy with stable container names (e.g., `fintracker-backend`).
- **Automation (`scripts/run_valuations.sh`)**: Helper executed inside the backend container to load `.env` (when present) and run the snapshot job.
- **Infrastructure**: Deployed to EC2; a systemd timer on the host runs `docker exec fintracker-backend /app/scripts/run_valuations.sh` daily at 16:00 UTC (13:00 UTC-3) to keep valuations up to date.

## Getting Started
1. **Prerequisites**
   - Docker / Docker Compose
   - `.env` with broker credentials, AWS settings, etc. (see `.env.example`)
2. **Local run**
   ```bash
   docker-compose up -d
   docker-compose exec backend python -m backend.core.daily_snapshot
   ```
   Generated CSV/Parquet files land under `data/positions/...`.
3. **Manual valuation run inside a container**
   ```bash
   docker exec fintracker-backend /app/scripts/run_valuations.sh
   ```
   The script ensures `.env` variables are loaded and is what automation invokes.

## Fetching Santander Mutual-Fund NAV
We now ship `scripts/fetch_santander_nav.py`, which mimics Santander's SPA headers, boots a session via the public landing page, and calls `https://www.santander.com.ar/fondosInformacion/funds/<id>/detail` to retrieve `currentShareValue` (`valor de la cuotaparte`) plus its date.

- **Local run** (after `pip install -r backend/requirements.txt`):
  ```bash
  python scripts/fetch_santander_nav.py 1 2
  ```
  Include any fund ids you track (defaults to `1`). The script prints `Fund <id>: <value> (as of <timestamp>)`.
- **Inside backend container** (after `docker compose build backend` to copy the script):
  ```bash
  docker compose exec backend python /app/scripts/fetch_santander_nav.py 1
  ```
- The script deliberately warms up a session against the info landing page and sends Santander's required headers (`channel-name`, `x-ibm-client-id`, `sec-fetch-*`, etc.). Plain `curl` without those headers is rejected with “Servicio temporalmente no disponible”, so always run this helper instead of hand-rolling the request.

### Santander funds inside the daily valuation flow

`python -m backend.core.daily_snapshot` now loads manual Santander holdings before saving the daily CSV/Parquet snapshots. Add your mutual funds to `data/manual/santander_holdings.json` (override with `SANTANDER_HOLDINGS_FILE` if you prefer another path). Each entry describes how many cuotapartes you currently hold:

```json
[
  {
    "fund_id": "1",
    "symbol": "SUPERFONACC",
    "display_name": "Superfondo Acciones",
    "quantity": 123.45,
    "currency": "ARS",
    "market": "santander",
    "source": "santander"
  }
]
```

During the snapshot run we:

- merge these manual holdings with the broker feed so storage/valuations always see a single consolidated positions file;
- call the Santander SPA endpoint only for the fund ids present in the JSON and persist those NAV quotes under `data/positions/prices/...` alongside the IOL quotes;
- feed the NAV values into `compute_valuations`, so the frontend totals reflect both the brokerage account and the mutual funds you maintain outside of IOL.

Edit `quantity` whenever you buy/sell cuotapartes and the next systemd run (or a manual `docker exec ... run_valuations.sh`) will pull the latest NAV automatically.

## Testing
Backend tests live under `backend/tests/` (e.g., `test_valuations.py` verifies snapshot loading plus error handling). Run them with:
```bash
cd backend
pip install -r requirements.txt
pytest
```

## Scheduled Valuations on EC2
Systemd units (not checked into the repo) live at:
- `/etc/systemd/system/valuations.service` – runs `docker exec fintracker-backend /app/scripts/run_valuations.sh` as a oneshot job and depends on `docker.service`.
- `/etc/systemd/system/valuations.timer` – `OnCalendar=*-*-* 16:00:00`, `Persistent=true`, `Unit=valuations.service`.

Reload with `sudo systemctl daemon-reload`, then `sudo systemctl enable --now valuations.timer`. Logs: `journalctl -u valuations.service`.

## Roadmap / Next Steps
- Frontend polish: filters, sortable columns, and lightweight charts on top of the existing snapshot view.
- Streamline manual asset inputs (web form / CLI) so keeping Santander holdings up to date is simpler.
- Historical views (charts, time series storage beyond CSV/Parquet).
- Optional S3 uploads for valuations alongside positions/prices/FX snapshots.
- Alerting around failed valuation runs (CloudWatch or similar).
