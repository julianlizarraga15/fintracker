# Fintracker

Personal finance tracker for pulling brokerage positions, computing valuations, and surfacing them through containerized services.

## Architecture
- **Backend (`backend/`)**: FastAPI app plus snapshot utilities (`backend/core`) that fetch positions/prices/FX, compute valuations, and write CSV/Parquet files via `python -m backend.core.iol_snapshot`. The API exposes `GET /valuations/latest?account_id=<hash>`, which streams the freshest on-disk valuation snapshot along with totals for the frontend.
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
   docker-compose exec backend python -m backend.core.iol_snapshot
   ```
   Generated CSV/Parquet files land under `data/positions/...`.
3. **Manual valuation run inside a container**
   ```bash
   docker exec fintracker-backend /app/scripts/run_valuations.sh
   ```
   The script ensures `.env` variables are loaded and is what automation invokes.

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
- Fetch mutual fund "cuotaparte" (share value) data from Santander so valuations cover those products.
- Historical views (charts, time series storage beyond CSV/Parquet).
- Optional S3 uploads for valuations alongside positions/prices/FX snapshots.
- Alerting around failed valuation runs (CloudWatch or similar).
