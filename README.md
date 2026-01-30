# Fintracker

Personal finance tracker for pulling brokerage positions, computing valuations, and surfacing them through containerized services.

## Architecture
- **Backend (`backend/`)**: FastAPI app plus snapshot utilities (`backend/core`) that fetch positions/prices/FX, compute valuations, and write CSV/Parquet files via `python -m backend.core.daily_snapshot`. The API exposes `POST /auth/login` (JWT issuance backed by env-provided demo credentials) plus `GET /valuations/latest?account_id=<hash>`, which streams the freshest on-disk valuation snapshot along with totals for the frontend.
- **Frontend (`frontend/`)**: Static site served through Nginx that queries `/api/valuations/latest` and renders the latest snapshot (status chip, totals, grouped-by-symbol positions with per-custodian details, and % of portfolio per position) for a provided account id.
- **Docker Compose (`docker-compose.yml`)**: Builds/runs backend, frontend, and Nginx reverse proxy with stable container names (e.g., `fintracker-backend`).
- **Automation (`scripts/run_valuations.sh`)**: Helper executed inside the backend container to load `.env` (when present) and run the snapshot job.
- **Infrastructure**: Deployed to EC2; a systemd timer on the host runs `docker exec fintracker-backend /app/scripts/run_valuations.sh` daily at 16:00 UTC (13:00 UTC-3) to keep valuations up to date.

## Deploy (GitHub Actions + SSM)
- CI/CD builds/pushes images to ECR, then SSM runs `git pull`, `docker-compose pull/down/up`, and restarts the stack on EC2. It sets `HOME=/home/ec2-user` and marks `/home/ec2-user/fintracker` as a Git safe.directory so pulls work.
- On EC2, always run `docker-compose` from the repo root so the backend bind mount `./data:/app/data` is applied and snapshots write to disk.

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

## Binance Spot Balances
- Enable with `.env`: set `ENABLE_BINANCE=1`, `BINANCE_API_KEY`, `BINANCE_API_SECRET` (optionally override `BINANCE_BASE_URL` or `BINANCE_RECV_WINDOW_MS`).
- Run `python -m backend.core.daily_snapshot` (locally or via `docker compose exec backend ...`) to pull Binance spot balances alongside IOL and manual holdings.
- Only spot balances are fetched; keep keys scoped accordingly.

## MetaMask / Ethereum Wallet Balances
- Enable with `.env`: set `ENABLE_ETHEREUM=true`, `ETHERSCAN_API_KEY`, and `ETHEREUM_WALLET_ADDRESSES` (comma-separated list of 0x... addresses).
- Automatically fetches native ETH and ERC-20 token balances using the Etherscan API.
- Token balances are automatically adjusted for decimals and priced alongside other crypto assets.

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

## Public Demo Auth + TLS
- **Configure secrets via env/secret store**: set `DEMO_AUTH_USERNAME`, `DEMO_AUTH_PASSWORD`, `JWT_SECRET`, and optionally override `JWT_EXPIRES_MINUTES` before the backend container boots. Keep these out of git and rotate `JWT_SECRET` if compromised.
- **Frontend auth flow**: the SPA now shows a login form, calls `/api/auth/login`, keeps the JWT in memory, attaches it to `/api/valuations/latest` fetches, and clears it on any `401` so the user is prompted to log back in.
- **Nginx with TLS**: the `nginx` service listens on `80` (HTTP) and `443` (HTTPS). Port 80 only redirects to HTTPS. Place your Let’s Encrypt certs/keys under `./tls` (ignored by git) so they mount at `/etc/nginx/tls/` and keep the actual domain (e.g., `$PUBLIC_DOMAIN`) outside of the repo. A typical host setup:
  ```bash
  export PUBLIC_DOMAIN=demo.example.com
  sudo certbot certonly --standalone -d "$PUBLIC_DOMAIN"
  sudo mkdir -p tls
  sudo cp /etc/letsencrypt/live/"$PUBLIC_DOMAIN"/fullchain.pem tls/
  sudo cp /etc/letsencrypt/live/"$PUBLIC_DOMAIN"/privkey.pem tls/
  docker compose up -d nginx
  ```
  `nginx.conf` forwards `Authorization: Bearer ...` headers on `/api/` so the backend can verify JWTs.

## Roadmap / Next Steps
- Frontend polish: filters, sortable columns, and lightweight charts on top of the existing snapshot view.
- Streamline manual asset inputs (web form / CLI) so keeping Santander holdings up to date is simpler.
- Historical views (charts, time series storage beyond CSV/Parquet).
- Optional S3 uploads for valuations alongside positions/prices/FX snapshots.
- Alerting around failed valuation runs (CloudWatch or similar).
- Add JWT auth for the public demo: FastAPI login endpoint that checks env-provided username/password, issues a short-lived JWT signed with `JWT_SECRET`, and enforces `Authorization: Bearer` on protected routes.
- Wire the frontend to show a login form, call `/api/auth/login`, keep the JWT in memory (or localStorage if you accept the risk), attach it on fetches, and handle 401 by re-prompting.
- Put Nginx on a public host with DNS + TLS (certbot), proxy `/api` to the backend and `/` to the frontend, and forward the `Authorization` header.
- Keep secrets and domains out of git; set `JWT_SECRET`, `JWT_EXPIRES_MINUTES`, allowed creds, and the public domain via env/secret store at deploy time.

## Manual crypto holdings (prep for BTC/ETH)
- To start tracking crypto balances manually, add `data/manual/crypto_holdings.json` (or point `CRYPTO_HOLDINGS_FILE` to another path).
- Schema per entry: `symbol` (e.g. `"BTC"`), `quantity`, optional `display_name`, `currency` (defaults to USD), `market` (e.g. `"crypto"`), `source` (e.g. `"manual"` or the wallet name), `account_id` (falls back to env-derived account id).
- Example:
  ```json
  [
    { "symbol": "BTC", "quantity": 0.25, "display_name": "BTC (Exodus)", "market": "crypto", "source": "exodus" },
    { "symbol": "ETH", "quantity": 3.1,  "display_name": "ETH (Binance)", "market": "crypto", "source": "binance" }
  ]
  ```
- BTC/ETH prices are fetched via CoinGecko simple price and folded into the daily snapshot and valuations.
