# Fintracker

Personal finance tracker for pulling brokerage positions, computing valuations, and surfacing them through containerized services.

## Architecture
- **Backend (`backend/`)**: FastAPI app plus snapshot utilities (`backend/core`) that fetch positions/prices/FX, compute valuations, and write CSV/Parquet files via `python -m backend.core.daily_snapshot`. The API exposes `POST /auth/login` (JWT issuance backed by env-provided demo credentials) plus `GET /valuations/latest?account_id=<hash>`, which streams the freshest on-disk valuation snapshot along with totals for the frontend.
- **Frontend (`frontend/`)**: Static site served through Nginx that queries `/api/valuations/latest` and renders the latest snapshot (status chip, totals, grouped-by-symbol positions with per-custodian details, and % of portfolio per position) for a provided account id.
- **Docker Compose**: `docker-compose.isolated.yml` manages the Fintracker backend and frontend. `docker-compose.proxy.yml` declares the Nginx proxy shared with watering-system. Both reuse the existing `fintracker_default` network; the proxy also joins `watering-proxy`.
- **Automation (`scripts/run_valuations.sh`)**: Helper executed inside the backend container to run the snapshot job. Production disables dotenv and loads approved web configuration from SSM.
- **Infrastructure**: Deployed to EC2; a systemd timer on the host runs `docker exec fintracker-backend /app/scripts/run_valuations.sh` daily at 16:00 UTC (13:00 UTC-3) to keep valuations up to date.

## Deploy (GitHub Actions + SSM)
- CI/CD tests, builds, and pushes uniquely tagged images to ECR. SSM runs the reviewed `scripts/deploy_isolated_web.sh` from that exact commit and recreates only changed Fintracker services with the isolated Compose file.
- Backend deployment pauses the valuation timer, drains the current job, acquires the shared job lock, and checks health before restoring the timer. The previous image remains available for rollback.
- The release helper validates and reloads the existing Nginx proxy. Proxy recreation, network removal, Compose `down`, and orphan removal are outside this release path because watering-system shares the host and proxy. See [shared proxy operations](docs/shared-proxy.md).

### Loading secrets from SSM Parameter Store

The web backend accepts only the explicit setting names in `backend/core/settings.py` under `/fintracker/web`. Create sensitive web settings such as `/fintracker/web/JWT_SECRET` as `SecureString` in the AWS console. Keep credential values out of CLI arguments and shell history.

On EC2, the backend uses the reviewed nonsecret `.isolation-bootstrap` file and disables dotenv. Its bootstrap includes:

```env
SSM_ENV_PATH=/fintracker/web
AWS_REGION=us-east-1
AWS_DEFAULT_REGION=us-east-1
PYTHON_DOTENV_DISABLED=1
```

The EC2 instance role needs `ssm:GetParameters` for the exact approved web parameter names, plus a scoped decrypt grant if those parameters use a customer-managed KMS key. It must not have access to broker credentials. The backend refuses startup when old brokerage credentials are present in its environment.

The production credential migration is complete. Follow [the broker isolation runbook](docs/broker-credential-isolation.md) for current deployment and rollback procedures. The old `/fintracker/prod` loading path is rejected.

## Getting started

Use the browser workflow in [Testing](#testing) for a local app with synthetic data. Production uses the isolated release workflow described above. The old default `docker-compose.yml` and `scripts/local_build_and_up.sh` were retired because they used the legacy deployment path.

To request a new production valuation, use the authenticated Jobs page. The existing scheduled service invokes the same helper inside the backend container:

```bash
docker exec fintracker-backend /app/scripts/run_valuations.sh
```

Generated CSV and Parquet files persist in the mounted `data/positions` directory. Do not put broker credentials in the web configuration.

## Inspecting Saved IOL Instrument Types

From the project directory on EC2, read the latest saved IOL positions using:

```bash
./scripts/inspect_iol_types.sh
```

This prints the CSV path, symbols, and instrument labels without contacting IOL or changing files. Locally, use `python3 scripts/inspect_iol_types.py`; pass `--root <snapshot-storage-directory>` to inspect another storage location.

## Binance Spot Balances
- Set `ENABLE_BINANCE=1` and `BINANCE_BALANCE_LAMBDA=fintracker-fetch-binance-balance` in approved web configuration. Binance credentials stay inside the existing reader in `sa-east-1`.
- Run the valuation helper inside the existing backend container to pull Binance spot balances alongside IOL and manual holdings.
- Only spot balances are fetched; keep keys scoped accordingly.

## IOL Positions
- Set `ENABLE_IOL=1`, `IOL_SOURCE_LAMBDA=fintracker-fetch-iol-data`, and `BROKER_LAMBDA_REGION=us-east-1` after deploying the isolated reader.
- IOL login, both portfolio markets, and authenticated quote requests run inside Lambda. EC2 receives normalized positions and prices without bearer or refresh tokens.

## PPI Positions
- Set `ENABLE_PPI=1`, `PPI_SOURCE_LAMBDA=fintracker-fetch-ppi-data`, and `BROKER_LAMBDA_REGION=us-east-1` after deploying the isolated reader. Configure credentials and optional `PPI_ACCOUNT_NUMBER` on the worker side using [the runbook](docs/broker-credential-isolation.md).
- Quick read-only check:
  ```bash
  .venv/bin/python scripts/fetch_ppi_positions.py
  ```
- This utility and daily snapshots invoke the fixed PPI reader. ARS valuation and local snapshot storage continue through the existing FX flow.

## MetaMask / Ethereum Wallet Balances
- In approved web configuration, set `ENABLE_ETHEREUM=true`, `ETHERSCAN_API_KEY`, and `ETHEREUM_WALLET_ADDRESSES` (comma-separated list of 0x... addresses).
- Automatically fetches native ETH and ERC-20 token balances using the Etherscan API.
- Token balances are automatically adjusted for decimals and priced alongside other crypto assets.

## Fetching Santander Mutual-Fund NAV
We now ship `scripts/fetch_santander_nav.py`, which mimics Santander's SPA API headers and calls `https://www.santander.com.ar/fondosInformacion/funds/<id>/detail` directly to retrieve `currentShareValue` (`valor de la cuotaparte`) plus its date.

- **Local run** (after `pip install -r backend/requirements.txt`):
  ```bash
  python scripts/fetch_santander_nav.py 1 2
  ```
  Include any fund ids you track (defaults to `1`). The script prints `Fund <id>: <value> (as of <timestamp>)`.
- The script sends Santander's required API headers (`channel-name`, `x-ibm-client-id`, `sec-fetch-*`, etc.) without preloading cookies from the info landing page. Plain `curl` without those headers is rejected with “Servicio temporalmente no disponible”, so always run this helper instead of hand-rolling the request.

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
Backend tests live under `backend/tests/`. Run the suite in Docker so local runs use the same runtime as the verified test environment:

```bash
scripts/run_tests_docker.sh
```

This builds a dedicated test image with `backend/` and `scripts/`. The root `.dockerignore` excludes environment files, local financial data, and credentials from the build context. If you are using a native Linux environment with dependencies installed, `scripts/run_tests.sh` runs the suite directly in local Python.

Browser workflows run against the real frontend and API with synthetic credentials and an isolated data directory. Install Node dependencies and Chromium with `npm install` and `npx playwright install --with-deps chromium`, then run `npm run test:e2e`. Playwright retains the HTML report, screenshots, videos, traces, backend log, and inspectable fixture data under `artifacts/e2e/`; the tests do not read `.env` or connect to external financial services.

## Scheduled Valuations on EC2
Systemd units (not checked into the repo) live at:
- `/etc/systemd/system/valuations.service` – runs `docker exec fintracker-backend /app/scripts/run_valuations.sh` as a oneshot job and depends on `docker.service`.
- `/etc/systemd/system/valuations.timer` – `OnCalendar=*-*-* 16:00:00`, `Persistent=true`, `Unit=valuations.service`.

   Reload with `sudo systemctl daemon-reload`, then `sudo systemctl enable --now valuations.timer`. Logs: `journalctl -u valuations.service`.

The authenticated job dashboard can also trigger an ad hoc valuation run with `POST /api/jobs/valuations/run`. The trigger returns immediately, uses the same on-disk job status files as scheduled runs, and rejects overlapping runs.

## Public Demo Auth + TLS
- **Configure secrets via env/secret store**: set `DEMO_AUTH_USERNAME`, `DEMO_AUTH_PASSWORD`, `JWT_SECRET`, and optionally override `JWT_EXPIRES_MINUTES` before the backend container boots. Keep these out of git and rotate `JWT_SECRET` if compromised.
- **Frontend auth flow**: the SPA now shows a login form, calls `/api/auth/login`, keeps the JWT in `sessionStorage` for the current browser tab, attaches it to authenticated fetches, and clears it on any `401` so the user is prompted to log back in.
- **Nginx with TLS**: the shared `nginx` service listens on ports `80` and `443`. It mounts the existing host `/etc/letsencrypt` directory read-only. `nginx.conf` selects the existing Fintracker and watering certificates and forwards authorization headers on `/api/`. Preserve those mounts, certificates, and both external networks during maintenance. See [shared proxy operations](docs/shared-proxy.md) before changing the proxy.

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
