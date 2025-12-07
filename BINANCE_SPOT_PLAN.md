# Binance Spot Integration Plan

Goal: add Binance spot balances to the existing snapshot/valuation flow alongside IOL/manual sources with minimal code churn.

## Config & Secrets
- Add env vars to `backend/core/config.py` and `.env.example`: `BINANCE_API_KEY`, `BINANCE_API_SECRET`, optional `BINANCE_BASE_URL` (default `https://api.binance.com`), `BINANCE_RECV_WINDOW_MS` (e.g., 5000), and a feature flag `ENABLE_BINANCE=1`.
- Compose already loads `.env`; no compose changes needed unless you want comments.

## Binance client (`backend/core/binance_client.py`)
- Implement a small signed HTTP helper with `requests`: add `timestamp` + `recvWindow`, sign with HMAC-SHA256 over the query string using `apiSecret`.
- Endpoints: `GET /api/v3/account` (SIGNED) for balances; optional `GET /api/v3/time` to sync server time if drift occurs.
- Constants (no magic numbers): `DEFAULT_TIMEOUT`, `DEFAULT_RECV_WINDOW_MS`, `BASE_URL`.
- Parse balances: per asset, sum `free + locked`, drop zero/negative.
- Error handling similar to IOL: explicit errors for auth (401/403), rate limits (429), and request exceptions; lightweight logging.

## Transform (`backend/core/binance_transform.py`)
- Normalize balances into a DataFrame matching `POSITION_COLUMNS` in `backend/core/daily_snapshot.py`.
- Fields: `symbol` = asset upper; `instrument_type` = `crypto`; `market` = `binance`; `source` = `binance`; `account_id` = `ACCOUNT_ID`; `quantity` = `free+locked`.
- `currency`: use `USD` (pricing will be USDT/USD). Stablecoins can set `price=1.0` and `valuation=quantity`; leave others as `price=None`.
- Deduplicate/consolidate like `iol_transform.extract_positions_as_df` to avoid duplicates.

## Pricing helper (same module or sibling)
- Use public `GET /api/v3/ticker/price` for pairs: map asset `X` -> symbol `XUSDT` (fallback `XUSD`) and return `valuation.models.Price` with `currency="USDT"`, `venue="BINANCE"`, `source="binance_api"`, `price_type="last"`, and a constant `QUALITY_SCORE_BINANCE` (e.g., 90).
- Stablecoins (`USDT`, `USDC`, `BUSD`, `FDUSD`) shortcut to price `1.0` in USD/USDT without calling the API.
- Skip assets without a USDT/USD pair; record misses so valuations mark them as missing instead of failing.
- Optional fallback: CoinGecko for BTC/ETH if Binance price fails.

## Wire into `backend/core/daily_snapshot.py`
- Gate on `ENABLE_BINANCE` and presence of key/secret.
- Fetch balances -> normalize DF -> merge with existing positions DF before saving snapshots (similar to Santander/crypto merge).
- Collect Binance symbols for pricing; call the pricing helper and append returned `Price` models to `prices` with `account_id` set, keeping existing IOL/Santander/crypto logic unchanged.
- Ensure `PositionModel` creation includes Binance rows (`source="binance"`, `market="binance"`), so valuations cover them.

## Tests (`backend/tests/`)
- `test_binance_client.py`: mocked responses for happy path, zero-balance filtering, auth failure, and 429 handling.
- `test_binance_transform.py`: balances -> DataFrame columns/quantities, stablecoin price=1 path, dedupe.
- `test_binance_prices.py`: ticker parsing, stablecoin shortcut, missing pair returns empty list.
- `test_daily_snapshot_binance_integration.py`: monkeypatch Binance client to return sample balances/prices; assert merged DF contains IOL + Binance, price list includes Binance rows, valuations do not crash on mixed sources.

## Docs & Ops
- Update `README.md` with a short “Enable Binance” note: set env vars, run `python -m backend.core.daily_snapshot`, spot is the only scope fetched.
- Keep secrets in env; no repo commits.
- Add a brief log in `daily_snapshot` summarizing how many Binance assets were loaded/priced.

## Manual validation checklist
- With real keys set, run `docker compose exec backend python -m backend.core.daily_snapshot` and confirm `data/positions/...` contains `source=binance` rows.
- Spot-check a Binance asset price vs. Binance UI to ensure USDT mapping is correct.
- Verify missing pairs result in valuations with `status=missing_input` rather than crashes.
