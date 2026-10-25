FROM python:3.12-slim

WORKDIR /app

COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY backend/app /app/backend/app
COPY backend/valuation /app/backend/valuation
COPY backend/core/__init__.py \
    backend/core/binance_common.py \
    backend/core/binance_prices.py \
    backend/core/binance_transform.py \
    backend/core/bitcoin_client.py \
    backend/core/broker_contract.py \
    backend/core/broker_lambda.py \
    backend/core/config.py \
    backend/core/crypto_holdings.py \
    backend/core/crypto_prices.py \
    backend/core/daily_snapshot.py \
    backend/core/ethereum_client.py \
    backend/core/ethereum_transform.py \
    backend/core/fx_rates.py \
    backend/core/santander_holdings.py \
    backend/core/santander_nav.py \
    backend/core/settings.py \
    backend/core/storage.py \
    /app/backend/core/
COPY backend/core/sources/__init__.py \
    backend/core/sources/binance.py \
    backend/core/sources/manual.py \
    backend/core/sources/models.py \
    backend/core/sources/wallets.py \
    /app/backend/core/sources/
COPY scripts/run_valuations.sh \
    scripts/run_valuations_with_status.sh \
    scripts/check_broker_web_config.py \
    /app/scripts/

RUN chmod +x /app/scripts/run_valuations.sh /app/scripts/run_valuations_with_status.sh

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
EXPOSE 8000
CMD ["uvicorn", "backend.app.main:app", "--host", "0.0.0.0", "--port", "8000"]
