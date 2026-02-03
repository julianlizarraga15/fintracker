from __future__ import annotations

import json
from datetime import datetime, date, timezone
from math import nan
from typing import Any

from backend.core.binance_client import get_account_balances
from backend.core.binance_prices import fetch_binance_prices
from backend.core.binance_transform import balances_to_df
from backend.core.ethereum_client import get_all_balances as get_ethereum_balances
from backend.core.bitcoin_client import get_all_btc_balances
from backend.core.ethereum_transform import ethereum_balances_to_df
from backend.core.config import (
    ACCOUNT_ID,
    BINANCE_BALANCE_LAMBDA,
    BINANCE_API_KEY,
    BINANCE_API_SECRET,
    BINANCE_BASE_URL,
    BINANCE_RECV_WINDOW_MS,
    ENABLE_BINANCE,
    ENABLE_ETHEREUM,
    ENABLE_EXODUS,
    ETHEREUM_WALLET_ADDRESSES,
    EXODUS_ETH_ADDRESSES,
    EXODUS_BTC_ADDRESSES,
    IOL_PASSWORD,
    IOL_USERNAME,
)
from backend.core.iol_client import get_bearer_tokens, get_positions
from backend.core.iol_transform import extract_positions_as_df
from backend.core.storage import save_snapshot_files, maybe_upload_to_s3
from backend.core.iol_client import get_prices_for_positions, get_fx_rates
from backend.core.santander_holdings import SantanderHolding, load_holdings
from backend.core.santander_nav import fetch_share_values as fetch_santander_nav_values
from backend.core.crypto_holdings import CryptoHolding, load_holdings as load_crypto_holdings
from backend.core.crypto_prices import fetch_simple_prices as fetch_crypto_prices
from backend.valuation.models import (
    FXRate,
    Position as PositionModel,
    Price as PriceModel,
    compute_valuations,
)
import pandas as pd


def _print_section(title: str) -> None:
    print(f"\n{title}")
    print("-" * len(title))


def _log_snapshot_paths(resource: str, info: dict) -> None:
    csv_path = info.get("csv")
    parquet_path = info.get("parquet")
    if csv_path:
        print(f"[{resource}] CSV saved -> {csv_path}")
    if parquet_path:
        print(f"[{resource}] Parquet saved -> {parquet_path}")

BASE_CURRENCY = "USD"

POSITION_COLUMNS = [
    "symbol",
    "description",
    "instrument_type",
    "market",
    "source",
    "account_id",
    "currency",
    "quantity",
    "price",
    "valuation",
]

USDT_USD_RATE = 1.0


def _fetch_binance_balances_from_lambda(function_name: str) -> list[dict[str, Any]]:
    try:
        import boto3
        from botocore.exceptions import BotoCoreError, ClientError
    except Exception as exc:
        raise RuntimeError("boto3 is required to invoke the Binance balance Lambda.") from exc

    try:
        client = boto3.client("lambda", region_name="sa-east-1")
        response = client.invoke(FunctionName=function_name, InvocationType="RequestResponse")
    except (BotoCoreError, ClientError) as exc:
        raise RuntimeError(f"Error invoking Lambda '{function_name}': {exc}") from exc

    status_code = response.get("StatusCode")
    payload_stream = response.get("Payload")
    payload_text = payload_stream.read().decode("utf-8") if payload_stream else ""

    if status_code and status_code >= 400:
        raise RuntimeError(f"Lambda '{function_name}' returned status {status_code}: {payload_text[:200]}")

    if not payload_text:
        return []

    try:
        payload_json = json.loads(payload_text)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Unexpected Lambda payload from '{function_name}': {payload_text[:200]}"
        ) from exc

    if isinstance(payload_json, dict) and "statusCode" in payload_json and payload_json.get("statusCode", 200) >= 400:
        raise RuntimeError(
            f"Lambda '{function_name}' responded with status {payload_json.get('statusCode')}: {payload_text[:200]}"
        )

    if isinstance(payload_json, dict) and "body" in payload_json:
        body_content = payload_json.get("body")
        try:
            payload_json = json.loads(body_content) if isinstance(body_content, str) else body_content
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"Unexpected Lambda body from '{function_name}': {str(body_content)[:200]}"
            ) from exc

    if not isinstance(payload_json, list):
        raise RuntimeError(
            f"Lambda '{function_name}' returned unexpected payload type: {type(payload_json).__name__}"
        )

    return payload_json


def _parse_nav_timestamp(raw_value: str | None) -> tuple[date, datetime | None]:
    if not raw_value:
        return date.today(), None
    sanitized = raw_value.replace("Z", "+00:00")
    try:
        ts = datetime.fromisoformat(sanitized)
    except ValueError:
        return date.today(), None
    return ts.date(), ts


def _manual_positions_df(holdings: list[SantanderHolding]) -> pd.DataFrame:
    rows = []
    for holding in holdings:
        price = holding.get("price")
        valuation = holding.get("valuation")
        rows.append(
            {
                "symbol": holding["symbol"],
                "description": holding.get("display_name"),
                "instrument_type": "fci",
                "market": holding["market"],
                "source": holding["source"],
                "account_id": holding["account_id"],
                "currency": holding["currency"],
                "quantity": holding["quantity"],
                "price": price if price is not None else nan,
                "valuation": valuation if valuation is not None else nan,
            }
        )

    df_manual = pd.DataFrame(rows)
    if df_manual.empty:
        return df_manual
    return df_manual.reindex(columns=POSITION_COLUMNS)


def _crypto_positions_df(holdings: list[CryptoHolding]) -> pd.DataFrame:
    rows = []
    for holding in holdings:
        rows.append(
            {
                "symbol": holding["symbol"],
                "description": holding.get("display_name"),
                "instrument_type": "crypto",
                "market": holding["market"],
                "source": holding["source"],
                "account_id": holding["account_id"],
                "currency": holding["currency"],
                "quantity": holding["quantity"],
                "price": nan,
                "valuation": nan,
            }
        )

    df_manual = pd.DataFrame(rows)
    if df_manual.empty:
        return df_manual
    return df_manual.reindex(columns=POSITION_COLUMNS)


def main():
    if not IOL_USERNAME or not IOL_PASSWORD:
        print("Missing IOL_USERNAME or IOL_PASSWORD in .env")
        return

    access_token, _ = get_bearer_tokens(IOL_USERNAME, IOL_PASSWORD)

    try:
        raw_items = get_positions(access_token)
    except Exception as e:
        print(f"Error fetching positions: {e}")
        return

    df = extract_positions_as_df(raw_items)
    # set account_id column if present
    if not df.empty:
        df["account_id"] = ACCOUNT_ID
    elif raw_items:
        # Fallback: keep raw positions if the parser returned empty
        fallback_rows = []
        for item in raw_items:
            if not isinstance(item, dict):
                continue
            symbol = item.get("simbolo") or item.get("ticker") or item.get("codigo")
            qty = item.get("cantidad") or item.get("cantidadNominal")
            try:
                qty_val = float(qty) if qty is not None else 0.0
            except (TypeError, ValueError):
                qty_val = 0.0
            if not symbol or qty_val <= 0:
                continue
            price_val = item.get("ultimoPrecio") or item.get("precio")
            try:
                price_coerced = float(price_val) if price_val is not None else nan
            except (TypeError, ValueError):
                price_coerced = nan
            fallback_rows.append(
                {
                    "symbol": symbol,
                    "description": item.get("descripcion"),
                    "instrument_type": item.get("tipoInstrumento") or item.get("instrumento") or item.get("tipo"),
                    "market": item.get("_market") or item.get("mercado"),
                    "source": "iol",
                    "account_id": ACCOUNT_ID,
                    "currency": None,
                    "quantity": qty_val,
                    "price": price_coerced,
                    "valuation": nan,
                }
            )
        if fallback_rows:
            df = pd.DataFrame(fallback_rows).reindex(columns=POSITION_COLUMNS)

    santander_holdings = load_holdings()
    holdings_by_fund_id: dict[str, list[SantanderHolding]] = {}
    if santander_holdings:
        manual_df = _manual_positions_df(santander_holdings)
        if not manual_df.empty:
            if not df.empty:
                combined_rows = df.to_dict(orient="records")
                combined_rows.extend(manual_df.to_dict(orient="records"))
                df = pd.DataFrame(combined_rows).reindex(columns=POSITION_COLUMNS)
            else:
                df = manual_df
        for holding in santander_holdings:
            holdings_by_fund_id.setdefault(holding["fund_id"], []).append(holding)
        if santander_holdings:
            print(f"Loaded {len(santander_holdings)} Santander manual holdings.")

    crypto_holdings = load_crypto_holdings()
    crypto_symbols: set[str] = set()
    if crypto_holdings:
        crypto_df = _crypto_positions_df(crypto_holdings)
        if not crypto_df.empty:
            if not df.empty:
                combined_rows = df.to_dict(orient="records")
                combined_rows.extend(crypto_df.to_dict(orient="records"))
                df = pd.DataFrame(combined_rows).reindex(columns=POSITION_COLUMNS)
            else:
                df = crypto_df
        crypto_symbols = {holding["symbol"] for holding in crypto_holdings}
        print(f"Loaded {len(crypto_holdings)} crypto manual holdings.")

    binance_symbols: set[str] = set()
    binance_balances: list[dict[str, Any]] = []
    if ENABLE_BINANCE:
        try:
            if BINANCE_BALANCE_LAMBDA:
                binance_balances = _fetch_binance_balances_from_lambda(BINANCE_BALANCE_LAMBDA)
            elif BINANCE_API_KEY and BINANCE_API_SECRET:
                binance_balances = get_account_balances(
                    api_key=BINANCE_API_KEY,
                    api_secret=BINANCE_API_SECRET,
                    base_url=BINANCE_BASE_URL,
                    recv_window_ms=BINANCE_RECV_WINDOW_MS,
                )
            else:
                print("ENABLE_BINANCE set but missing BINANCE_API_KEY or BINANCE_API_SECRET.")

            if binance_balances:
                binance_df = balances_to_df(binance_balances, position_columns=POSITION_COLUMNS)
                if not binance_df.empty:
                    if not df.empty:
                        combined_rows = df.to_dict(orient="records")
                        combined_rows.extend(binance_df.to_dict(orient="records"))
                        df = pd.DataFrame(combined_rows).reindex(columns=POSITION_COLUMNS)
                    else:
                        df = binance_df
                    binance_symbols = set(binance_df["symbol"])
                    print(f"Loaded {len(binance_symbols)} Binance assets.")
        except Exception as e:
            print(f"Error fetching Binance balances: {e}")

    ethereum_symbols: set[str] = set()
    if ENABLE_ETHEREUM:
        try:
            addresses = [a.strip() for a in ETHEREUM_WALLET_ADDRESSES.split(",") if a.strip()]
            if addresses:
                eth_balances = get_ethereum_balances(addresses)
                if eth_balances:
                    eth_df = ethereum_balances_to_df(eth_balances, position_columns=POSITION_COLUMNS)
                    if not eth_df.empty:
                        if not df.empty:
                            combined_rows = df.to_dict(orient="records")
                            combined_rows.extend(eth_df.to_dict(orient="records"))
                            df = pd.DataFrame(combined_rows).reindex(columns=POSITION_COLUMNS)
                        else:
                            df = eth_df
                        ethereum_symbols = set(eth_df["symbol"])
                        print(f"Loaded {len(ethereum_symbols)} Ethereum assets.")
            else:
                print("ENABLE_ETHEREUM set but ETHEREUM_WALLET_ADDRESSES is empty.")
        except Exception as e:
            print(f"Error fetching Ethereum balances: {e}")

    exodus_symbols: set[str] = set()
    if ENABLE_EXODUS:
        # Exodus ETH
        try:
            eth_addresses = [a.strip() for a in EXODUS_ETH_ADDRESSES.split(",") if a.strip()]
            if eth_addresses:
                eth_balances = get_ethereum_balances(eth_addresses, source="exodus")
                if eth_balances:
                    eth_df = ethereum_balances_to_df(eth_balances, position_columns=POSITION_COLUMNS)
                    if not eth_df.empty:
                        if not df.empty:
                            combined_rows = df.to_dict(orient="records")
                            combined_rows.extend(eth_df.to_dict(orient="records"))
                            df = pd.DataFrame(combined_rows).reindex(columns=POSITION_COLUMNS)
                        else:
                            df = eth_df
                        exodus_symbols.update(eth_df["symbol"])
                        print(f"Loaded {len(eth_balances)} Exodus Ethereum assets.")
        except Exception as e:
            print(f"Error fetching Exodus Ethereum balances: {e}")

        # Exodus BTC
        try:
            btc_addresses = [a.strip() for a in EXODUS_BTC_ADDRESSES.split(",") if a.strip()]
            if btc_addresses:
                btc_balances = get_all_btc_balances(btc_addresses, source="exodus")
                if btc_balances:
                    # Reuse ethereum_balances_to_df for simplicity as it's just a mapper
                    # but we'll manually fix the display name if needed or just let it be
                    btc_df = ethereum_balances_to_df(btc_balances, position_columns=POSITION_COLUMNS)
                    if not btc_df.empty:
                        if not df.empty:
                            combined_rows = df.to_dict(orient="records")
                            combined_rows.extend(btc_df.to_dict(orient="records"))
                            df = pd.DataFrame(combined_rows).reindex(columns=POSITION_COLUMNS)
                        else:
                            df = btc_df
                        exodus_symbols.update(btc_df["symbol"])
                        print(f"Loaded {len(btc_balances)} Exodus Bitcoin assets.")
        except Exception as e:
            print(f"Error fetching Exodus Bitcoin balances: {e}")

    if df.empty:
        print("No positions found or unexpected API format.")
        try:
            preview = {"items_len": len(raw_items), "first_item": raw_items[0] if raw_items else None}
            print("Raw preview:", str(preview)[:500])
        except Exception:
            pass
        return

    _print_section(f"Positions ({df.shape[0]} rows x {df.shape[1]} cols)")
    with pd.option_context(
        "display.max_columns",
        None,
        "display.width",
        140,
        "display.float_format",
        lambda val: f"{val:,.2f}",
    ):
        print(df.to_string(index=False))

    # Save snapshot files (partitioned by date/source/account)
    info = save_snapshot_files(df)
    _print_section("Positions snapshot saved")
    _log_snapshot_paths("positions", info)

    # Optional: upload to S3 if configured
    upload_csv = False
    if upload_csv:
        maybe_upload_to_s3([info.get("csv"), info.get("parquet")], info.get("dt"))
    else:
        maybe_upload_to_s3([info.get("parquet")], info.get("dt"))

    # Quick totals by currency
    try:
        totals = df.groupby("currency")["valuation"].sum().dropna()
        if not totals.empty:
            _print_section("Totals by currency")
            formatted_totals = totals.apply(lambda val: f"{val:,.2f}")
            print(formatted_totals.to_string())
    except Exception:
        pass

    # Fetch and persist prices for the current positions (simple inline behavior)
    prices: list[PriceModel] = []
    try:
        prices = get_prices_for_positions(raw_items, access_token)
    except Exception as e:
        print(f"Error fetching prices from IOL: {e}")

    nav_display_names: dict[str, str] = {}
    santander_price_models: list[PriceModel] = []
    if holdings_by_fund_id:
        try:
            fund_ids = list(holdings_by_fund_id.keys())
            nav_rows = fetch_santander_nav_values(fund_ids)
            for nav_row in nav_rows:
                fund_id = nav_row["fund_id"]
                holdings = holdings_by_fund_id.get(fund_id) or []
                asof_dt, asof_ts = _parse_nav_timestamp(nav_row.get("current_share_value_date"))
                for holding in holdings:
                    santander_price_models.append(
                        PriceModel(
                            asof_dt=asof_dt,
                            asof_ts=asof_ts,
                            symbol=holding["symbol"],
                            price_type="nav",
                            price=nav_row["current_share_value"],
                            currency=holding["currency"],
                            venue="SANTANDER",
                            source="santander_nav",
                            quality_score=95,
                        )
                    )
                    fund_name = nav_row.get("fund_name")
                    if fund_name:
                        nav_display_names[holding["symbol"]] = fund_name
        except Exception as e:
            print(f"Error fetching Santander NAV: {e}")

    if nav_display_names and not df.empty:
        for symbol, display_name in nav_display_names.items():
            df.loc[df["symbol"] == symbol, "description"] = display_name

    if santander_price_models:
        prices.extend(santander_price_models)

    crypto_price_models: list[PriceModel] = []
    if crypto_symbols:
        try:
            crypto_price_rows = fetch_crypto_prices(list(crypto_symbols))
            for row in crypto_price_rows:
                crypto_price_models.append(
                    PriceModel(
                        asof_dt=row["asof_dt"],
                        asof_ts=row.get("asof_ts"),
                        symbol=row["symbol"],
                        price_type="last",
                        price=row["price"],
                        currency=row["currency"],
                        venue=row["venue"],
                        source=row["source"],
                        quality_score=row["quality_score"],
                    )
                )
        except Exception as e:
            print(f"Error fetching crypto prices: {e}")

    if crypto_price_models:
        prices.extend(crypto_price_models)

    binance_price_models: list[PriceModel] = []
    binance_missing_symbols: list[str] = []
    binance_requires_usdt_fx = False
    if binance_symbols:
        try:
            binance_price_models, binance_missing_symbols = fetch_binance_prices(list(binance_symbols))
            binance_requires_usdt_fx = any(p.currency == "USDT" for p in binance_price_models)
        except Exception as e:
            print(f"Error fetching Binance prices: {e}")

    if binance_price_models:
        prices.extend(binance_price_models)
        print(f"Priced {len(binance_price_models)} Binance assets (missing {len(binance_missing_symbols)}).")
    elif binance_symbols:
        print("No Binance prices fetched.")

    # Fetch prices for Ethereum assets if not already fetched
    if ethereum_symbols:
        missing_eth_symbols = ethereum_symbols - {p.symbol for p in prices}
        if missing_eth_symbols:
            try:
                eth_price_rows = fetch_crypto_prices(list(missing_eth_symbols))
                for row in eth_price_rows:
                    prices.append(
                        PriceModel(
                            asof_dt=row["asof_dt"],
                            asof_ts=row.get("asof_ts"),
                            symbol=row["symbol"],
                            price_type="last",
                            price=row["price"],
                            currency=row["currency"],
                            venue=row["venue"],
                            source=row["source"],
                            quality_score=row["quality_score"],
                        )
                    )
                print(f"Priced {len(eth_price_rows)} Ethereum assets.")
            except Exception as e:
                print(f"Error fetching Ethereum asset prices: {e}")

    # Fetch prices for Exodus assets if not already fetched
    if exodus_symbols:
        missing_exodus_symbols = exodus_symbols - {p.symbol for p in prices}
        if missing_exodus_symbols:
            try:
                exodus_price_rows = fetch_crypto_prices(list(missing_exodus_symbols))
                for row in exodus_price_rows:
                    prices.append(
                        PriceModel(
                            asof_dt=row["asof_dt"],
                            asof_ts=row.get("asof_ts"),
                            symbol=row["symbol"],
                            price_type="last",
                            price=row["price"],
                            currency=row["currency"],
                            venue=row["venue"],
                            source=row["source"],
                            quality_score=row["quality_score"],
                        )
                    )
                print(f"Priced {len(exodus_price_rows)} Exodus assets.")
            except Exception as e:
                print(f"Error fetching Exodus asset prices: {e}")

    if prices:
        try:
            rows = [p.model_dump() for p in prices]
            df_prices = pd.DataFrame(rows)
            if not df_prices.empty:
                df_prices["account_id"] = ACCOUNT_ID
            info_prices = save_snapshot_files(df_prices, resource_name="prices")
            _print_section("Prices snapshot saved")
            _log_snapshot_paths("prices", info_prices)
            if upload_csv:
                maybe_upload_to_s3(
                    [info_prices.get("csv"), info_prices.get("parquet")],
                    info_prices.get("dt"),
                    resource_name="prices",
                )
            else:
                maybe_upload_to_s3(
                    [info_prices.get("parquet")],
                    info_prices.get("dt"),
                    resource_name="prices",
                )
        except Exception as e:
            print(f"Error saving prices snapshot: {e}")
    else:
        print("No prices fetched from IOL, Santander, crypto, or Binance sources.")

    extra_fx_rates: list[FXRate] = []
    if binance_requires_usdt_fx:
        try:
            extra_fx_rates.append(
                FXRate(
                    asof_dt=date.today(),
                    from_ccy="USDT",
                    to_ccy=BASE_CURRENCY,
                    rate=USDT_USD_RATE,
                    source="binance_usdt_peg",
                )
            )
        except Exception:
            print("Error building USDT->USD FX rate placeholder.")

    fx_rates = []
    try:
        fx_rates = get_fx_rates()
        if extra_fx_rates:
            fx_rates.extend(extra_fx_rates)
        if fx_rates:
            df_fx = pd.DataFrame([r.model_dump() for r in fx_rates])
            info_fx = save_snapshot_files(
                df_fx,
                resource_name="fx",
                source="dolarapi_blue_venta",
            )
            _print_section("FX rates saved")
            _log_snapshot_paths("fx", info_fx)
            if upload_csv:
                maybe_upload_to_s3(
                    [info_fx.get("csv"), info_fx.get("parquet")],
                    info_fx.get("dt"),
                    resource_name="fx",
                    source="dolarapi_blue_venta",
                )
            else:
                maybe_upload_to_s3(
                    [info_fx.get("parquet")],
                    info_fx.get("dt"),
                    resource_name="fx",
                    source="dolarapi_blue_venta",
                )
        else:
            print("No FX rates fetched.")
    except Exception as e:
        if extra_fx_rates:
            fx_rates.extend(extra_fx_rates)
        print(f"Error fetching/saving FX rates: {e}")

    try:
        snapshot_dt_str = info.get("dt")
        try:
            snapshot_dt = datetime.strptime(snapshot_dt_str, "%Y-%m-%d").date() if snapshot_dt_str else date.today()
        except ValueError:
            snapshot_dt = date.today()

        pos_models = []
        snapshot_ts = datetime.now(timezone.utc)
        for row in df.to_dict(orient="records"):
            symbol = row.get("symbol")
            if not symbol:
                continue
            try:
                quantity = float(row.get("quantity") or 0.0)
            except (TypeError, ValueError):
                quantity = 0.0
            if quantity <= 0:
                continue
            pos_models.append(
                PositionModel(
                    snapshot_dt=snapshot_dt,
                    snapshot_ts=snapshot_ts,
                    account_id=row.get("account_id") or ACCOUNT_ID,
                    source=row.get("source") or "unknown",
                    market=row.get("market"),
                    symbol=symbol,
                    quantity=quantity,
                    currency=row.get("currency"),
                )
            )

        if not pos_models:
            print("No positions to value.")
            return

        valuations = compute_valuations(
            pos_models,
            prices,
            fx_rates,
            base_currency=BASE_CURRENCY,
            snapshot_dt=snapshot_dt,
        )

        if valuations:
            df_valuations = pd.DataFrame([v.model_dump() for v in valuations])
            info_val = save_snapshot_files(
                df_valuations,
                resource_name="valuations",
                account_id=ACCOUNT_ID,
            )
            _print_section("Valuations saved")
            _log_snapshot_paths("valuations", info_val)
            if upload_csv:
                maybe_upload_to_s3(
                    [info_val.get("csv"), info_val.get("parquet")],
                    info_val.get("dt"),
                    resource_name="valuations",
                    account_id=ACCOUNT_ID,
                )
            else:
                maybe_upload_to_s3(
                    [info_val.get("parquet")],
                    info_val.get("dt"),
                    resource_name="valuations",
                    account_id=ACCOUNT_ID,
                )
        else:
            print("No valuations generated.")
    except Exception as e:
        print(f"Error computing/saving valuations: {e}")


if __name__ == "__main__":
    main()
