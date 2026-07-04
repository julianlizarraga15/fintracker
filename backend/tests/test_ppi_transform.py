from __future__ import annotations

import pytest

from backend.core import ppi_transform


def test_positions_to_df_maps_cash_and_instruments(monkeypatch):
    monkeypatch.setattr(ppi_transform, "ACCOUNT_ID", "acct-ppi")
    payload = {
        "groupedAvailability": [
            {
                "currency": "ARS",
                "availability": [
                    {"name": "Pesos", "symbol": "ARS", "amount": 1000, "settlement": "INMEDIATA"},
                    {"name": "Dollars", "symbol": "USD", "amount": 0, "settlement": "INMEDIATA"},
                ],
            }
        ],
        "groupedInstruments": [
            {
                "name": "CEDEARS",
                "instruments": [
                    {
                        "ticker": "AAPL",
                        "description": "Apple",
                        "quantity": 2,
                        "price": 5000,
                        "amount": 10000,
                        "currency": "PESOS",
                    }
                ],
            }
        ],
    }

    df = ppi_transform.positions_to_df(payload)

    assert set(df["symbol"]) == {"CASH_ARS_INMEDIATA", "AAPL"}
    assert set(df["source"]) == {"ppi"}
    assert set(df["account_id"]) == {"acct-ppi"}

    cash_row = df[df["symbol"] == "CASH_ARS_INMEDIATA"].iloc[0]
    assert cash_row["instrument_type"] == "cash"
    assert cash_row["price"] == pytest.approx(1.0)

    cedear_row = df[df["symbol"] == "AAPL"].iloc[0]
    assert cedear_row["instrument_type"] == "cedear"
    assert cedear_row["currency"] == "ARS"
    assert cedear_row["price"] == pytest.approx(5000.0)
    assert cedear_row["valuation"] == pytest.approx(10000.0)


def test_prices_from_positions_builds_price_models():
    payload = {
        "groupedAvailability": [
            {
                "currency": "ARS",
                "availability": [
                    {"name": "Pesos", "symbol": "ARS", "amount": 1000, "settlement": "INMEDIATA"},
                ],
            }
        ],
        "groupedInstruments": [
            {
                "name": "BONOS",
                "instruments": [
                    {"ticker": "AL30", "quantity": 1, "price": 70.5, "currency": "DOLARES BILLETE | MEP"},
                    {"ticker": "BAD", "quantity": 1, "price": None, "currency": "ARS"},
                ],
            }
        ],
    }

    prices = ppi_transform.prices_from_positions(payload)

    assert {price.symbol for price in prices} == {"CASH_ARS_INMEDIATA", "AL30"}
    al30 = next(price for price in prices if price.symbol == "AL30")
    assert al30.price == pytest.approx(70.5)
    assert al30.currency == "USD"
    assert al30.source == "ppi_api"
