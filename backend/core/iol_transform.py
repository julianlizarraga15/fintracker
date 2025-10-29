import pandas as pd
from .config import ACCOUNT_ID


def extract_positions_as_df(items) -> pd.DataFrame:
    """
    Normalize to: symbol, description, quantity, currency, price, valuation, instrument_type, market.
    Supports both 'activos' (with nested 'titulo') and flat 'tenencias' shapes returned by the IOL portfolio endpoints.
    """
    if not isinstance(items, list):
        return pd.DataFrame()

    rows = []
    for it in items:
        if not isinstance(it, dict):
            continue

        market = it.get("_market")

        # A) Schema with nested 'titulo'
        titulo = it.get("titulo")
        if isinstance(titulo, dict):
            symbol = titulo.get("simbolo")
            description = titulo.get("descripcion")
            currency = titulo.get("moneda")
            instr_type = titulo.get("tipo") or it.get("tipoInstrumento")
            qty = it.get("cantidad") or it.get("cantidadNominal")
            price = it.get("ultimoPrecio") or it.get("precio")
            valuation = (
                it.get("valorizado")
                or it.get("valuacion")
                or (qty and price and qty * price)
            )
        else:
            # B) Flat schema
            symbol = it.get("simbolo") or it.get("ticker") or it.get("codigo")
            description = it.get("descripcion")
            currency = it.get("moneda") or it.get("divisa")
            instr_type = it.get("tipoInstrumento") or it.get("instrumento") or it.get("tipo")
            qty = it.get("cantidad") or it.get("cantidadNominal")
            price = it.get("ultimoPrecio") or it.get("precio")
            valuation = (
                it.get("valorizado")
                or it.get("valuacion")
                or (qty and price and qty * price)
            )

        # Only include rows with a symbol and a non-zero quantity
        try:
            has_qty = qty is not None and float(qty) != 0.0
        except Exception:
            has_qty = False

        if symbol and has_qty:
            rows.append({
                "symbol": symbol,
                "description": description,
                "quantity": float(qty),
                "currency": currency,
                "price": float(price) if price is not None else None,
                "valuation": float(valuation) if valuation is not None else None,
                "instrument_type": instr_type,
                "market": market,
                "source": "iol",
                "account_id": ACCOUNT_ID,
            })

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    # dedupe / consolidate (protects against API duplicates)
    df = (
        df.groupby(
            ["symbol", "currency", "instrument_type", "market", "source", "account_id"],
            as_index=False,
        )
        .agg({
            "description": "last",
            "price": "last",
            "quantity": "sum",
            "valuation": "sum",
        })
    )

    # reorder columns for predictable output
    cols = [
        "symbol", "description", "instrument_type", "market", "source", "account_id",
        "currency", "quantity", "price", "valuation",
    ]
    df = df.reindex(columns=cols)
    return df
