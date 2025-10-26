import os
import requests
import pandas as pd
from dotenv import load_dotenv

# Cargar variables desde .env (si existe)
load_dotenv()

URL = "https://api.invertironline.com"
ENDPOINT_TOKENS = "/token"
ENDPOINT_PORTFOLIO = "/api/v2/estadocuenta"

IOL_USERNAME = os.getenv("IOL_USERNAME")
IOL_PASSWORD = os.getenv("IOL_PASSWORD")

def get_bearer_tokens(username: str, password: str):
    """Basic auth"""
    data = {
        "username": username,
        "password": password,
        "grant_type": "password"
    }
    r = requests.post(URL + ENDPOINT_TOKENS, data=data)
    r.raise_for_status()
    j = r.json()
    return j["access_token"], j["refresh_token"]

def get_portfolio(access_token: str):
    headers = {"Authorization": f"Bearer {access_token}"}
    r = requests.get(URL + ENDPOINT_PORTFOLIO, headers=headers)
    r.raise_for_status()
    return r.json()

def extract_positions_as_df(data: dict) -> pd.DataFrame:
    """Convert response to simple DataFrame"""
    items = []
    for k in ("activos", "titulos", "cartera", "portafolio"):
        if isinstance(data.get(k), list):
            items.extend(data[k])
    if not items and isinstance(data, list):
        items = data
    rows = []
    for it in items:
        symbol = it.get("simbolo") or it.get("ticker")
        qty = it.get("cantidad") or it.get("cantidadNominal")
        currency = it.get("moneda") or it.get("divisa")
        price = it.get("ultimoPrecio") or it.get("precio")
        valuation = it.get("valuacion") or (qty and price and qty * price)
        if symbol and qty and valuation:
            rows.append({
                "symbol": symbol,
                "quantity": qty,
                "currency": currency,
                "price": price,
                "valuation": valuation
            })
    return pd.DataFrame(rows)

def main():
    if not IOL_USERNAME or not IOL_PASSWORD:
        print("Missing IOL_USERNAME or IOL_PASSWORD")
        return
    access, _ = get_bearer_tokens(IOL_USERNAME, IOL_PASSWORD)
    portfolio = get_portfolio(access)
    df = extract_positions_as_df(portfolio)
    print(df)
    print("\nTotales por moneda:")
    print(df.groupby("currency")["valuation"].sum())

if __name__ == "__main__":
    main()
