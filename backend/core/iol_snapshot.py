import os
import requests
import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

BASE_URL = "https://api.invertironline.com"
TOKEN_ENDPOINT = "/token"

# Try these in order; some accounts expose only one of them
PORTFOLIO_ENDPOINTS = [
    "/api/v2/portafolio",
]

IOL_USERNAME = os.getenv("IOL_USERNAME")
IOL_PASSWORD = os.getenv("IOL_PASSWORD")


def get_bearer_tokens(username: str, password: str):
    """Authenticate with IOL and get access/refresh tokens."""
    data = {"username": username, "password": password, "grant_type": "password"}
    r = requests.post(BASE_URL + TOKEN_ENDPOINT, data=data, timeout=20)
    r.raise_for_status()
    j = r.json()
    return j["access_token"], j["refresh_token"]


def _safe_text(resp: requests.Response) -> str:
    try:
        return resp.text[:300].replace("\n", " ")
    except Exception:
        return "<no body>"


def get_positions(access_token: str):
    """
    Fetch holdings (tenencias) trying multiple endpoint/param combos.
    Returns a LIST with the raw items from all successful calls combined.
    """
    headers = {"Authorization": f"Bearer {access_token}"}

    # We try ARG, US, and no param (some deployments require/ignore 'pais')
    market_params = [
        {"pais": "argentina"},
        {"pais": "estados_unidos"},
    ]

    collected = []
    last_err = None

    for ep in PORTFOLIO_ENDPOINTS:
        url = BASE_URL + ep
        for params in market_params:
            try:
                r = requests.get(url, headers=headers, params=params, timeout=20)
                if r.status_code == 200:
                    data = r.json()
                    # Accept either {"tenencias":[...]} or {"activos":[...]} or direct list
                    items = []
                    if isinstance(data, dict):
                        if isinstance(data.get("tenencias"), list):
                            items = data["tenencias"]
                        elif isinstance(data.get("activos"), list):
                            # IOL often returns holdings under 'activos' with nested 'titulo'
                            items = data["activos"]
                        elif isinstance(data.get("portafolio"), list):
                            items = data["portafolio"]
                    elif isinstance(data, list):
                        items = data

                    if items:
                        collected.extend(items)
                else:
                    last_err = f"HTTP {r.status_code} at {url} params={params} body={_safe_text(r)}"
            except requests.RequestException as e:
                last_err = f"RequestException at {url} params={params}: {e}"

    if not collected:
        raise RuntimeError("Unable to fetch positions from IOL. Last error: " + (last_err or "unknown"))

    return collected


def extract_positions_as_df(items) -> pd.DataFrame:
    """
    Normalize raw items into a simple DataFrame with:
    symbol, description, quantity, currency, price, valuation, instrument_type
    Supports both 'tenencias' schema and 'activos' schema (with nested 'titulo').
    """
    if not isinstance(items, list):
        return pd.DataFrame()

    rows = []
    for it in items:
        # Case A: schema with nested 'titulo' (as in {'activos': [...]})
        titulo = it.get("titulo") if isinstance(it, dict) else None
        if isinstance(titulo, dict):
            symbol = titulo.get("simbolo")
            description = titulo.get("descripcion")
            currency = titulo.get("moneda")
            instr_type = titulo.get("tipo") or it.get("tipoInstrumento")
            qty = it.get("cantidad") or it.get("cantidadNominal")
            price = it.get("ultimoPrecio") or it.get("precio")
            valuation = it.get("valorizado") or it.get("valuacion") or (
                qty and price and qty * price
            )

        else:
            # Case B: flat schema ('tenencias' or variants)
            symbol = it.get("simbolo") or it.get("ticker") or it.get("codigo")
            description = it.get("descripcion")
            currency = it.get("moneda") or it.get("divisa")
            instr_type = it.get("tipoInstrumento") or it.get("instrumento") or it.get("tipo")
            qty = it.get("cantidad") or it.get("cantidadNominal")
            price = it.get("ultimoPrecio") or it.get("precio")
            valuation = it.get("valorizado") or it.get("valuacion") or (
                qty and price and qty * price
            )

        if symbol and qty:
            rows.append(
                {
                    "symbol": symbol,
                    "description": description,
                    "quantity": qty,
                    "currency": currency,
                    "price": price,
                    "valuation": valuation,
                    "instrument_type": instr_type,
                }
            )

    return pd.DataFrame(rows)


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

    if df.empty:
        print("No positions found or unexpected API format.")
        # Short peek to help debugging
        try:
            preview = {"items_len": len(raw_items), "first_item": raw_items[0] if raw_items else None}
            print("Raw preview:", str(preview)[:500])
        except Exception:
            pass
        return

    print(df)

    # Export snapshot to CSV so user can inspect all columns/rows easily
    try:
        out_csv = os.path.join(os.getcwd(), "iol_snapshot.csv")
        df.to_csv(out_csv, index=False)
        print(f"\nExported snapshot CSV to: {out_csv}")
    except Exception as e:
        print(f"Failed to export CSV: {e}")

    print("\nTotals by currency:")
    print(df.groupby("currency")["valuation"].sum())


if __name__ == "__main__":
    main()
