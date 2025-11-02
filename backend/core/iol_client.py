import logging
from datetime import date, datetime
from typing import Tuple, List, Optional

import requests

from ..valuation import models as valuation_models

BASE_URL = "https://api.invertironline.com"
TOKEN_ENDPOINT = "/token"
PORTFOLIO_ENDPOINTS = ["/api/v2/portafolio"]

# Markets to query
MARKET_PARAMS = [
    {"pais": "argentina"},
    {"pais": "estados_unidos"},
]

LOG = logging.getLogger(__name__)


def _safe_text(resp: requests.Response) -> str:
    try:
        return resp.text[:300].replace("\n", " ")
    except Exception:
        return "<no body>"


def get_bearer_tokens(username: str, password: str) -> Tuple[str, str]:
    data = {"username": username, "password": password, "grant_type": "password"}
    r = requests.post(BASE_URL + TOKEN_ENDPOINT, data=data, timeout=20)
    r.raise_for_status()
    j = r.json()
    return j["access_token"], j.get("refresh_token")


def get_positions(access_token: str) -> List[dict]:
    """
    Fetch holdings from configured portfolio endpoints and markets.
    Returns a list of raw items tagged with "_market".
    """
    headers = {"Authorization": f"Bearer {access_token}"}
    collected: List[dict] = []
    last_err = None

    for ep in PORTFOLIO_ENDPOINTS:
        url = BASE_URL + ep
        for params in MARKET_PARAMS:
            market = params.get("pais") or "desconocido"
            try:
                r = requests.get(url, headers=headers, params=params, timeout=20)
                if r.status_code == 200:
                    data = r.json()
                    items = []
                    if isinstance(data, dict):
                        if isinstance(data.get("tenencias"), list):
                            items = data["tenencias"]
                        elif isinstance(data.get("activos"), list):
                            items = data["activos"]
                        elif isinstance(data.get("portafolio"), list):
                            items = data["portafolio"]
                    elif isinstance(data, list):
                        items = data

                    for it in items or []:
                        # attach market tag so we don't lose where it came from
                        if isinstance(it, dict):
                            it = dict(it)
                            it["_market"] = market
                        collected.append(it)
                else:
                    last_err = f"HTTP {r.status_code} at {url} params={params} body={_safe_text(r)}"
            except requests.RequestException as e:
                last_err = f"RequestException at {url} params={params}: {e}"

    if not collected:
        raise RuntimeError("Unable to fetch positions from IOL. Last error: " + (last_err or "unknown"))

    return collected


def _extract_symbol_and_market(it: dict) -> Optional[tuple]:
    """Return (symbol, market, instrument_type) or None if symbol missing."""
    if not isinstance(it, dict):
        return None
    # Try nested 'titulo' first
    titulo = it.get("titulo")
    if isinstance(titulo, dict):
        symbol = titulo.get("simbolo") or titulo.get("ticker")
        instr_type = titulo.get("tipo") or it.get("tipoInstrumento")
    else:
        symbol = it.get("simbolo") or it.get("ticker") or it.get("codigo")
        instr_type = it.get("tipoInstrumento") or it.get("instrumento") or it.get("tipo")

    market = it.get("_market") or it.get("mercado")
    if not symbol:
        return None
    return symbol, market, instr_type


def _guess_panel(instr_type: Optional[str]) -> str:
    """Very small heuristic to pick a panel string for the IOL Cotizaciones endpoint."""
    if not instr_type:
        return "Acciones"
    s = instr_type.lower()
    if "cedear" in s:
        return "CEDEAR"
    if "etf" in s:
        return "ETF"
    if "bon" in s or "renta" in s:
        return "RentaFija"
    if "fci" in s or "fondo" in s:
        return "Fondos"
    # default to equities panel
    return "Acciones"


def _extract_price_from_quote(q: dict) -> Optional[tuple]:
    """Try to read a numeric price and currency from a Cotizaciones response dict."""
    if not isinstance(q, dict):
        return None
    # common fields
    for fld in ("ultimo", "ultimoPrecio", "precio", "last", "valor"):
        v = q.get(fld)
        if v is not None:
            try:
                return float(v), q.get("moneda") or q.get("divisa")
            except Exception:
                continue
    return None


def get_price_for_symbol(symbol: str, market: str, panel: str, access_token: str) -> Optional[valuation_models.Price]:
    """Fetch a single quote from IOL and return a validated Price model or None if not found.

    This keeps behavior simple: build the Cotizaciones URL, GET it, and parse a best-effort price.
    """
    url = f"{BASE_URL}/api/v2/Cotizaciones/{market}/{symbol}/{panel}"
    headers = {"Authorization": f"Bearer {access_token}"}
    try:
        r = requests.get(url, headers=headers, timeout=15)
        if r.status_code != 200:
            LOG.debug("IOL cotizacion miss: %s -> %s (status=%s) body=%s", market, symbol, r.status_code, _safe_text(r))
            return None
        j = r.json()
        # The response format may vary; try to find an object with price info
        maybe = None
        if isinstance(j, dict):
            # some endpoints return a dict with 'cotizacion' or similar
            for key in ("cotizacion", "data", "quote", "instrumento"):
                if key in j and isinstance(j[key], dict):
                    maybe = j[key]
                    break
            if maybe is None:
                maybe = j
        elif isinstance(j, list) and j:
            maybe = j[0]

        res = _extract_price_from_quote(maybe or {})
        if not res:
            return None
        price_value, currency = res

        p = valuation_models.Price(
            asof_dt=date.today(),
            asof_ts=datetime.utcnow(),
            symbol=symbol,
            price_type="last",
            price=price_value,
            currency=currency or "ARS",
            venue=market,
            source="iol",
            quality_score=100,
        )
        return p
    except requests.RequestException as e:
        LOG.debug("IOL request error for %s/%s/%s: %s", market, symbol, panel, e)
        return None


def get_prices_for_positions(items: List[dict], access_token: str) -> List[valuation_models.Price]:
    """Iterate over raw position items and fetch missing prices from IOL.

    Strategy (keep it simple):
    - For each position extract symbol/market/instrument_type.
    - If the position already contains an obvious price field (ultimoPrecio, precio, ultimo), skip fetching.
    - Otherwise, guess a panel from instrument_type and call Cotizaciones endpoint.
    - Return a list of `valuation.models.Price` objects for successful fetches.
    """
    out: List[valuation_models.Price] = []
    for it in items:
        try:
            res = _extract_symbol_and_market(it)
            if not res:
                continue
            symbol, market, instr_type = res

            # quick skip if the position already has a price
            for fld in ("ultimoPrecio", "precio", "ultimo", "last"):
                if (isinstance(it.get(fld), (int, float)) or (isinstance(it.get(fld), str) and it.get(fld).replace('.', '', 1).isdigit())):
                    # We already have a price; build a Price model from it instead of fetching
                    try:
                        price_val = float(it.get(fld))
                    except Exception:
                        price_val = None
                    if price_val is not None:
                        p = valuation_models.Price(
                            asof_dt=date.today(),
                            asof_ts=datetime.utcnow(),
                            symbol=symbol,
                            price_type="last",
                            price=price_val,
                            currency=it.get("moneda") or it.get("divisa") or "ARS",
                            venue=market,
                            source="iol",
                            quality_score=100,
                        )
                        out.append(p)
                        break

            else:
                panel = _guess_panel(instr_type)
                p = get_price_for_symbol(symbol, market or "argentina", panel, access_token)
                if p:
                    out.append(p)
        except Exception:
            LOG.exception("unexpected error while fetching price for position: %r", it)

    return out
