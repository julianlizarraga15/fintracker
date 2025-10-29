import requests
from typing import Tuple, List

BASE_URL = "https://api.invertironline.com"
TOKEN_ENDPOINT = "/token"
PORTFOLIO_ENDPOINTS = ["/api/v2/portafolio"]

# Markets to query
MARKET_PARAMS = [
    {"pais": "argentina"},
    {"pais": "estados_unidos"},
]


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
