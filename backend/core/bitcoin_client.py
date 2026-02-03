from __future__ import annotations

import logging
import requests
from typing import Any, Dict, List, Optional
from backend.core.config import BLOCKCYPHER_API_KEY

LOG = logging.getLogger(__name__)

# We'll use BlockCypher as the primary and Blockchain.info as a fallback if needed.
# BlockCypher has a better API for multiple addresses and tokens.
BLOCKCYPHER_BASE_URL = "https://api.blockcypher.com/v1/btc/main/addrs/{address}/balance"

def get_btc_balance(address: str, source: str = "exodus") -> Dict[str, Any]:
    """Fetch BTC balance for a single address using BlockCypher."""
    url = BLOCKCYPHER_BASE_URL.format(address=address)
    params = {}
    if BLOCKCYPHER_API_KEY:
        params["token"] = BLOCKCYPHER_API_KEY

    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        
        # BlockCypher returns balance in Satoshis
        satoshis = data.get("balance", 0)
        btc_balance = satoshis / 10**8
        
        return {
            "symbol": "BTC",
            "quantity": btc_balance,
            "source": source,
            "market": "crypto",
            "address": address
        }
    except Exception as e:
        LOG.error(f"Error fetching BTC balance for {address} from BlockCypher: {e}")
        # Fallback to Blockchain.info if BlockCypher fails
        return _get_btc_balance_blockchain_info(address, source)

def _get_btc_balance_blockchain_info(address: str, source: str) -> Dict[str, Any]:
    """Fallback BTC balance fetcher using Blockchain.info."""
    url = f"https://blockchain.info/balance?active={address}"
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        
        addr_data = data.get(address, {})
        satoshis = addr_data.get("final_balance", 0)
        btc_balance = satoshis / 10**8
        
        return {
            "symbol": "BTC",
            "quantity": btc_balance,
            "source": source,
            "market": "crypto",
            "address": address
        }
    except Exception as e:
        LOG.error(f"Error fetching BTC balance for {address} from Blockchain.info: {e}")
        return {
            "symbol": "BTC",
            "quantity": 0.0,
            "source": source,
            "market": "crypto",
            "address": address,
            "error": str(e)
        }

def get_all_btc_balances(addresses: List[str], source: str = "exodus") -> List[Dict[str, Any]]:
    """Fetch BTC balances for a list of addresses."""
    all_holdings = []
    for addr in addresses:
        addr = addr.strip()
        if not addr:
            continue
        balance_info = get_btc_balance(addr, source=source)
        if balance_info.get("quantity", 0) > 0:
            all_holdings.append(balance_info)
    return all_holdings
