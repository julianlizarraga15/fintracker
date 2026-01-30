from __future__ import annotations

import logging
import requests
from typing import Any, Dict, List, Optional
from backend.core.config import ETHERSCAN_API_KEY

LOG = logging.getLogger(__name__)

ETHERSCAN_BASE_URL = "https://api.etherscan.io/v2/api"
ETHEREUM_SOURCE = "metamask"
ETHEREUM_MARKET = "crypto"

class EtherscanAPIError(RuntimeError):
    def __init__(self, message: str, status_code: Optional[int] = None, payload: Any = None):
        super().__init__(message)
        self.status_code = status_code
        self.payload = payload

def _get_etherscan(params: Dict[str, Any], timeout: int = 15) -> Any:
    if not ETHERSCAN_API_KEY:
        raise EtherscanAPIError("Etherscan API key missing.")

    params["apikey"] = ETHERSCAN_API_KEY
    params["chainid"] = "1"  # Ethereum Mainnet
    
    try:
        resp = requests.get(ETHERSCAN_BASE_URL, params=params, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as exc:
        raise EtherscanAPIError(f"Etherscan request error: {exc}") from exc
    except ValueError as exc:
        raise EtherscanAPIError(f"Etherscan returned non-JSON: {exc}") from exc

    if data.get("status") != "1" and data.get("message") != "No transactions found":
        # "0" status usually means error, but "No transactions found" is fine for tokens
        raise EtherscanAPIError(f"Etherscan API error: {data.get('result') or data.get('message')}", payload=data)

    return data.get("result")

def get_eth_balance(address: str) -> Dict[str, Any]:
    """Fetch native ETH balance for an address."""
    params = {
        "module": "account",
        "action": "balance",
        "address": address,
        "tag": "latest"
    }
    result = _get_etherscan(params)
    
    # Etherscan returns balance in Wei
    try:
        wei_balance = int(result)
        eth_balance = wei_balance / 10**18
    except (ValueError, TypeError):
        eth_balance = 0.0

    return {
        "symbol": "ETH",
        "quantity": eth_balance,
        "source": ETHEREUM_SOURCE,
        "market": ETHEREUM_MARKET,
        "address": address
    }

def get_token_balances(address: str) -> List[Dict[str, Any]]:
    """
    Fetch ERC-20 token balances for an address.
    Note: Etherscan doesn't have a single 'get all token balances' for free tier.
    We use the 'addresstokenbalance' or similar if available, 
    but usually we'd need to know the contract addresses.
    
    A better way for free tier is to check 'tokentx' and see what they hold,
    but Etherscan recently added 'pro' features for this.
    
    For the free tier, we can use 'tokenbalance' if we have a list of tokens,
    OR we can use the 'tokenlist' if the user has a Pro API key.
    
    Actually, Etherscan has a 'tokenbalance' action but it requires contractaddress.
    
    Wait, the plan mentioned 'module=account&action=tokenlist'. 
    Let's check if that works or if we should use a different approach.
    """
    # Attempting to use the 'tokenlist' action if available (some Etherscan-like APIs support it)
    # If not, we might need to fall back to a known list of tokens or a different provider.
    # For now, let's implement the 'tokenlist' as suggested in the plan.
    
    params = {
        "module": "account",
        "action": "tokenlist",
        "address": address
    }
    
    try:
        result = _get_etherscan(params)
    except EtherscanAPIError as e:
        LOG.warning(f"Could not fetch token list for {address}: {e}")
        return []

    tokens = []
    for item in result:
        try:
            symbol = item.get("symbol", "").upper()
            decimals = int(item.get("decimals", 18))
            balance_raw = int(item.get("balance", 0))
            quantity = balance_raw / 10**decimals
            
            if quantity > 0:
                tokens.append({
                    "symbol": symbol,
                    "quantity": quantity,
                    "source": ETHEREUM_SOURCE,
                    "market": ETHEREUM_MARKET,
                    "address": address
                })
        except (ValueError, TypeError):
            continue
            
    return tokens

def get_all_balances(addresses: List[str]) -> List[Dict[str, Any]]:
    all_holdings = []
    for addr in addresses:
        addr = addr.strip()
        if not addr:
            continue
        
        # Get ETH
        try:
            all_holdings.append(get_eth_balance(addr))
        except Exception as e:
            LOG.error(f"Error fetching ETH balance for {addr}: {e}")

        # Get Tokens
        try:
            all_holdings.extend(get_token_balances(addr))
        except Exception as e:
            LOG.error(f"Error fetching token balances for {addr}: {e}")
            
    return all_holdings
