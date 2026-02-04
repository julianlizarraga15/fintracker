from __future__ import annotations

import logging
import requests
import time
from typing import Any, Dict, List, Optional
from backend.core.config import ETHERSCAN_API_KEY, ETHEREUM_TOKEN_CONTRACTS

LOG = logging.getLogger(__name__)

ETHERSCAN_BASE_URL = "https://api.etherscan.io/v2/api"
ETHEREUM_MARKET = "crypto"

class EtherscanAPIError(RuntimeError):
    def __init__(self, message: str, status_code: Optional[int] = None, payload: Any = None):
        super().__init__(message)
        self.status_code = status_code
        self.payload = payload

def _get_etherscan(params: Dict[str, Any], timeout: int = 15, max_retries: int = 3) -> Any:
    if not ETHERSCAN_API_KEY:
        raise EtherscanAPIError("Etherscan API key missing.")

    params["apikey"] = ETHERSCAN_API_KEY
    params["chainid"] = "1"  # Ethereum Mainnet
    
    for attempt in range(max_retries):
        try:
            # Small delay to respect 5 calls/sec limit (free tier)
            time.sleep(0.25)
            
            resp = requests.get(ETHERSCAN_BASE_URL, params=params, timeout=timeout)
            
            # Handle rate limiting (429)
            if resp.status_code == 429:
                LOG.warning(f"Etherscan rate limit hit (429). Attempt {attempt + 1}/{max_retries}. Sleeping...")
                time.sleep(1.0 * (attempt + 1))
                continue

            resp.raise_for_status()
            data = resp.json()
            
            # Etherscan sometimes returns 200 but with a rate limit message in the JSON
            if data.get("status") == "0" and "rate limit" in (data.get("result") or "").lower():
                LOG.warning(f"Etherscan internal rate limit hit. Attempt {attempt + 1}/{max_retries}. Sleeping...")
                time.sleep(1.0 * (attempt + 1))
                continue
                
            break # Success
        except requests.RequestException as exc:
            if attempt == max_retries - 1:
                raise EtherscanAPIError(f"Etherscan request error: {exc}") from exc
            time.sleep(1.0 * (attempt + 1))
        except ValueError as exc:
            raise EtherscanAPIError(f"Etherscan returned non-JSON: {exc}") from exc

    if data.get("status") != "1" and data.get("message") != "No transactions found":
        # "0" status usually means error, but "No transactions found" is fine for tokens
        raise EtherscanAPIError(f"Etherscan API error: {data.get('result') or data.get('message')}", payload=data)

    return data.get("result")

def get_eth_balance(address: str, source: str = "metamask") -> Dict[str, Any]:
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
        "source": source,
        "market": ETHEREUM_MARKET,
        "address": address
    }

def get_token_balances(address: str, source: str = "metamask") -> List[Dict[str, Any]]:
    """
    Fetch ERC-20 token balances for an address using specific contract addresses.
    Config format: SYMBOL:CONTRACT_ADDRESS,SYMBOL:CONTRACT_ADDRESS
    """
    if not ETHEREUM_TOKEN_CONTRACTS:
        return []

    token_configs = [t.strip() for t in ETHEREUM_TOKEN_CONTRACTS.split(",") if ":" in t]
    tokens = []

    for config in token_configs:
        try:
            symbol, contract = config.split(":", 1)
            symbol = symbol.strip().upper()
            contract = contract.strip()

            # First get the balance
            balance_params = {
                "module": "account",
                "action": "tokenbalance",
                "contractaddress": contract,
                "address": address,
                "tag": "latest"
            }
            
            balance_result = _get_etherscan(balance_params)
            balance_raw = int(balance_result)

            if balance_raw <= 0:
                continue

            # Then get the decimals (could be cached, but for now we fetch)
            # Note: module=token&action=tokeninfo is a Pro feature, 
            # but module=account&action=tokentx often has decimals, or we can use common defaults.
            # For simplicity and reliability on free tier, we'll try to get decimals via a standard call if possible,
            # or default to 18 which is most common.
            
            # Etherscan V2 has a way to get token info? 
            # Actually, common tokens like USDC (6) are exceptions. 
            # Let's use a small map for common ones and default to 18.
            common_decimals = {
                "USDC": 6,
                "USDT": 6,
                "DAI": 18,
                "LINK": 18,
                "WBTC": 8,
            }
            decimals = common_decimals.get(symbol, 18)

            quantity = balance_raw / 10**decimals
            
            if quantity > 0:
                tokens.append({
                    "symbol": symbol,
                    "quantity": quantity,
                    "source": source,
                    "market": ETHEREUM_MARKET,
                    "address": address
                })
        except Exception as e:
            LOG.warning(f"Error fetching balance for token {config}: {e}")
            continue
            
    return tokens

def get_all_balances(addresses: List[str], source: str = "metamask") -> List[Dict[str, Any]]:
    all_holdings = []
    for addr in addresses:
        addr = addr.strip()
        if not addr:
            continue
        
        # Get ETH
        try:
            all_holdings.append(get_eth_balance(addr, source=source))
        except Exception as e:
            LOG.error(f"Error fetching ETH balance for {addr}: {e}")

        # Get Tokens
        try:
            all_holdings.extend(get_token_balances(addr, source=source))
        except Exception as e:
            LOG.error(f"Error fetching token balances for {addr}: {e}")
            
    return all_holdings
