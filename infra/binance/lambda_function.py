"""Bound the legacy Binance worker response and keep dependency failures private."""

from contextlib import redirect_stderr, redirect_stdout
import json
import logging
import math
import os
import re
import sys


MAX_BALANCE_ROWS = 2048
MAX_RESPONSE_BYTES = 256 * 1024
ASSET_PATTERN = re.compile(r'[A-Z0-9]{1,32}', re.ASCII)
AMOUNT_FIELDS = ('free', 'locked', 'quantity')


def lambda_handler(event, context):
    if type(event) is not dict or event:
        return {'error_code': 'source_unavailable'}

    previous_logging_level = logging.root.manager.disable
    try:
        # Lambda invokes one request per environment; silence the legacy client
        # before importing it, because imports can also emit credentials.
        with open(os.devnull, 'w') as sink, redirect_stdout(sink), redirect_stderr(sink):
            logging.disable(sys.maxsize)
            try:
                from binance_client import get_account_balances

                rows = get_account_balances()
                if type(rows) is not list or len(rows) > MAX_BALANCE_ROWS:
                    return {'error_code': 'source_unavailable'}

                balances = []
                for row in rows:
                    if type(row) is not dict:
                        return {'error_code': 'source_unavailable'}
                    asset = row.get('asset')
                    if type(asset) is not str or not ASSET_PATTERN.fullmatch(asset):
                        return {'error_code': 'source_unavailable'}
                    amounts = {field: row[field] for field in AMOUNT_FIELDS}
                    if any(type(value) not in (int, float) or not math.isfinite(value)
                           for value in amounts.values()):
                        return {'error_code': 'source_unavailable'}
                    if amounts['quantity'] <= 0:
                        return {'error_code': 'source_unavailable'}
                    balances.append({'asset': asset, **amounts, 'market': 'binance', 'source': 'binance'})

                if len(json.dumps(balances, allow_nan=False).encode('utf-8')) > MAX_RESPONSE_BYTES:
                    return {'error_code': 'source_unavailable'}
                return balances
            finally:
                logging.disable(previous_logging_level)
    except Exception:
        return {'error_code': 'source_unavailable'}
