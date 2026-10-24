import importlib.util
import json
import logging
from pathlib import Path
import sys
import types

import pytest


WRAPPER = Path(__file__).resolve().parents[2] / 'infra/binance/lambda_function.py'
SENTINEL = 'synthetic-private-auth-token'
ERROR = {'error_code': 'source_unavailable'}
ROW = {'asset': 'BTC', 'free': 1.25, 'locked': 0.5, 'quantity': 1.75,
       'market': 'binance', 'source': 'binance'}


@pytest.fixture
def worker():
    spec = importlib.util.spec_from_file_location('bounded_binance_lambda', WRAPPER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def dependency(monkeypatch, fetch):
    module = types.ModuleType('binance_client')
    module.get_account_balances = fetch
    monkeypatch.setitem(sys.modules, 'binance_client', module)


def test_dependency_diagnostics_are_suppressed(worker, monkeypatch, capsys, caplog):
    def fail():
        print(SENTINEL)
        print(SENTINEL, file=sys.stderr)
        logging.getLogger('binance_client').critical(SENTINEL)
        raise RuntimeError(SENTINEL)

    dependency(monkeypatch, fail)
    previous_level = logging.root.manager.disable
    result = worker.lambda_handler({}, None)
    captured = capsys.readouterr()
    assert result == ERROR
    assert SENTINEL not in json.dumps(result) + captured.out + captured.err + caplog.text
    assert logging.root.manager.disable == previous_level


def test_import_diagnostics_are_suppressed(worker, monkeypatch, tmp_path, capsys, caplog):
    monkeypatch.delitem(sys.modules, 'binance_client', raising=False)
    (tmp_path / 'binance_client.py').write_text(
        'import logging, sys\n'
        f'print({SENTINEL!r})\n'
        f'print({SENTINEL!r}, file=sys.stderr)\n'
        f'logging.critical({SENTINEL!r})\n'
        f'raise RuntimeError({SENTINEL!r})\n'
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    assert worker.lambda_handler({}, None) == ERROR
    captured = capsys.readouterr()
    assert SENTINEL not in captured.out + captured.err + caplog.text


def test_valid_rows_preserve_legacy_contract(worker, monkeypatch, capsys, caplog):
    rows = [ROW, {**ROW, 'asset': 'ETH', 'free': 0, 'locked': 2, 'quantity': 2},
            {**ROW, 'free': 5, 'locked': -1, 'quantity': 4}]
    def fetch():
        print(SENTINEL)
        logging.critical(SENTINEL)
        return rows

    dependency(monkeypatch, fetch)
    assert worker.lambda_handler({}, None) == rows
    captured = capsys.readouterr()
    assert SENTINEL not in captured.out + captured.err + caplog.text


def test_only_allowlisted_output_fields_survive(worker, monkeypatch):
    dependency(monkeypatch, lambda: [{**ROW, 'token': SENTINEL, 'market': SENTINEL, 'source': SENTINEL}])
    assert worker.lambda_handler({}, None) == [ROW]


@pytest.mark.parametrize('event', [None, [], '', {'api_key': SENTINEL}, {'body': '{}'}])
def test_rejects_request_overrides_without_calling_dependency(worker, monkeypatch, event):
    def unexpected():
        pytest.fail('Invalid requests must not fetch balances')
    dependency(monkeypatch, unexpected)
    assert worker.lambda_handler(event, None) == ERROR


@pytest.mark.parametrize('rows', [None, {}, [None], [{**ROW, 'asset': SENTINEL}],
    [{**ROW, 'asset': 'A' * 33}], [{**ROW, 'free': '1.25'}],
    [{**ROW, 'free': True}], [{**ROW, 'free': float('inf')}],
    [{**ROW, 'quantity': float('nan')}], [{**ROW, 'quantity': 0}],
    [{**ROW, 'quantity': -1}],
    [{'asset': 'BTC'}]])
def test_rejects_malformed_balances(worker, monkeypatch, rows):
    dependency(monkeypatch, lambda: rows)
    assert worker.lambda_handler({}, None) == ERROR


def test_bounds_rows_and_serialized_bytes(worker, monkeypatch):
    dependency(monkeypatch, lambda: [ROW] * (worker.MAX_BALANCE_ROWS + 1))
    assert worker.lambda_handler({}, None) == ERROR
    monkeypatch.setattr(worker, 'MAX_RESPONSE_BYTES', len(json.dumps([ROW]).encode()) - 1)
    dependency(monkeypatch, lambda: [ROW])
    assert worker.lambda_handler({}, None) == ERROR


def test_empty_balances_are_valid(worker, monkeypatch):
    dependency(monkeypatch, lambda: [])
    assert worker.lambda_handler({}, None) == []
