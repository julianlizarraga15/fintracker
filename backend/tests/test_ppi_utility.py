import pandas as pd

from backend.core.sources.models import SourcePositions
from scripts import fetch_ppi_positions


def test_ppi_utility_uses_the_isolated_reader(monkeypatch, capsys):
    def read(source, function_name, region, account_id):
        assert source == "ppi"
        assert function_name == fetch_ppi_positions.PPI_SOURCE_LAMBDA
        assert region == fetch_ppi_positions.BROKER_LAMBDA_REGION
        assert account_id == fetch_ppi_positions.ACCOUNT_ID
        return SourcePositions(df=pd.DataFrame([{"symbol": "TEST", "quantity": 2}]))

    monkeypatch.setattr(fetch_ppi_positions, "fetch_broker_positions", read)
    assert fetch_ppi_positions.main() == 0
    assert "Positions loaded: 1" in capsys.readouterr().out
