import pandas as pd

from backend.core import daily_snapshot
from backend.core.sources import iol as iol_source


def test_iol_source_preserves_daily_snapshot_fallback_shape():
    raw_items = [
        {
            "simbolo": "AL30",
            "cantidad": "2",
            "ultimoPrecio": "101.5",
            "descripcion": "Bond",
            "tipoInstrumento": "bond",
            "mercado": "BCBA",
        }
    ]

    result = iol_source.load_positions(
        username="user",
        password="pass",
        account_id="acct-1",
        position_columns=daily_snapshot.POSITION_COLUMNS,
        get_bearer_tokens=lambda _u, _p: ("token", "refresh"),
        get_positions=lambda _token: raw_items,
        extract_positions_as_df=lambda _items: pd.DataFrame(),
    )

    assert result.access_token == "token"
    assert result.raw_items == raw_items
    assert list(result.df.columns) == daily_snapshot.POSITION_COLUMNS
    row = result.df.iloc[0]
    assert row["symbol"] == "AL30"
    assert row["description"] == "Bond"
    assert row["instrument_type"] == "bond"
    assert row["market"] == "BCBA"
    assert row["source"] == "iol"
    assert row["account_id"] == "acct-1"
    assert row["currency"] is None
    assert row["quantity"] == 2.0
    assert row["price"] == 101.5
    assert pd.isna(row["valuation"])
