from __future__ import annotations

from pathlib import Path

from backend.app.job_runs import build_job_run_payload, parse_job_run_log


def test_parse_job_run_log_extracts_binance_missing_symbols_and_outputs():
    log_text = """
Loaded 6 Binance assets.
Positions (10 rows x 9 cols)
[positions] CSV saved -> /data/positions.csv
[job-detail] {"event":"source_counts","source":"binance","loaded":6,"priced":3,"missing":3}
Priced 3 Binance assets (missing 3).
Missing Binance prices: ARS, ETHW, OLD
[job-detail] {"event":"pricing_issue","source":"binance","symbol":"ARS","issue_type":"missing_price","details":"No Binance ticker"}
[prices] Parquet saved -> /data/prices.parquet
[fx] Parquet saved -> /data/fx.parquet
[valuations] Parquet saved -> /data/valuations.parquet
"""

    parsed = parse_job_run_log(log_text)

    assert parsed["positions_count"] == 10
    assert parsed["loaded"]["binance_assets"] == 6
    assert parsed["pricing"]["binance_priced"] == 3
    assert parsed["pricing"]["binance_missing"] == 3
    assert parsed["counts_by_source"]["binance"] == {"loaded": 6, "priced": 3, "missing": 3}
    assert {issue["symbol"] for issue in parsed["pricing_issues"]} == {"ARS", "ETHW", "OLD"}
    assert parsed["outputs"]["positions"]["csv"] == "/data/positions.csv"
    assert parsed["outputs"]["prices"]["parquet"] == "/data/prices.parquet"
    assert parsed["outputs"]["fx"]["parquet"] == "/data/fx.parquet"
    assert parsed["outputs"]["valuations"]["parquet"] == "/data/valuations.parquet"


def test_build_job_run_payload_separates_warnings_and_errors(tmp_path: Path):
    log_file = tmp_path / "run.log"
    log_file.write_text(
        "No FX rates fetched.\n"
        "Error fetching Binance prices: boom\n"
        "[valuations] Parquet saved -> /data/valuations.parquet\n",
        encoding="utf-8",
    )

    payload = build_job_run_payload(
        job_name="valuations",
        run_id="2026-06-21_120000",
        started_at="2026-06-21T12:00:00Z",
        finished_at="2026-06-21T12:00:05Z",
        exit_code=0,
        log_file=log_file,
    )

    assert payload["status"] == "success"
    assert payload["duration_seconds"] == 5
    assert payload["warnings"] == ["No FX rates fetched."]
    assert payload["errors"] == ["Error fetching Binance prices: boom"]
