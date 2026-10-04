"""Print IOL instrument labels from the latest saved positions CSV."""

from __future__ import annotations

import argparse
import csv
import os
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(os.getenv("SNAPSHOTS_LOCAL_DIR", "data/positions")),
        help="Snapshot storage root (defaults to SNAPSHOTS_LOCAL_DIR or data/positions).",
    )
    args = parser.parse_args()
    positions_dir = args.root / "positions"
    files = sorted(
        positions_dir.rglob("positions_*.csv"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    for path in files:
        with path.open(newline="", encoding="utf-8-sig") as handle:
            rows = [
                row
                for row in csv.DictReader(handle)
                if row.get("source", "").strip().lower() == "iol"
            ]
        if not rows:
            continue
        print(f"File: {path}")
        print(f"IOL positions: {len(rows)}")
        print("Symbol\tInstrument label")
        for row in rows:
            label = row.get("instrument_type", "").strip()
            print(f"{row.get('symbol', '')}\t{label or '(missing)'}")
        return 0

    print(f"No saved IOL positions found under {positions_dir}.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
