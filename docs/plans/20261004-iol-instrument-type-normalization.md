# Normalize IOL instrument types

Date: 2026-10-04
Status: Planned

## Objective

Classify IOL holdings with the canonical instrument types expected by valuation snapshots, so CEDEARs and mutual funds no longer appear as Unknown in the dashboard.

## Evidence

The latest saved IOL positions on EC2 contain seven rows:

- `BRKB`, `EEM`, `NVDA`, `SPY`, and `VEA` use IOL label `CEDEARS`.
- `PRMCAPB` and `PRPEDOB` use IOL label `FondoComundeInversion`.

`backend/core/daily_snapshot.py::_normalized_asset_type` lowercases labels and accepts only canonical types. These two valid IOL labels therefore become `None` and are recorded without a type.

## Implementation

- Normalize `cedears` to canonical `cedear`.
- Normalize `fondocomundeinversion` to canonical `fci`.
- Keep accepting existing canonical asset types case-insensitively; keep unknown labels unmapped rather than guessing.
- Add focused tests for both IOL labels, canonical labels, and an unsupported label.

## Refresh and verification

- Run focused tests and the backend test suite where practical.
- Deploy the mapping to EC2.
- Run `docker exec fintracker-backend /app/scripts/run_valuations.sh` on EC2 to fetch current positions and produce a new valuation snapshot.
- Inspect the resulting valuation rows and confirm the dashboard allocation shows the expected CEDEAR and FCI totals instead of Unknown.

The snapshot refresh contacts configured position and pricing sources and writes new snapshot files. It does not alter the existing source holdings.
