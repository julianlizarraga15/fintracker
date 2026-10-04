#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
docker exec -i fintracker-backend python - "$@" < "$SCRIPT_DIR/inspect_iol_types.py"
