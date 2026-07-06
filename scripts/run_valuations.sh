#!/usr/bin/env bash
set -euo pipefail

APP_ROOT="${FINTRACKER_APP_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"

# Navigate to application root if not already there.
cd "${APP_ROOT}"

# Load environment variables if .env is present (e.g., when running outside Docker)
if [[ -f .env ]]; then
  set -o allexport
  # shellcheck disable=SC1091
  source .env
  set +o allexport
fi

exec "${APP_ROOT}/scripts/run_valuations_with_status.sh"
