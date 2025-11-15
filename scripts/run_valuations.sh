#!/usr/bin/env bash
set -euo pipefail

# Navigate to application root inside the container if not already there
cd /app

# Load environment variables if .env is present (e.g., when running outside Docker)
if [[ -f .env ]]; then
  set -o allexport
  # shellcheck disable=SC1091
  source .env
  set +o allexport
fi

python -m backend.core.daily_snapshot
