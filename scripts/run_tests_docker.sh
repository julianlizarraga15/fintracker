#!/usr/bin/env bash
set -euo pipefail

APP_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE_NAME="fintracker-backend-tests"

docker build \
  --tag "$IMAGE_NAME" \
  --file "$APP_ROOT/backend/Dockerfile.test" \
  "$APP_ROOT/backend"
docker run --rm "$IMAGE_NAME"
