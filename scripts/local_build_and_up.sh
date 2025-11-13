#!/usr/bin/env bash
set -euo pipefail

# Build backend/frontend images with the same tags docker-compose expects,
# then bring the stack up with any extra args passed to this script.

ROOT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BACKEND_TAG="875551125619.dkr.ecr.us-east-1.amazonaws.com/fintracker-backend:latest"
FRONTEND_TAG="875551125619.dkr.ecr.us-east-1.amazonaws.com/fintracker-frontend:latest"

cd "$ROOT_DIR"

echo "[info] Building backend image -> $BACKEND_TAG"
docker build -f backend/Dockerfile -t "$BACKEND_TAG" .

echo "[info] Building frontend image -> $FRONTEND_TAG"
docker build -f frontend/Dockerfile -t "$FRONTEND_TAG" frontend

echo "[info] Starting docker-compose stack"
docker-compose up -d "$@"
