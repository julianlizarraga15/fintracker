#!/usr/bin/env bash
set -Eeuo pipefail

BACKEND_IMAGE=${1:?backend image (or -) required}
FRONTEND_IMAGE=${2:?frontend image (or -) required}
DEPLOY_BACKEND=${3:?backend change flag required}
DEPLOY_FRONTEND=${4:?frontend change flag required}
RELEASE_TAG=${5:?release tag required}
AWS_REGION=${6:?AWS region required}
ECR_REGISTRY=${7:?ECR registry required}
BACKEND_REPO=${8:?backend repository required}
FRONTEND_REPO=${9:?frontend repository required}
RELEASE_SHA=${10:?release commit required}

PROJECT_DIR=${FINTRACKER_PROJECT_DIR:-/home/ec2-user/fintracker}
DATA_DIR="$PROJECT_DIR/data"
BOOTSTRAP="$PROJECT_DIR/.isolation-bootstrap"
COMPOSE_FILE=""
DOCKER_CONFIG_DIR=""
TEMP_BOOTSTRAP=""
DEPLOY_LOCK=""
TIMER_WAS_ACTIVE=false
TIMER_STOPPED=false
LOCK_OWNED=false
BACKEND_STARTED=false
FRONTEND_STARTED=false
ROLLBACK_OK=false
DRAIN_ATTEMPTS=120
LOCK_WAIT_ATTEMPTS=120
SERVICE_POLL_SECONDS=5
BACKEND_HEALTH_ATTEMPTS=60
BACKEND_HEALTH_POLL_SECONDS=5
FRONTEND_HEALTH_ATTEMPTS=30
FRONTEND_HEALTH_POLL_SECONDS=2

fail() {
  echo "Isolated release aborted: $1" >&2
  exit 1
}

write_bootstrap() {
  local image=$1
  local temporary
  temporary=$(mktemp "$PROJECT_DIR/.isolation-bootstrap.XXXXXX")
  TEMP_BOOTSTRAP=$temporary
  {
    printf 'SSM_ENV_PATH=/fintracker/web\n'
    printf 'AWS_REGION=%s\n' "$AWS_REGION"
    printf 'AWS_DEFAULT_REGION=%s\n' "$AWS_REGION"
    printf 'PYTHON_DOTENV_DISABLED=1\n'
    printf 'FINTRACKER_BACKEND_IMAGE=%s\n' "$image"
  } > "$temporary" || { rm -f "$temporary"; TEMP_BOOTSTRAP=""; return 1; }
  chmod 600 "$temporary" || { rm -f "$temporary"; TEMP_BOOTSTRAP=""; return 1; }
  mv -f "$temporary" "$BOOTSTRAP" || { rm -f "$temporary"; TEMP_BOOTSTRAP=""; return 1; }
  TEMP_BOOTSTRAP=""
}

compose() {
  (cd "$PROJECT_DIR" && docker-compose --env-file /dev/null -p fintracker -f "$COMPOSE_FILE" "$@")
}

backend_healthy() {
  local attempt
  for attempt in $(seq 1 "$BACKEND_HEALTH_ATTEMPTS"); do
    if docker exec fintracker-backend python -c \
      'import urllib.request; urllib.request.urlopen("http://127.0.0.1:8000/health", timeout=3)' \
      >/dev/null 2>&1; then
      return 0
    fi
    sleep "$BACKEND_HEALTH_POLL_SECONDS"
  done
  return 1
}

frontend_healthy() {
  local attempt
  for attempt in $(seq 1 "$FRONTEND_HEALTH_ATTEMPTS"); do
    if docker exec fintracker-frontend wget -q -O /dev/null http://127.0.0.1/ \
      >/dev/null 2>&1; then
      return 0
    fi
    sleep "$FRONTEND_HEALTH_POLL_SECONDS"
  done
  return 1
}

restore_timer() {
  if [[ "$TIMER_STOPPED" == true && "$TIMER_WAS_ACTIVE" == true ]]; then
    sudo systemctl start valuations.timer
    sudo systemctl is-active --quiet valuations.timer
  fi
}

release_deploy_lock() {
  if [[ "$LOCK_OWNED" == true ]]; then
    rm -f "$DEPLOY_LOCK/pid" "$DEPLOY_LOCK/run_id" "$DEPLOY_LOCK/started_at" || return 1
    rmdir "$DEPLOY_LOCK" || return 1
    LOCK_OWNED=false
  fi
}

rollback() {
  local services=()
  if [[ "$BACKEND_STARTED" == true ]]; then
    write_bootstrap "$ROLLBACK_BACKEND_IMAGE" || return 1
    services+=(backend)
  fi
  if [[ "$FRONTEND_STARTED" == true ]]; then
    services+=(frontend)
  fi
  if ((${#services[@]})); then
    FINTRACKER_BACKEND_IMAGE="$ROLLBACK_BACKEND_IMAGE" \
      FINTRACKER_FRONTEND_IMAGE="$ROLLBACK_FRONTEND_IMAGE" \
      compose up -d --no-deps --force-recreate "${services[@]}" || return 1
  fi
  if [[ "$BACKEND_STARTED" == true ]] && ! backend_healthy; then
    return 1
  fi
  if [[ "$FRONTEND_STARTED" == true ]] && ! frontend_healthy; then
    return 1
  fi
  if ((${#services[@]})); then
    docker exec fintracker-nginx nginx -t >/dev/null 2>&1 || return 1
    docker exec fintracker-nginx nginx -s reload >/dev/null 2>&1 || return 1
  fi
  ROLLBACK_OK=true
}

cleanup() {
  local exit_code=$?
  trap - EXIT
  set +e

  if [[ "$exit_code" -ne 0 ]] && \
    { [[ "$BACKEND_STARTED" == true ]] || [[ "$FRONTEND_STARTED" == true ]]; }; then
    if rollback; then
      echo "Previous isolated image restored."
    else
      echo "Rollback failed; valuation lock remains held and timer remains stopped." >&2
      exit_code=1
    fi
  fi

  if [[ "$exit_code" -eq 0 || "$ROLLBACK_OK" == true || \
    ( "$BACKEND_STARTED" == false && "$FRONTEND_STARTED" == false ) ]]; then
    if ! release_deploy_lock; then
      echo "Could not release valuation deployment lock." >&2
      exit_code=1
    elif ! restore_timer; then
      echo "Could not restore the valuation timer." >&2
      exit_code=1
    fi
  fi

  [[ -z "$COMPOSE_FILE" ]] || rm -f "$COMPOSE_FILE"
  [[ -z "$TEMP_BOOTSTRAP" ]] || rm -f "$TEMP_BOOTSTRAP"
  [[ -z "$DOCKER_CONFIG_DIR" ]] || rm -rf "$DOCKER_CONFIG_DIR"
  if [[ "$exit_code" -eq 0 ]]; then
    echo "Isolated release succeeded."
  fi
  exit "$exit_code"
}
trap cleanup EXIT

[[ "$DEPLOY_BACKEND" == true || "$DEPLOY_BACKEND" == false ]] || fail "invalid backend change flag"
[[ "$DEPLOY_FRONTEND" == true || "$DEPLOY_FRONTEND" == false ]] || fail "invalid frontend change flag"
[[ "$DEPLOY_BACKEND" == true || "$DEPLOY_FRONTEND" == true ]] || fail "no deployable service selected"
[[ "$RELEASE_TAG" =~ ^[a-f0-9]{12}-[0-9]+-[0-9]+$ ]] || fail "invalid release tag"
[[ "$RELEASE_SHA" =~ ^[a-f0-9]{40}$ ]] || fail "invalid release commit"
[[ "$AWS_REGION" == us-east-1 && "$ECR_REGISTRY" == 875551125619.dkr.ecr.us-east-1.amazonaws.com ]] || fail "unexpected registry configuration"
[[ "$BACKEND_REPO" == fintracker-backend && "$FRONTEND_REPO" == fintracker-frontend ]] || fail "unexpected image repository"

if [[ "$DEPLOY_BACKEND" == true ]]; then
  [[ "$BACKEND_IMAGE" == "$ECR_REGISTRY/$BACKEND_REPO:$RELEASE_TAG" ]] || fail "backend image does not match release"
fi
if [[ "$DEPLOY_FRONTEND" == true ]]; then
  [[ "$FRONTEND_IMAGE" == "$ECR_REGISTRY/$FRONTEND_REPO:$RELEASE_TAG" ]] || fail "frontend image does not match release"
fi

[[ -f "$BOOTSTRAP" ]] || fail "isolated bootstrap is missing"
[[ -d "$DATA_DIR/job_runs/valuations" ]] || fail "valuation state directory is missing"

DOCKER_CONFIG_DIR=$(mktemp -d)
export DOCKER_CONFIG="$DOCKER_CONFIG_DIR"
aws ecr get-login-password --region "$AWS_REGION" | \
  docker login --username AWS --password-stdin "$ECR_REGISTRY" >/dev/null

if [[ "$DEPLOY_BACKEND" == true ]]; then
  docker pull "$BACKEND_IMAGE" >/dev/null
  BACKEND_CONTAINER_IMAGE=$(docker inspect --format '{{.Image}}' fintracker-backend)
  ROLLBACK_BACKEND_IMAGE="fintracker-backend:rollback-$RELEASE_TAG"
  docker image tag "$BACKEND_CONTAINER_IMAGE" "$ROLLBACK_BACKEND_IMAGE"
else
  ROLLBACK_BACKEND_IMAGE=fintracker-backend:isolation-070cbf5
fi

if [[ "$DEPLOY_FRONTEND" == true ]]; then
  docker pull "$FRONTEND_IMAGE" >/dev/null
  FRONTEND_CONTAINER_IMAGE=$(docker inspect --format '{{.Image}}' fintracker-frontend)
  ROLLBACK_FRONTEND_IMAGE="fintracker-frontend:rollback-$RELEASE_TAG"
  docker image tag "$FRONTEND_CONTAINER_IMAGE" "$ROLLBACK_FRONTEND_IMAGE"
else
  ROLLBACK_FRONTEND_IMAGE=875551125619.dkr.ecr.us-east-1.amazonaws.com/fintracker-frontend:latest
fi

COMPOSE_FILE=$(mktemp "$PROJECT_DIR/.isolated-compose.XXXXXX")
chmod 600 "$COMPOSE_FILE"
git -c safe.directory="$PROJECT_DIR" -C "$PROJECT_DIR" show \
  "$RELEASE_SHA:docker-compose.isolated.yml" > "$COMPOSE_FILE"
compose config -q

if [[ "$DEPLOY_BACKEND" == true ]]; then
  sudo systemctl is-enabled --quiet valuations.timer || fail "valuation timer is not enabled"
  sudo systemctl is-active --quiet valuations.timer || fail "valuation timer is not active"
  TIMER_WAS_ACTIVE=true
  sudo systemctl stop valuations.timer
  TIMER_STOPPED=true

  for attempt in $(seq 1 "$DRAIN_ATTEMPTS"); do
    if ! sudo systemctl is-active --quiet valuations.service; then
      break
    fi
    sleep "$SERVICE_POLL_SECONDS"
  done
  sudo systemctl is-active --quiet valuations.service && fail "scheduled valuation did not drain"

  [[ "$(docker exec fintracker-backend python -c 'print(open("/proc/1/comm").read().strip())')" == uvicorn ]] || \
    fail "backend PID 1 is not uvicorn; deploy lock cannot be guaranteed"

  container_runs_root=$(docker exec fintracker-backend python -c \
    'from pathlib import Path; from backend.core.settings import load_settings; load_settings(); from backend.app.job_runs import JOB_RUNS_ROOT; root = JOB_RUNS_ROOT if JOB_RUNS_ROOT.is_absolute() else Path.cwd() / JOB_RUNS_ROOT; print(root.resolve())')
  case "$container_runs_root" in
    /app/data/*) ;;
    *) fail "valuation state is outside the reviewed data mount" ;;
  esac
  container_lock_path="$container_runs_root/valuations/.running.lock"
  host_data_mount=$(docker inspect --format \
    '{{range .Mounts}}{{if eq .Destination "/app/data"}}{{.Source}}{{end}}{{end}}' fintracker-backend)
  [[ "$host_data_mount" == "$DATA_DIR" ]] || fail "backend data mount differs from the reviewed host path"
  DEPLOY_LOCK="$host_data_mount/${container_lock_path#/app/data/}"

  mkdir -p "$(dirname "$DEPLOY_LOCK")"
  lock_acquired=false
  for attempt in $(seq 1 "$LOCK_WAIT_ATTEMPTS"); do
    if mkdir "$DEPLOY_LOCK" 2>/dev/null; then
      LOCK_OWNED=true
      lock_acquired=true
      printf '1\n' > "$DEPLOY_LOCK/pid"
      printf '%s\n' "$RELEASE_TAG" > "$DEPLOY_LOCK/run_id"
      date -u +'%Y-%m-%dT%H:%M:%SZ' > "$DEPLOY_LOCK/started_at"
      break
    fi

    run_state=$(docker exec -e "JOB_RUNS_LOCAL_DIR=$container_runs_root" fintracker-backend python -c \
      'from backend.app.job_trigger import is_job_running; print("busy" if is_job_running() else "idle")' \
      2>/dev/null || echo unavailable)
    if [[ "$run_state" == idle ]]; then
      continue
    fi
    sleep "$SERVICE_POLL_SECONDS"
  done
  [[ "$lock_acquired" == true ]] || fail "valuation job did not become idle"

  BACKEND_STARTED=true
  write_bootstrap "$BACKEND_IMAGE"
  FINTRACKER_BACKEND_IMAGE="$BACKEND_IMAGE" \
    FINTRACKER_FRONTEND_IMAGE="$ROLLBACK_FRONTEND_IMAGE" \
    compose up -d --no-deps --force-recreate backend
  backend_healthy || fail "new backend health check failed"
fi

if [[ "$DEPLOY_FRONTEND" == true ]]; then
  FRONTEND_STARTED=true
  FINTRACKER_BACKEND_IMAGE="$ROLLBACK_BACKEND_IMAGE" \
    FINTRACKER_FRONTEND_IMAGE="$FRONTEND_IMAGE" \
    compose up -d --no-deps --force-recreate frontend
  frontend_healthy || fail "new frontend health check failed"
fi

docker exec fintracker-nginx nginx -t >/dev/null 2>&1
docker exec fintracker-nginx nginx -s reload >/dev/null 2>&1
