#!/usr/bin/env sh
set -eu

DEPLOY_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
BACKEND_DIR=$(CDPATH= cd -- "$DEPLOY_DIR/.." && pwd)
PROJECT_DIR=$(CDPATH= cd -- "$BACKEND_DIR/.." && pwd)
FRONTEND_DIR="$PROJECT_DIR/demo_frontend"
BLUEGREEN="$DEPLOY_DIR/bluegreen.sh"
COMPOSE_FILE="$DEPLOY_DIR/compose.backend.yaml"

compose() {
  docker compose -f "$COMPOSE_FILE" "$@"
}

log() {
  printf '%s\n' "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

ensure_clean_worktree() {
  repo_dir="$1"
  repo_name="$2"
  if [ "${ALLOW_DIRTY:-0}" = "1" ]; then
    return
  fi
  if [ -n "$(git -C "$repo_dir" status --porcelain)" ]; then
    echo "$repo_name has uncommitted changes. Commit/stash them or run with ALLOW_DIRTY=1." >&2
    exit 1
  fi
}

pull_main() {
  repo_dir="$1"
  repo_name="$2"
  log "pull $repo_name origin main"
  git -C "$repo_dir" fetch origin main
  git -C "$repo_dir" checkout main
  git -C "$repo_dir" pull --ff-only origin main
}

inactive_color() {
  active="$1"
  case "$active" in
    blue) printf '%s\n' green ;;
    green) printf '%s\n' blue ;;
    *) printf '%s\n' blue ;;
  esac
}

health_via_nginx() {
  expected_color="$1"
  health_url="${PUBLIC_HEALTH_URL:-http://127.0.0.1:6001/api/health}"
  tries=0
  until curl -fsS "$health_url" | grep -q "\"release_color\":\"$expected_color\""; do
    tries=$((tries + 1))
    if [ "$tries" -ge 30 ]; then
      echo "public health check did not route to $expected_color: $health_url" >&2
      return 1
    fi
    sleep 1
  done
}

if [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ]; then
  cat <<'EOF'
Usage:
  ./deploy-main-bluegreen.sh

Environment:
  ALLOW_DIRTY=1        Allow deploy with local uncommitted changes.
  SKIP_FRONTEND=1      Skip npm ci/build.
  RELEASE_VERSION=x    Override release version. Default: backend git short sha.

Flow:
  check clean worktrees -> git pull origin main -> build frontend ->
  deploy inactive API -> health check -> nginx switch -> stop previous API
EOF
  exit 0
fi

ensure_clean_worktree "$BACKEND_DIR" "backend_v1"
ensure_clean_worktree "$FRONTEND_DIR" "demo_frontend"

pull_main "$BACKEND_DIR" "backend_v1"
pull_main "$FRONTEND_DIR" "demo_frontend"

release_version="${RELEASE_VERSION:-$(git -C "$BACKEND_DIR" rev-parse --short=12 HEAD)}"

if [ "${SKIP_FRONTEND:-0}" != "1" ]; then
  log "build frontend"
  if [ -f "$FRONTEND_DIR/package-lock.json" ]; then
    npm --prefix "$FRONTEND_DIR" ci
  else
    npm --prefix "$FRONTEND_DIR" install
  fi
  npm --prefix "$FRONTEND_DIR" run build
fi

active="$("$BLUEGREEN" active || true)"
target="$(inactive_color "$active")"

log "active=${active:-none}, target=$target, release=$release_version"

log "deploy api-$target"
RELEASE_VERSION="$release_version" "$BLUEGREEN" deploy "$target"

log "verify nginx routes to api-$target"
health_via_nginx "$target"

if [ "$active" = "blue" ] || [ "$active" = "green" ]; then
  log "stop previous api-$active"
  "$BLUEGREEN" stop "$active"
fi

log "deployment complete: active=$target release=$release_version"
