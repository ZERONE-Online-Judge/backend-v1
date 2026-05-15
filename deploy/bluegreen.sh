#!/usr/bin/env sh
set -eu

DEPLOY_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
COMPOSE_FILE="$DEPLOY_DIR/compose.backend.yaml"
UPSTREAM_FILE="$DEPLOY_DIR/nginx/api-upstream.conf"

compose() {
  docker compose -f "$COMPOSE_FILE" "$@"
}

usage() {
  cat <<'EOF'
Usage:
  ./bluegreen.sh status
  ./bluegreen.sh active
  ./bluegreen.sh migrate
  ./bluegreen.sh start blue|green
  ./bluegreen.sh switch blue|green
  ./bluegreen.sh deploy blue|green
  ./bluegreen.sh stop blue|green

Deploy order:
  migrate -> start target api -> health check -> switch nginx upstream
EOF
}

normalize_color() {
  case "${1:-}" in
    blue|green) printf '%s\n' "$1" ;;
    *) echo "color must be blue or green" >&2; exit 2 ;;
  esac
}

active_color() {
  ensure_upstream_file
  sed -n 's/.*server api-\(blue\|green\):8000.*/\1/p' "$UPSTREAM_FILE" | head -n 1
}

ensure_upstream_file() {
  if [ ! -f "$UPSTREAM_FILE" ]; then
    write_upstream blue
  fi
}

write_upstream() {
  color=$(normalize_color "$1")
  tmp="$UPSTREAM_FILE.tmp"
  {
    echo "upstream api_backend {"
    echo "  server api-$color:8000;"
    echo "  keepalive 32;"
    echo "}"
  } > "$tmp"
  mv "$tmp" "$UPSTREAM_FILE"
}

healthcheck() {
  color=$(normalize_color "$1")
  tries=0
  until compose exec -T "api-$color" python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/api/health', timeout=5)" >/dev/null 2>&1; do
    tries=$((tries + 1))
    if [ "$tries" -ge 60 ]; then
      echo "api-$color health check failed" >&2
      return 1
    fi
    sleep 1
  done
}

apply_nginx() {
  compose up -d --force-recreate nginx
  compose exec -T nginx nginx -t
}

cmd="${1:-}"
case "$cmd" in
  status)
    echo "active=$(active_color)"
    compose ps
    ;;
  active)
    active_color
    ;;
  migrate)
    compose run --rm migrate
    ;;
  start)
    color=$(normalize_color "${2:-}")
    compose up -d --build "api-$color"
    healthcheck "$color"
    ;;
  switch)
    color=$(normalize_color "${2:-}")
    healthcheck "$color"
    write_upstream "$color"
    apply_nginx
    echo "active=$color"
    ;;
  stop)
    color=$(normalize_color "${2:-}")
    compose stop "api-$color"
    ;;
  deploy)
    color=$(normalize_color "${2:-}")
    compose run --rm migrate
    compose up -d --build "api-$color"
    healthcheck "$color"
    write_upstream "$color"
    apply_nginx
    echo "active=$color"
    ;;
  *)
    usage
    exit 2
    ;;
esac
