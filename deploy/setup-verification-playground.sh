#!/usr/bin/env bash
# Run on the ZOJ application VM as root. Does not restart existing containers.
set -euo pipefail
if [ "$(id -u)" != 0 ]; then
  echo '관리자 권한이 필요합니다: sudo bash deploy/setup-verification-playground.sh' >&2
  exit 1
fi
DEPLOY_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
BACKEND_DIR=$(CDPATH= cd -- "$DEPLOY_DIR/.." && pwd)
if ! docker info --format '{{json .Runtimes}}' | grep -q '"runsc"'; then
  stage=$(mktemp -d)
  trap 'rm -rf "$stage"' EXIT
  arch=$(uname -m)
  case "$arch" in x86_64|aarch64) ;; *) echo '지원되지 않는 CPU' >&2; exit 1;; esac
  base="https://storage.googleapis.com/gvisor/releases/release/latest/$arch"
  curl -fSL "$base/gvisor.tar.bz2" -o "$stage/gvisor.tar.bz2"
  curl -fSL "$base/gvisor.tar.bz2.sha512" -o "$stage/gvisor.tar.bz2.sha512"
  (cd "$stage" && sha512sum -c gvisor.tar.bz2.sha512)
  tar -xjf "$stage/gvisor.tar.bz2" -C /usr/local/bin
  if [ -f /etc/docker/daemon.json ]; then
    cp -p /etc/docker/daemon.json "/etc/docker/daemon.json.zoj-$(date +%Y%m%d%H%M%S).bak"
  fi
  /usr/local/bin/runsc install
  systemctl reload docker
fi
docker info --format '{{json .Runtimes}}' | grep -q '"runsc"'
docker build -f "$BACKEND_DIR/playground/Dockerfile.runner" -t zoj-verification-playground:1 "$BACKEND_DIR/playground"
# Verify the runtime is selected by Docker itself; guest output is not proof.
id=$(docker create --runtime runsc --network none --entrypoint /bin/true zoj-verification-playground:1)
[ "$(docker inspect --format '{{.HostConfig.Runtime}}' "$id")" = runsc ]
docker start -a "$id"
docker rm "$id" >/dev/null
python3 "$DEPLOY_DIR/configure-playground-env.py"
docker compose -f "$DEPLOY_DIR/compose.backend.yaml" --profile verification-playground up -d --build playground-control
# Recreate only the verifier with its new endpoint/token. The API need not know this token.
docker compose -f "$DEPLOY_DIR/compose.backend.yaml" up -d --no-deps --force-recreate verification-ai-worker
printf '\n플레이그라운드 설치 완료. 기존 API 컨테이너는 재시작하지 않았습니다.\n'
