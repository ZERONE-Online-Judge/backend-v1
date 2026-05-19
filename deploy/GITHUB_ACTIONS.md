# Backend GitHub Actions 배포 설정

`.github/workflows/backend-ci-cd.yml`는 `main` 브랜치에 백엔드 변경이 push되면 테스트 후 서버에 SSH 접속해서 기존 blue-green 배포 스크립트를 실행합니다.

## GitHub Secrets

Repository Settings → Secrets and variables → Actions에 아래 값을 등록합니다.

| Secret | 예시 | 설명 |
| --- | --- | --- |
| `BACKEND_DEPLOY_HOST` | `1.2.3.4` | 배포 서버 IP 또는 도메인 |
| `BACKEND_DEPLOY_PORT` | `22` | SSH 포트. 비워도 22 사용 |
| `BACKEND_DEPLOY_USER` | `ubuntu` | 서버 SSH 사용자 |
| `BACKEND_DEPLOY_SSH_KEY` | `-----BEGIN OPENSSH PRIVATE KEY-----...` | 서버 접속용 private key |
| `BACKEND_DEPLOY_DIR` | `/srv/zerone_online_judge/backend_v1/deploy` | 서버의 `deploy-main-bluegreen.sh`가 있는 디렉터리 |
| `PUBLIC_HEALTH_URL` | `https://test.judge.zerone01.kr/api/health` | 배포 후 공개 헬스체크 URL |

## 서버 사전 준비

서버의 `BACKEND_DEPLOY_DIR`에서 아래 명령이 동작해야 합니다.

```bash
./deploy-main-bluegreen.sh
```

그리고 서버의 배포용 계정은 아래 권한이 필요합니다.

```bash
git fetch origin main
git pull --ff-only origin main
docker compose -f compose.backend.yaml ps
docker compose -f compose.backend.yaml run --rm migrate
docker compose -f compose.backend.yaml up -d --build api-blue api-green nginx
docker compose -f compose.backend.yaml exec -T nginx nginx -s reload
```

`deploy-main-bluegreen.sh`가 worktree dirty 상태에서 중단되므로, 서버에서 직접 수정한 파일은 커밋하거나 별도 env 파일처럼 git ignore되는 위치에 둡니다.

## 동작 흐름

1. GitHub runner에서 `backend_v1` 테스트 실행
2. 테스트 통과 시 서버 SSH 접속
3. 서버에서 `deploy-main-bluegreen.sh` 실행
4. 서버가 `origin main`을 pull
5. Alembic migration 실행
6. inactive API pool 빌드/기동
7. Nginx upstream 전환 후 컨테이너 재생성 없이 reload
8. 공개 health check 확인
