# Production Compose Runtime

## 1. Generate Runtime Env Files

```bash
sh backend_v1/deploy/init-env.sh
```

This creates ignored runtime files from `backend_v1/deploy/env/*.env.example`.
Edit every `change-me` value before running production.

Required production values:

- `backend_v1/deploy/env/backend.env`: domain, CORS, PostgreSQL URL, MinIO credentials, SMTP credentials, bootstrap service master account
- `backend_v1/deploy/env/db.env`: PostgreSQL database/user/password
- `backend_v1/deploy/env/minio.env`: MinIO root user/password

## 2. Build Frontend

```bash
npm --prefix demo_frontend run build
```

The production frontend uses relative `/api`, which Nginx proxies to the backend container.
Nginx is the only public entrypoint for the backend stack.

Optional test frontend domain:

- `judge.zerone01.kr` is served from `${FRONTEND_DIST_PATH:-../../demo_frontend/dist}`.
- `test.judge.zerone01.kr` is served from `${TEST_FRONTEND_DIST_PATH:-../../demo_frontend/dist}`.
- Both domains proxy `/api`, `/minio`, and `/minio-console` to the same backend stack.

If you want a different build for the test domain, build it into a separate directory and pass it to compose:

```bash
TEST_FRONTEND_DIST_PATH=/srv/zoj/test-frontend-dist \
docker compose -f backend_v1/deploy/compose.backend.yaml up -d --force-recreate nginx
```

If a host-level Nginx terminates TLS in front of this compose stack, proxy both hostnames to the compose Nginx port while preserving `Host`:

```nginx
server {
  listen 443 ssl http2;
  server_name judge.zerone01.kr test.judge.zerone01.kr;

  location / {
    proxy_pass http://127.0.0.1:6001;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
  }
}
```

Also point DNS for `test.judge.zerone01.kr` to the same server and issue a certificate that covers the test hostname.

## 3. Start Backend Stack

```bash
docker compose -f backend_v1/deploy/compose.backend.yaml up -d --build
```

Services:

- `nginx`: frontend static files, `/api`, `/minio`, `/minio-console` reverse proxy
- `api-blue` and `api-green`: FastAPI backend pools for blue-green switching, internal compose network only
- `migrate`: Alembic migration job
- `postgres`: production DB
- `redis`: reserved runtime queue/cache dependency
- `minio` and `minio-init`: object storage and bucket creation, internal compose network only
- `mail-worker`: SMTP mail queue consumer
- `postgres-backup`: optional profile, daily `pg_dump -Fc`

Enable backup profile:

```bash
docker compose -f backend_v1/deploy/compose.backend.yaml --profile backup up -d postgres-backup
```

## 4. Blue-Green API Deploy

Nginx proxies `/api` to `api_backend`, which is defined by `deploy/nginx/api-upstream.conf`.
The active pool is either `api-blue:8000` or `api-green:8000`.
Traffic switches by rewriting the upstream file and running `nginx -s reload`; the Nginx container is not recreated during blue-green deploy.

Check current state:

```bash
cd backend_v1/deploy
./bluegreen.sh status
```

Deploy a new API version to the inactive pool:

```bash
cd backend_v1/deploy
./bluegreen.sh deploy green
```

Pull backend `main`, deploy the inactive API pool, switch traffic, and stop the previous API pool:

```bash
cd backend_v1/deploy
./deploy-main-bluegreen.sh
```

The one-command deploy script only pulls `backend_v1` from `origin main`.
The backend deploy aborts when the backend worktree has uncommitted changes. Override only when intentional:

```bash
ALLOW_DIRTY=1 ./deploy-main-bluegreen.sh
```

If the public health endpoint is not `http://127.0.0.1:6001/api/health`, set it explicitly:

```bash
PUBLIC_HEALTH_URL=https://judge.example.com/api/health ./deploy-main-bluegreen.sh
```

Rollback is only an upstream switch:

```bash
cd backend_v1/deploy
./bluegreen.sh switch blue
```

The deploy command runs in this order:

1. Run Alembic migrations.
2. Start/build the target API pool.
3. Check the target pool with `/api/health`.
4. Rewrite `nginx/api-upstream.conf`.
5. Validate Nginx config with `nginx -t`.
6. Reload Nginx with `nginx -s reload`.

Database migrations must be backward-compatible with the currently active API.
Use expand-and-contract migrations:

1. Add nullable columns/tables/indexes first.
2. Deploy the new API.
3. Switch traffic.
4. Remove old columns or incompatible behavior in a later deploy.

Runtime release metadata:

```bash
RELEASE_VERSION=2026-05-14.1 ./bluegreen.sh deploy green
curl https://judge.example.com/api/health
```

Feature flags live in `env/backend.env`:

```env
FEATURE_SUBMISSION_RUNTIME_METRICS=true
FEATURE_PUBLIC_SCOREBOARD_PENALTY=true
FEATURE_EMERGENCY_NOTICE_AUTO=true
```
