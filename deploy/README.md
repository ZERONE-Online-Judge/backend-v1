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

Pull `main`, build the frontend, deploy the inactive API pool, switch traffic, and stop the previous API pool:

```bash
cd backend_v1/deploy
./deploy-main-bluegreen.sh
```

The one-command deploy script pulls both `backend_v1` and `demo_frontend` from `origin main`.
It aborts when either worktree has uncommitted changes. Override only when intentional:

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
5. Reload Nginx.

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
