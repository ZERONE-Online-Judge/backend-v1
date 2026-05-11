# Production Compose Runtime

## 1. Generate Runtime Env Files

```bash
sh backend/deploy/init-env.sh
```

This creates ignored runtime files from `backend/deploy/env/*.env.example`.
Edit every `change-me` value before running production.

Required production values:

- `backend/deploy/env/backend.env`: domain, CORS, PostgreSQL URL, MinIO credentials, SMTP credentials, bootstrap service master account
- `backend/deploy/env/db.env`: PostgreSQL database/user/password
- `backend/deploy/env/minio.env`: MinIO root user/password

## 2. Build Frontend

```bash
npm --prefix frontend run build
```

The production frontend uses relative `/api`, which Nginx proxies to the backend container.
Nginx is the only public entrypoint for the backend stack.

## 3. Start Backend Stack

```bash
docker compose -f backend/deploy/compose.backend.yaml up -d --build
```

Services:

- `nginx`: frontend static files, `/api`, `/minio`, `/minio-console` reverse proxy
- `api`: FastAPI backend, internal compose network only
- `migrate`: Alembic migration job
- `postgres`: production DB
- `redis`: reserved runtime queue/cache dependency
- `minio` and `minio-init`: object storage and bucket creation, internal compose network only
- `mail-worker`: SMTP mail queue consumer
- `postgres-backup`: optional profile, daily `pg_dump -Fc`

Enable backup profile:

```bash
docker compose -f backend/deploy/compose.backend.yaml --profile backup up -d postgres-backup
```
