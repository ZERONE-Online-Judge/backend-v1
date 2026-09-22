# Production Compose Runtime

## Division ranking release

Final ranking releases require migration `0023_scoreboard_releases` before deploying the corresponding frontend.
After a contest ends, operators can start an individual release or reveal all ranks for a single division.
The release stores a fixed ranking snapshot after that division's participant judging finishes. Later rejudging does not change this snapshot.
Revealing a rank reveals all teams tied at that rank. Other divisions retain their existing state.

- `GET /api/operator/contests/{contest_id}/divisions/{division_id}/scoreboard/release`: release state and rank choices (no hidden team identities).
- `POST` to the same endpoint with `{"action":"start"}`, `{"action":"rank","rank":2}`, or `{"action":"all"}`.
- Public scoreboards and the presentation use the same persisted state. Unrevealed rows omit team identities and results on the server.
- The operator internal scoreboard remains live for review. Existing access policies still determine who can view the public scoreboard.

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

- `zoj.kr` is served from `${FRONTEND_DIST_PATH:-../../demo_frontend/dist}`.
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
  server_name zoj.kr www.zoj.kr judge.zerone01.kr test.judge.zerone01.kr;

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
- `mail-worker`: SMTP or Resend mail queue consumer
- `notice-worker`: scheduled emergency notice publisher
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

## Canonical public domain

The production site is `https://zoj.kr`. The compose Nginx redirects all
`www.zoj.kr` and `judge.zerone01.kr` requests to this origin with HTTP 308,
preserving the path, query string and HTTP method. This includes the API;
there is no legacy judge API exception. All agents must use the new origin
or a private API endpoint before deploying the redirect. Python urllib does
not replay POST requests across 308 redirects.

`test.judge.zerone01.kr` remains a separate test frontend. Localhost/IP requests
remain available for deployment health checks. The outer TLS proxy must
preserve `Host` and maintain DNS and valid certificates for all production
hostnames, including the redirect aliases.

Runtime settings in `deploy/env/backend.env`:

```env
PUBLIC_BASE_URL=https://zoj.kr
CORS_ALLOW_ORIGINS=https://zoj.kr
```

Preserve additional test/local origins if the installation uses them. Recreate
API and background workers through the blue-green deploy to apply env changes.
The public URL controls email links and signed storage URLs. Relative frontend
`/api` requests need no change. Canonical/Open Graph metadata, robots.txt,
sitemap.xml and the development API proxy must also use zoj.kr.

Each judge machine must have runtime `INTERNAL_API_BASE_URL=https://zoj.kr/api`
(private API endpoints can remain). Recreate agents when they have no active
jobs and verify their heartbeats before enabling the redirects. The judge-agent
repository provides `deploy/update-api-domain.sh` for the existing node fleet.
Changing installer defaults alone does not update existing agents.

Validate HTTPS, signed downloads/uploads, CORS, judge heartbeats, and redirect
paths/queries after deploying. Existing browser login state and local code
drafts are origin-specific and do not automatically move to zoj.kr. Accounts
and submissions on the server remain unchanged. Keep token signing keys and
database/storage credentials unchanged during this migration.
