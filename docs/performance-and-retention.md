# Web load and log retention

The API keeps PostgreSQL authoritative for authentication and authorization.
Redis stores derived results only; a cache failure falls back to database reads.

## Request handling

- The production Compose API runs fourteen Uvicorn workers by default. Set
  `API_WORKERS` in the deployment shell/Compose environment to change this.
- Each process has three PostgreSQL connections without overflow and a five-second
  pool wait. Result-wait authentication has one additional connection per API
  process. The default fourteen-worker API therefore uses at most 56 connections.
  Both API colors, four background pools of two, and a two-connection migration
  pool total 122 connections. PostgreSQL is configured for 160. Deployment checks
  the rendered configuration against the running database and retains at least
  ten spare connections before starting services. Set `API_DATABASE_POOL_SIZE`,
  `WORKER_DATABASE_POOL_SIZE` and `MIGRATION_DATABASE_POOL_SIZE` for role-specific
  overrides. Increasing workers alone can exhaust database capacity and reduce
  throughput. See [the 14-vCPU measurements](performance-14vcpu.md).
- Synchronous participant/public/auth handlers and audit database work execute
  outside the event loop. Each process admits at most six normal worker threads.
  A synchronous read request reuses one request-scoped SQLAlchemy session; it is
  closed when that handler finishes and is never shared between requests.
- Admission runs before authentication and audit work. Per process, short API
  work is limited to 32 in-flight requests, result waits to 128 and session streams
  to 2,048. One bearer token can occupy at most eight requests in each lane.
  Excess work returns 503 (global) or 429 (token) with `Retry-After: 1`, rather
  than building an unlimited database queue. Limits are configurable through
  `API_MAX_INFLIGHT_REQUESTS`, `API_MAX_WAIT_REQUESTS`,
  `API_MAX_SESSION_STREAMS` and `API_MAX_ACCOUNT_REQUESTS`. These are process
  concurrency limits, not exact cluster-wide requests-per-second quotas. No
  per-IP quota is used, so a school's shared external address does not penalize
  all its participants. Edge DDoS protection remains a separate concern.
- Compose limits API containers to 8 GiB and API SQL statements to ten seconds,
  with a 1.5-second database lock wait. These statement settings are API-only;
  migrations retain their own database policy. Pool/query saturation returns a
  retryable 503. A 503 on a submission does not prove the write was uncommitted:
  the browser does not automatically retry mutations, to avoid duplicate codes.
- Running public scoreboards use two-second snapshots. While a rebuild is in
  progress, a same-policy previous snapshot has a three-second cache lifetime
  from rebuild completion. This is not an end-to-end freshness deadline: SQL,
  calculation and browser polling add delay. Ended contests and internal/operator
  boards use commit invalidation.
  Current authorization, contest visibility, freeze boundary and release revision
  are checked before each response, including a 304. Policy changes never reuse
  a previous-policy snapshot. Access-log statistics cache for five seconds.
- Scoreboard queries fetch scoring columns only. Redis shares gzip JSON results
  across workers; a token-owned Redis lock coalesces cache misses across processes.
  Rebuilds have both a bounded wait and a per-process CPU/memory budget. Cache
  entries allow up to 16 MiB, avoiding the former 2 MiB cutoff for large boards.
  Encoded process-local caches are capped at 64 MiB and do not extend shared TTLs.
  Redis failure uses bounded local snapshots; authorization still queries SQL.
- Public scoreboard responses support gzip and conditional ETags. Their cached
  success envelope contains `data`; the unique correlation ID is provided by
  `X-Request-ID`, not embedded in the cached body. Scoreboard changes invalidate
  only the relevant contest; judge progress updates do not flush scoring data.
- Submission lists omit code. The browser fetches code only when a row is opened,
  and no longer duplicates list polling with one result wait per pending row.
  Hidden tabs pause polling; refresh and retry delays include jitter. Source byte
  counts and visible queue positions are computed in SQL without loading all code.
- Result waits batch pending IDs in groups of 500 every half-second. Waiting does
  not retain a SQL connection. Initial/final authorization uses a separate bounded
  thread executor and pool, and is checked again after the wait. Cancellation does
  not release executor capacity while its underlying SQL is still running.
- Authentication/revocation reads remain current. Session activity timestamps
  write at most once per 30 seconds using a conditional update. This reduces WAL
  and contention without caching permission or delaying forced logout.
- Nginx allows 8,192 connections per worker, enables JSON gzip and uses a 65,536
  descriptor limit. Participant source uploads are capped at 4 MiB at the proxy;
  operator problem archive uploads keep their existing larger limit.
- The web proxy adds a shared 400-request/second budget with a burst of 100,
  without delaying excess requests in another queue. Overload returns JSON 429,
  `Retry-After: 1` and `X-Request-ID` before occupying an API thread or SQL pool.
  This is a measured default for the 14-vCPU, approximately 40-GB web VM, not a
  maximum account count. API workers, SQL pools and request limits are tuned
  together; VM resource changes do not update these settings automatically.
  It is shared across API paths and addresses; health probes and private judge
  traffic do not consume this budget. Operator archive routes retain their own
  existing configuration. Align the overload CORS allowlist in
  `nginx-global/nginx.conf` with application CORS settings when adding a web origin.
  Nginx and the private gateway also rotate Docker logs at 10 MiB, three files.
  The main web configuration is directory-mounted at `/etc/nginx/zoj`; atomic
  Git file replacements are visible to subsequent validated Nginx reloads.
  The rate budget is per proxy instance; recalculate it before adding replicas.
  See [Nginx request limiting](https://nginx.org/en/docs/http/ngx_http_limit_req_module.html)
  for shared-zone and burst behavior.
- Session streams check watched session IDs in batches of 500 once per second,
  deduplicating tabs that watch the same session. This is independent of Redis and
  works across API workers. SQL errors do not falsely log out a user.
- Agent credential verification is reused for 30 seconds in bounded process
  memory. Every request still reads the node's current approval and stored hash;
  disabling a node or rotating its secret immediately invalidates authorization.

## Retention

Defaults selected for production:

| Environment variable | Default | Meaning |
| --- | ---: | --- |
| `ACCESS_LOG_RETENTION_DAYS` | 365 | Account access logs |
| `AUDIT_LOG_RETENTION_DAYS` | 365 | Operator/administrator action logs |
| `JUDGE_LOG_RETENTION_DAYS` | 90 | Agent diagnostic logs, including removed nodes |
| `LOG_CLEANUP_BATCH_SIZE` | 1000 | Maximum deletions per category per pass |

Setting a retention period to zero disables deletion for that category.
The notice worker performs cleanup hourly, or once per minute while a backlog
remains. Each category commits separately. PostgreSQL cleanup queries have a
three-second statement limit and a 500ms lock wait, and skip locked rows. Indexes
support selecting the oldest expired rows without scanning all logs.

Usage analytics keep their existing 395-day retention. Current submissions,
results, participant accounts and contest data are not subject to log cleanup.
Agent ingestion also retains its existing 5,000-row cap per active node.
Application worker/API console logs rotate at 10MB, retaining three files per
container. Redis is limited to 256MB and may evict derived cache results.

## Validation and operations

`tests/test_performance_controls.py` covers credential revocation/rotation,
cache expiry/failure/concurrent misses, scoreboard freeze boundaries and writes,
1,000-session batch queries, shared stream subscriptions and bounded retention.
Set `TEST_REDIS_URL` to run these checks against an actual disposable Redis.
CI runs the full backend suite with Redis enabled.

Operational checks should compare API and PostgreSQL CPU, connection counts,
lock waits, cache hit/miss counters and p95/p99 endpoint latency. An idle-time
measurement is not a concurrent-user capacity guarantee; use synthetic accounts
and an isolated database for load testing. Cleanup makes space reusable inside
PostgreSQL; it does not promise an immediate reduction in the data-file size.

The repeatable isolated load harness is in [tools/capacity](../tools/capacity/).
Distinguish accepted-response latency, overload rejections, unexpected failures
and load-generator dropped iterations. An unbounded request source cannot be
served without rejection by a finite machine. The 2026-10-16 production-host
canary used a separate database, synthetic identities and a dedicated Redis;
it did not send jobs to the real judge queue or replace the public API.


Participant inbox notifications use a separate contest-based policy. They
expire at the current contest end time plus `NOTIFICATION_GRACE_HOURS`
(default 12). The notice worker cleans batches of at most
`NOTIFICATION_CLEANUP_BATCH_SIZE` (1000) every
`NOTIFICATION_CLEANUP_INTERVAL_SECONDS` (60). API lists and counts exclude
expired entries before cleanup completes. See [notifications](notifications.md).
