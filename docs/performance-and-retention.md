# Web load and log retention

The API keeps PostgreSQL authoritative for authentication and authorization.
Redis stores derived results only; a cache failure falls back to database reads.

## Request handling

- The production Compose API runs two Uvicorn workers by default. Set
  `API_WORKERS` in the deployment shell/Compose environment to change this.
- Each process has a PostgreSQL pool of five persistent connections plus five
  overflow connections, with a five-second pool wait. Include background workers
  and both API colors during deployment when budgeting against `max_connections`.
- Blocking scoreboard and audit-log GET handlers execute in FastAPI's thread
  pool, so database/Redis waits do not block the API event loop.
- Scores cache for two seconds and access-log statistics for five seconds.
  Committed changes invalidate the appropriate shared cache generation. Public
  and operator scoreboards have separate keys. Current contest policy, time-based
  freeze state and release revision are checked before every scoreboard lookup.
- Scoreboard queries do not fetch submission source code. Concurrent misses are
  coalesced inside each API worker. Redis shares results between workers.
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
