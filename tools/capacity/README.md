# Contest API capacity checks

These tools use synthetic identities and a disposable database on a production
host. They must not seed the service database or send synthetic submissions to
the service's judge queue. They do not measure judge execution time.

`seed.py` refuses to run unless `APP_ENV=capacity`, the database name starts with
`zoj_capacity_`, and the database contains no contests. It creates three contests,
3,000 independent participants, 30 problems and 60,000 historical submissions.
All addresses are synthetic `example.com` addresses. It writes private test
credentials to `/capacity/actors.json` with mode 0600. Never commit that file,
environment files, database passwords, or request authorization headers.

## Prerequisites

- A separate PostgreSQL database and Redis instance; no service workers or judge
  agents may connect to that database.
- An API container named `zoj-capacity-api` using `APP_ENV=capacity`, the candidate
  app source and a 4 GiB memory limit. Use the existing locked application image
  for dependencies. Run four Uvicorn workers with six SQL connections plus one
  separate result-auth connection per worker, without SQL pool overflow.
- A proxy listening only on loopback, forwarding to that API. The default
  benchmark URL is `http://127.0.0.1:18081`. The harness verifies the API's
  environment before load. Do not point `BASE_URL` at the public service.
- Put the scripts, private `capacity.env` and private `actors.json` in a mode-0700
  folder. The runner reads credentials but never prints them.

`phases.py --folder /private/test/folder --only browser-1000-150rps` starts a
bounded k6 run. The pinned k6 image is recorded in `phases.py`. `workload.js`
supports:

| Parameter | Meaning |
| --- | --- |
| `USERS` | Unique participant accounts, up to 3,000 |
| `RATE` | Requests started per second; unset for continuous virtual users |
| `DURATION` | Finite test duration, e.g. `60s` |
| `PROFILE=interactive` | Scoreboard, submission list and problem list |
| `PROFILE=mixed` | Above plus pending-result long polls |
| `PROFILE=status` | Pending-result long polls only |
| `WRITES=true` | Replace 5% of requests with source submissions |
| `CONDITIONAL=false` | Disable ETag requests for a comparison |
| `PACE` | Optional think time for continuous-user runs |

Arrival-rate tests spread requests across all accounts and three contests.
Continuous-user tests keep the requested virtual users issuing requests without
think time. Initial activation is spread over five seconds; arrival-rate tests
have no ramp. Request timeout is eight seconds, with ten seconds of completion
grace. Do not infer a successful request from an iteration that was dropped.

The runner stops only its own load container after three failed live-service
health checks or if host available memory falls below 2 GiB. API-only health
readiness is checked first; wait for every candidate worker to finish startup
before measuring steady state. Container startup, proxy 502 responses and warm
cache behavior must be reported separately from steady-state capacity.

`metrics.py --seconds 60 --output /private/test/folder/metrics.jsonl` records
aggregate PostgreSQL counters, connection/lock counts, available memory and load.
It assumes the existing host PostgreSQL container is `deploy-postgres-1`; adapt
that name only to the intended host. It does not query user records.

## Interpreting evidence

Record accepted 200/304 responses, 429/503 overload responses, unexpected errors,
dropped iterations and successful-response p95/p99 independently. A low latency
that consists mostly of rejected requests is not high useful throughput. The
per-kind counters include all responses, whereas `success_*` excludes rejections.
The overall k6 `http_req_failed` also counts intentional 429/503 responses.

Backend submissions are verified by comparing accepted HTTP responses to rows
and queued jobs in the disposable database. This is acceptance/queueing, not
execution. No claim about actual judge CPU time follows from this test.

A minute or two demonstrates a bounded short run, not a multi-hour soak or a
guarantee for all API routes. This workload does not cover login email delivery,
large archive upload, external TLS/WAN bandwidth, or active judge workloads.
They require separate scenarios. Recheck capacity after changing VM resources,
worker counts, SQL pools, the schema or scoreboard size.

Use `--label candidate` or `--label baseline` to keep independent summaries and
source submission receipts. The manifest records the target's health version.
`streams.py --users 1000 --seconds 135 --output /capacity/streams.json` holds
session-event connections during a separate k6 run. It requires `httpx` from the
application image, verifies the target environment, and reports counts only.

After collecting summaries, remove only the test containers, test database and
private credentials created for the run. Verify public health and deployment
version again. Keep sanitized JSON summaries and aggregate evidence only.
