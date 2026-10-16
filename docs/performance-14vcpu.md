# Capacity profile for the 14-vCPU web VM

Measured on 2026-10-16 on `zoj-app`: 14 guest vCPUs and approximately 40 GB
allocated RAM (39.17 GiB visible). This is the selected configuration among
the profiles tested, not a guarantee of a universal maximum throughput.

## Selected deployment

| Setting | Value |
| --- | ---: |
| Uvicorn processes | 14 |
| Normal SQL pool per process | 3, no overflow |
| Separate result-auth SQL pool per process | 1 |
| Normal worker threads per process | 6 |
| Normal in-flight requests per process | 32 |
| API container memory limit | 8 GiB |
| Web proxy budget | 400 requests/s, burst 100 |
| PostgreSQL maximum connections | 160 |
| Both API colors + background + migration pool budget | 122 |

The worker, SQL and admission settings must be considered together. Increasing
the SQL pool to four and in-flight admission to 48 made latency worse with
1,000 session streams; that candidate also returned one unexpected HTTP 500
and was rejected. Its root cause was not established. Increasing processes
to sixteen did not improve the observed useful throughput over fourteen.

Six threads and three SQL connections per process gave session monitoring
room to share the normal executor and pool without using an unbounded queue.
The dedicated result-wait authentication pool is additional to this pool.
Session monitoring does not use that dedicated result-authentication pool.

## Method

- A disposable database on the existing PostgreSQL instance, a separate Redis,
  and a loopback-only candidate API/proxy on the production web VM.
- Three contests, 3,000 separate synthetic participant accounts, 30 problems
  and 60,000 historical submissions. No real account credentials were used.
- The deployed `8442ab4` application image was kept constant. These changes
  alter deployment configuration; application handler code is unchanged.
- The primary request mix rotates scoreboard, submission list and problem
  list requests, replacing every twentieth request with a submission. The
  mixed profile additionally includes two-second result waits.
- k6 2.3.0 ran in a container limited to two CPUs and 4 GiB on the same VM.
  Requests used authentication, gzip and conditional scoreboard requests.
  Latencies below include successful responses only, with client blocked time.
- Each accepted submission was reconciled against both its submission row
  and queued judge job. No judge agent or background worker consumed this DB.
- Session-stream tests started the API workload only after all 1,000 streams
  returned HTTP 200. They remained open throughout the final 180-second run.
- Later stability runs preserved the growing synthetic queue. The final
  session-stream run began with 14,320 queued jobs, in addition to history.

## Results

The first two rows compare clean starts without session streams. Subsequent
rows are distinct scenarios and must not be treated as identical workloads.

| Scenario | Requests | Accepted | 429/503 | Other failures | Submission p95 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Old 4-worker configuration, 200/s, 60s | 12,001 | 11,142 | 859 | 0 | 1,413 ms |
| 14 workers, pool 2, threads 4, 500/s, 45s, no edge cap | 22,500 | 22,353 | 147 | 0 | 731 ms |
| Final profile, 1,000 established streams + 400/s, 180s | 72,001 | 71,805 | 196 | 0 | 675 ms |
| Final profile, 500 KB source padding, 300/s, 30s | 9,000 | 8,474 | 526 | 0 | 1,097 ms |
| Final profile, 3,000 accounts, mixed 1,000/s, 20s | 20,001 | 7,668 | 12,333 | 0 | 1,417 ms |
| Final profile, 1,000 continuous users, mixed, 20s | 87,322 | 6,702 | 80,620 | 0 | 2,618 ms |

In the final stream test, successful scoreboard p95 was 693 ms and p99 was
1,268 ms; successful submission p99 was 1,194 ms. Of 72,001 requests, 99.73%
succeeded and 0.27% were deliberately rejected under saturation. All 3,592
accepted submissions had matching queue rows. Peak total PostgreSQL connections
were 75, with no sampled database lock waits.

The large-source test accepted and reconciled 421 submissions. Its 500,000-byte
padding was deterministic random ASCII inside a valid C++ comment, rather than
highly compressible spaces. The fixed-rate overload and continuous-user tests
accepted and reconciled 336 and 369 submissions respectively. All these final
scenarios had zero dropped iterations and no production-health guard stop.

The proxy intentionally rejects excess traffic before it consumes API/SQL
capacity. A low latency consisting mainly of 429 responses is not throughput.
The continuous-user run shows the protection behavior; it does not mean the
server successfully serves 87,322 requests in twenty seconds.

## Limits and interpretation

- These tests cover API submission acceptance and queue insertion, not code
  execution, final verdict delivery or busy judge-agent traffic. They do not
  measure production contest scoring changes driven by active judges.
- The traffic is local to the VM. External TLS, network bandwidth, OTP delivery,
  login storms, archive uploads and operator-heavy endpoints need other tests.
- RAM was not exhausted. Adding memory alone does not parallelize Python work
  or remove executor/database contention. PostgreSQL cache parameters remain
  unchanged; the current real database is small relative to available memory.
- A 400/s budget is a protection setting, not a promise that every mix fits.
  Near-limit source files already cause rejections at 300/s. For those requests,
  allow more recovery time rather than interpreting 429/503 as a successful save.
- The final stream run lasted three minutes. This is not an hours-long soak.
  A browser population of 1,000 users is not the same as 1,000 requests/s.
- Host-side PVE affinity could not be read because browser access was denied
  by the tool. The measured guest has 14 vCPUs; no host affinity changes are
  claimed. Guest CPU-steal samples alone do not prove physical-core exclusivity.
- One comparison overlapped a profiler and two setup attempts had controller
  overlap. Those runs were excluded from profile ranking. The controller was
  then protected by an exclusive file lock. Only sanitized profile summaries
  are retained; neither raw process-memory samples nor credentials are published.

Deployment validates the rendered pool budget against the running PostgreSQL
limit before changing services, including both colors during rollout. See
[deployment operations](../deploy/README.md) and the
[repeatable load tools](../tools/capacity/README.md).
