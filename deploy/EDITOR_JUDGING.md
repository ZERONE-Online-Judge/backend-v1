# Standalone Editor judging

`/api/editor/judge` is available only on the existing private Editor ingress.
It uses the publishing bridge HMAC, authenticated common account identity,
content hashes, nonce replay protection and TLS. It accepts no client URLs.

Migration `0047_editor_judge_runs` adds an independent queue. Contest jobs
are claimed first; remaining slots can take Editor jobs using the existing
node credential and lease protocol. No contest, submission, score, staff
permission or participant notification is created for an Editor run.

Snapshot format `zoj-editor-judge-v1` accepts only batch standard I/O and
cpp17, c99, python313 and java8. It provides both the test bundle and real
per-object fallback paths. Node-side resource limits and TLE confirmation
remain unchanged. Results retain the assigned node and agent version.

The Editor can request remaining tests after an observed first failure.
The server removes only that recorded test from immutable stored data.
Repeated operation IDs are idempotent; cancellation and expired leases
reject stale results. Dataset objects are reused across solutions and
subsets. Derived subset bundles are removed on completion. The notice
worker expires unfinished work after one hour and collects terminal work
older than 48 hours without deleting other runs' shared objects.

Deploy this backend before enabling `EDITOR_JUDGE_BACKEND=zoj` in Editor.
The existing Editor bridge key and private CA are reused; no agent update
is required. Editor stores durable user-facing results independently of
these short-lived dispatch records. The Editor repository provides the
manual `Physical Judge Integration Check` workflow for deployed testing.
