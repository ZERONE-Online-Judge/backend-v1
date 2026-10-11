# Staged rejudging

Operators with both `contest.submission.view` and `contest.problem.test`
can request rejudging of a completed submission, or completed participant
submissions for a problem (optionally filtered by team and language).
Requests require a reason. Pending submissions and future submissions are
excluded from a bulk request. AI verification trials are immutable.

Each request stores an immutable judge manifest (tests, package resources,
checker/validator hashes and resource limits). The existing judge agents
receive their normal claim payload; agent upgrades are not required.
Cached bundles are reused; missing bundles are queued for the existing
background builder. Bundle versions include per-test resource overrides.
Ordinary jobs have priority over rejudging, followed by AI trial jobs.

Rejudging creates jobs and separate result records, not new submissions.
Claiming, progress, final reporting, lease expiry and credential rotation
only change the candidate result. Official verdicts, submission timestamps,
participant lists, verification analysis links and scores stay unchanged.

All candidate results must be terminal and free of system errors before
an operator can apply a batch. Applying verifies that judge inputs and
original results/source have not changed. It updates the same submission
IDs atomically, retains previous results and previous analysis references,
refreshes verification context and invalidates scoreboard caches through
the existing transaction hooks. Penalties use original submission times.
Already released scoreboard snapshots retain the existing release policy.

Applying a changed participant verdict creates a durable inbox notification
for its author in the same transaction. It contains the old/new verdicts,
operator-entered reason, and a link to the submission. Unchanged verdicts,
cancelled/unapplied results and operator/preview/AI submissions do not notify.
Legacy shared-team submissions without an author notify current team members.
Only the recipient's active session can list, read or hide the notification;
read/hidden state persists across devices. No email is sent.

A nullable unique active submission key prevents overlapping requests,
including batches waiting for review. Apply/cancel are serialized with
job-first locks and a batch compare-and-set. Stale jobs cannot report
progress or results after cancellation. Running code may finish locally;
cancellation prevents its results from being accepted rather than killing
a VM process immediately. Completed candidate evidence remains available.

## API

All paths are under `/api/operator/contests/{contest_id}`:

- `POST /rejudgings`: `{submission_id, reason}` or
  `{problem_id, team_id?, language?, reason}`.
- `GET /rejudgings?offset=0&limit=20`: paginated summaries and total count.
- `GET /rejudgings/{id}?offset=0&limit=50`: paginated old/new results.
- `POST /rejudgings/{id}/apply`: atomically apply all candidate results.
- `POST /rejudgings/{id}/cancel`: release active keys and revoke leases.

Read-only submission viewers can inspect records but cannot request or
apply/cancel a batch. Cross-contest records cannot be read or modified.
Request, apply and cancellation are recorded in the existing audit log.
The existing system-error immediate retry endpoint remains available and
rejects submissions that have an active staged rejudging.

The recipient inbox uses `GET /api/notifications/rejudging` (paginated),
`POST /api/notifications/rejudging/{id}/read`, and
`DELETE /api/notifications/rejudging/{id}` (hide). A general login sees its
own memberships across contests; a legacy participant token also requires
`contest_id` and is restricted to its own member in that contest. Hidden
notifications retain their evidence and never reappear in the server feed.

## Deployment and rollback

Run `alembic upgrade head` before serving the new backend/frontend. Revision
`0040_staged_rejudging` creates `rejudgings` and `rejudging_items`, indexes,
foreign keys, and unique active submission/job constraints.
Revision `0041_rejudging_notifications` adds the durable recipient inbox.
Revision `0042_notification_inbox` promotes it to the unified inbox,
preserving verdict-change history, read states and hidden entries.

Cancel pending batches before rolling application code back: old agents
are compatible, but old backends do not recognize staged jobs. Leave the
new tables in place to preserve evidence. A schema downgrade to `0039`
removes the new tables (and their evidence) without changing submissions;
use it only for a disposable test database or after archiving the records.

## Verification

`tests/test_staged_rejudging.py` covers input snapshots, authorization,
unchanged official verdicts, score calculation, released snapshots,
verification analysis history, cancellation, lease recovery, credential
revocation, prioritization, scope filters and concurrent decisions.

Optional actual PostgreSQL checks use a local disposable database whose
name ends in `_rejudging_test`:

```sh
REJUDGING_TEST_POSTGRES_URL=postgresql+psycopg://... \
  python -m pytest tests/test_rejudging_postgres.py -q
```

These tests replace the database schema and check the real migration,
duplicate requests, apply/cancel races and complete transaction rollback.
Never point that test variable at a deployment database.
