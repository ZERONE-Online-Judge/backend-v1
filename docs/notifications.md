# Participant inbox

New contest notices, answers to a participant team's questions, completed
participant judging results and applied verdict changes are stored in
`notifications`. Each recipient has a separate record and state. Creation
shares the source transaction, so failed writes cannot leave a published
result without its notification. No active participant session is needed.

Notices go to current contest members; answers go only to the question's
team. Judging results go to the submitting member. Older submissions with
no author notify their current team members. Operator preview/test teams,
verification runs and unapplied candidate verdicts do not produce alerts.
An unchanged applied verdict does not create a verdict-change alert.

Event/recipient uniqueness prevents repeated delivery from resetting read
or hidden state. The event key uses the judge job ID for ordinary results
and the rejudging item ID for changed verdicts, so later legitimate retries
and changes remain separate events. Scheduled notices use their existing
stable notice IDs. Editing a notice does not create another alert.

A general login accesses only current memberships for its own email. A
legacy participant login also requires its contest ID. Session revocation
and expiry are checked on every request; another recipient's IDs return
404. Recipient emails are snapshotted so assigning a member to another
email cannot expose the previous account's notification history. Synthetic
preview credentials cannot access the inbox. No recipient identities are
returned in the response.

## States and API

- `read_at`: the recipient opened/closed the toast, opened the full inbox,
  or explicitly marked the item read.
- `dismissed_at`: the floating toast was closed; closing also sets read_at
  atomically and preserves any earlier read timestamp.
- `deleted_at`: hidden from the recipient's inbox; the record is retained.

`GET /api/notifications` returns the newest page, `next_offset`,
`unread_count` and `total_count`. Counts include unloaded pages and exclude
hidden/expired items. Optional `contest_id`, `offset` and `limit` scope/paginate it;
page sizes are capped at 100. The feed uses the indexed recipient/time
query, and the header polls only this endpoint while visible.

`POST /api/notifications/{id}/read`, `/unread` and `/dismiss` update each
state. `DELETE /api/notifications/{id}` hides the item idempotently.
`POST /api/notifications/read-all` marks the current recipient's inbox read,
including unloaded pages; events created after the request remain unread.
Opening the header inbox automatically calls read-all once after a
successful feed load; a failed load does not mark unseen items read. Later
arrivals and manually unread entries are not repeatedly marked read by
polling. These states are saved across login sessions and devices.

The legacy `/api/notifications/rejudging` API remains available and filters
only verdict-change notifications. The frontend no longer stores private
notification bodies or infers notifications by polling source lists. The
old unscoped browser cache is cleared, not imported into another account.
Previous browser-only notifications cannot be reconstructed reliably;
durable storage starts with new events after deploying this feature.

## Migration and verification

Apply `0042_notification_inbox` before deploying the new header. It renames
and extends the prior inbox and preserves its IDs and read/hidden states.
Downgrading to 0041 discards other notification types, so archive them first;
application rollback should retain the schema and records when possible.
Revision `0043_notification_expiry` adds the contest/notification index
used by bounded cleanup. Apply it before running the updated notice worker.

## Lifetime

Contest notifications are visible until the configured contest end time
plus `NOTIFICATION_GRACE_HOURS` (default 12). The current end_at determines
expiry, so changing the end time changes existing notifications' lifetime.
Draft and schedule-TBD placeholder dates are excluded from expiry.

Expired items immediately drop out of API feeds, counts and update/read-all
operations even when the worker is unavailable. New events after that
contest's expiry do not create inbox records. Original contest notices,
answers, submissions and rejudging evidence retain their own lifecycle.

The existing notice worker permanently deletes expired notification rows,
including read, unread and soft-deleted entries. It checks every
`NOTIFICATION_CLEANUP_INTERVAL_SECONDS` (default 60), deletes at most
`NOTIFICATION_CLEANUP_BATCH_SIZE` (default 1000) in a transaction and resumes
quickly when a full batch was removed. PostgreSQL statement/lock timeouts
bound each cycle, and skip-locked row locks allow concurrent workers while
serializing expiry with contest rescheduling. The public API never performs
bulk cleanup during a participant request. Deleted expired records cannot
be restored by later extending the contest schedule.

Tests cover offline delivery, recipient isolation, duplicate delivery,
automatic and manual read states, deletion, unloaded counts, source transaction
rollback, preview exclusions, ordinary versus staged judging events and
account email reassignment. Expiry tests cover boundary times, schedule
extensions, bounded cleanup and worker retries. PostgreSQL checks run only
in a local disposable `*_rejudging_test` database and verify migration
preservation and skip-locked cleanup during schedule changes.
