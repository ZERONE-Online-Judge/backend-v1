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

- `read_at`: the recipient opened or explicitly marked the item read.
- `dismissed_at`: the floating toast was closed; this does not mark it read.
- `deleted_at`: hidden from the recipient's inbox; the record is retained.

`GET /api/notifications` returns the newest page, `next_offset`,
`unread_count` and `total_count`. Counts include unloaded pages and exclude
hidden items. Optional `contest_id`, `offset` and `limit` scope/paginate it;
page sizes are capped at 100. The feed uses the indexed recipient/time
query, and the header polls only this endpoint while visible.

`POST /api/notifications/{id}/read`, `/unread` and `/dismiss` update each
state. `DELETE /api/notifications/{id}` hides the item idempotently.
`POST /api/notifications/read-all` marks the current recipient's inbox read,
including unloaded pages; events created after the request remain unread.
These states are saved across login sessions and devices.

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
No automatic notification expiry is introduced.

Tests cover offline delivery, recipient isolation, duplicate delivery,
read/dismiss/delete independence, unloaded counts, source transaction
rollback, preview exclusions, ordinary versus staged judging events and
account email reassignment. PostgreSQL checks run only in a local disposable
`*_rejudging_test` database and verify upgrade/downgrade preservation.
