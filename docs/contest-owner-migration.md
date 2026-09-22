# Contest owner migration

Each contest with staff has one owner. The first master assignment becomes the
owner; later master assignments remain ordinary masters. Only the current owner
can transfer ownership to another existing contest operator. The former owner
keeps master access. Ordinary role edits and removals cannot remove ownership.

Migration `0029_contest_owner` (after `0028_usage_analytics`) keeps the existing protected master when there is
exactly one, or the only master when there is just one master. Contests with no
staff remain without an owner until their first assignment.

For contests with several eligible masters, an administrator must explicitly
choose the initial owner. The migration stops before deployment when that choice
is missing or does not match a current master. Provide a JSON mapping from
contest IDs to existing account emails through `CONTEST_OWNER_MIGRATION_ASSIGNMENTS`
in the migration container's environment. For example:

```json
{"contest-id": "owner@example.com"}
```

The optional `"*"` key supplies the same account for all ambiguous contests. It
does not override a uniquely identified owner. Do not commit real account emails
to source control. Remove the temporary setting after the migration completes.
The deployment compose migration service reads `deploy/env/backend.env`.

Already assigned contests are skipped on subsequent runs, including contests
whose ownership has been transferred. Deploy the backend migration before the
frontend that shows the owner role and delegation controls.

Verification: every contest with non-service staff must have an
`owner_staff_account_id`; exactly that account has role `owner`, scopes
`contest.*` and `contest.owner`, and the legacy compatibility protection flag.
The API checks the current owner while locking the contest and commits both
accounts' role changes and the owner ID together.
