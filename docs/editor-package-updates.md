# Verified Editor package updates

Editor and the ZOJ problem library require a completed, verified ZOJ package.
Unverified exports remain downloadable in Editor for archival use, but cannot
be published, imported as an Editor ZIP, or applied to a ZOJ problem.

The library accepts `testsets` (up to 20 names) as well as the legacy single
`testset`. Selected sets are combined into the new active ZOJ test version.
Personal, directly shared and team-folder projects use current publish grants.

## Updating an existing problem

In ZOJ, open **Editor에서 가져오기**, prepare the verified package, and select
an existing problem as the destination. In Editor, select a current verified
package and use **새 패키지로 업데이트** in a previous ZOJ publication record.

Both paths preview the target problem and archive hashes, changed settings,
bounded statement excerpts, changed file names and testcase changes. Asset IDs
are normalized by content hash when comparing statements. Apply requires a
reason; a running contest also requires an explicit acknowledgment. Changing
the target or archive invalidates the confirmation. Pending staged rejudging
blocks package updates until the operator applies or cancels that rejudging.

The transaction rechecks the operator's current permissions and locks the
contest, problem and current child rows. Before changing data it exports the
previous problem to a private ZIP backup. Storage failure rolls back database
changes and cleans up newly written objects. Retries reuse an atomic receipt.

The existing problem ID, division, problem code, order, allowed languages and
language-specific resource overrides are preserved. Statement translations,
editorials, base limits and package assets are replaced. Old tests remain as
inactive versions; incoming versions receive new, increasing version numbers.
Old object contents remain available to already-issued immutable judge URLs.

Ordinary judge claims skip a problem locked by an update and retry later,
preventing mixed old/new metadata, testcases and package files. Existing
submissions, their timestamps and verdicts are not changed. Operators use the
existing staged rejudging workflow to change official results separately.

## Audit and backups

Update reasons, source project/revision (when supplied), prior target hash and
backup reference are stored in the same transaction as the update. ZOJ's
problem ZIP card lists up to 50 retained update audit records. Backup downloads
require the current problem-resource-view permission and a matching receipt;
they are not participant statement assets. Audit history follows the existing
operational-log retention policy; backups are separate private objects.

No database migration or automatic production-contest update is introduced.
