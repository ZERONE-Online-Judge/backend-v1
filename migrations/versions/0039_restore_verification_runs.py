"""Link unambiguous legacy operator judgments to their verification files."""

from collections import defaultdict
import hashlib
import re

from alembic import op
import sqlalchemy as sa

revision = "0039_restore_verification_runs"
down_revision = "0038_manual_verification"
branch_labels = None
depends_on = None


def upgrade():
    db = op.get_bind()
    assets = sa.table(
        "problem_assets",
        sa.column("asset_id", sa.String),
        sa.column("contest_id", sa.String),
        sa.column("problem_id", sa.String),
        sa.column("storage_key", sa.String),
        sa.column("sha256", sa.String),
        sa.column("created_at", sa.DateTime(timezone=True)),
    )
    submissions = sa.table(
        "submissions",
        sa.column("submission_id", sa.String),
        sa.column("contest_id", sa.String),
        sa.column("problem_id", sa.String),
        sa.column("submission_kind", sa.String),
        sa.column("source_code", sa.Text),
        sa.column("submitted_at", sa.DateTime(timezone=True)),
    )
    runs = sa.table(
        "verification_runs",
        sa.column("submission_id", sa.String),
        sa.column("contest_id", sa.String),
        sa.column("problem_id", sa.String),
        sa.column("asset_id", sa.String),
        sa.column("expected_status", sa.String),
        sa.column("created_at", sa.DateTime(timezone=True)),
    )
    trials = sa.table("verification_trials", sa.column("submission_id", sa.String))
    by_source = defaultdict(list)
    for asset in db.execute(sa.select(assets).where(
        assets.c.storage_key.like("%/verification-solutions/%")
    )).mappings():
        match = re.search(
            r"/verification-solutions/(accepted|wrong_answer|time_limit_exceeded|memory_limit_exceeded)/",
            asset["storage_key"],
        )
        if match:
            by_source[(asset["contest_id"], asset["problem_id"], asset["sha256"])].append(
                (asset, match[1])
            )
    if not by_source:
        return
    query = sa.select(submissions).where(
        submissions.c.submission_kind == "operator_test",
        sa.exists(sa.select(assets.c.asset_id).where(
            assets.c.contest_id == submissions.c.contest_id,
            assets.c.problem_id == submissions.c.problem_id,
            assets.c.storage_key.like("%/verification-solutions/%"),
        )),
        ~sa.exists(sa.select(runs.c.submission_id).where(
            runs.c.submission_id == submissions.c.submission_id,
        )),
        ~sa.exists(sa.select(trials.c.submission_id).where(
            trials.c.submission_id == submissions.c.submission_id,
        )),
    )
    batch = []
    for submission in db.execute(query.execution_options(yield_per=250)).mappings():
        source_hash = hashlib.sha256(submission["source_code"].encode("utf-8")).hexdigest()
        candidates = by_source.get((submission["contest_id"], submission["problem_id"], source_hash), [])
        # Never guess between duplicate files or attach a judgment that predates a file.
        if len(candidates) != 1:
            continue
        asset, expected = candidates[0]
        if submission["submitted_at"] < asset["created_at"]:
            continue
        batch.append({
            "submission_id": submission["submission_id"],
            "contest_id": submission["contest_id"],
            "problem_id": submission["problem_id"],
            "asset_id": asset["asset_id"],
            "expected_status": expected,
            # Preserve request ordering; do not fabricate a historical snapshot or AI report.
            "created_at": submission["submitted_at"],
        })
        if len(batch) >= 250:
            db.execute(runs.insert(), batch)
            batch = []
    if batch:
        db.execute(runs.insert(), batch)


def downgrade():
    # Data-only repair: retain recovered history when rolling back application code.
    pass
