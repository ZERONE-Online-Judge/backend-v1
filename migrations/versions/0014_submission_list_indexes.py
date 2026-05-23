"""submission list indexes

Revision ID: 0014_submission_list_indexes
Revises: 0013_judge_node_agent_version
Create Date: 2026-05-23 00:00:00.000000
"""

from alembic import op


revision = "0014_submission_list_indexes"
down_revision = "0013_judge_node_agent_version"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_submissions_submitted_id "
        "ON submissions (submitted_at DESC, submission_id DESC)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_submissions_contest_submitted_id "
        "ON submissions (contest_id, submitted_at DESC, submission_id DESC)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_submissions_contest_division_submitted_id "
        "ON submissions (contest_id, division_id, submitted_at DESC, submission_id DESC)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_submissions_contest_team_submitted_id "
        "ON submissions (contest_id, participant_team_id, submitted_at DESC, submission_id DESC)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_submissions_contest_problem_submitted_id "
        "ON submissions (contest_id, problem_id, submitted_at DESC, submission_id DESC)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_submissions_contest_problem_submitted_id")
    op.execute("DROP INDEX IF EXISTS idx_submissions_contest_team_submitted_id")
    op.execute("DROP INDEX IF EXISTS idx_submissions_contest_division_submitted_id")
    op.execute("DROP INDEX IF EXISTS idx_submissions_contest_submitted_id")
    op.execute("DROP INDEX IF EXISTS idx_submissions_submitted_id")
