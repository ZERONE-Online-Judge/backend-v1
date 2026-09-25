"""Require an explicit request before starting a verification analysis."""

from alembic import op
import sqlalchemy as sa

revision = "0038_manual_verification"
down_revision = "0037_verification_tasks"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "verification_analyses", sa.Column("requested_at", sa.DateTime(timezone=True))
    )
    # Free-form tasks were always explicit requests; already running work may finish.
    op.execute(
        """UPDATE verification_analyses SET requested_at = COALESCE(started_at, created_at)
        WHERE status = 'running' OR analysis_id IN (SELECT analysis_id FROM verification_tasks)"""
    )
    op.execute("""UPDATE verification_analyses SET status = 'awaiting_request'
        WHERE status = 'queued' AND requested_at IS NULL""")


def downgrade():
    op.execute(
        "UPDATE verification_analyses SET status = 'queued' WHERE status = 'awaiting_request'"
    )
    op.drop_column("verification_analyses", "requested_at")
