"""Goal-driven verification tasks with shared continuations."""

from alembic import op
import sqlalchemy as sa

revision = "0037_verification_tasks"
down_revision = "0036_verification_agent"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "verification_tasks",
        sa.Column("analysis_id", sa.String(36), primary_key=True),
        sa.Column("contest_id", sa.String(36), nullable=False),
        sa.Column("problem_id", sa.String(36), nullable=False),
        sa.Column("parent_task_id", sa.String(36)),
        sa.Column("goal", sa.Text(), nullable=False),
        sa.Column("source_asset_id", sa.String(36)),
        sa.Column("created_by", sa.String(36)),
        sa.Column(
            "cancel_requested", sa.Boolean(), nullable=False, server_default=sa.false()
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    for name in ("contest_id", "problem_id"):
        op.create_index(f"ix_verification_tasks_{name}", "verification_tasks", [name])


def downgrade():
    op.drop_table("verification_tasks")
