"""Resumable tool-based verification and isolated judge trials."""

from alembic import op
import sqlalchemy as sa

revision = "0036_verification_agent"
down_revision = "0035_verification_ai"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "verification_analyses",
        sa.Column("engine_version", sa.Integer(), nullable=False, server_default="1"),
    )
    op.add_column("verification_analyses", sa.Column("agent_state", sa.JSON()))
    op.add_column(
        "verification_analyses", sa.Column("heartbeat_at", sa.DateTime(timezone=True))
    )
    op.add_column(
        "verification_analyses", sa.Column("next_step_at", sa.DateTime(timezone=True))
    )
    op.create_index(
        "ix_verification_analyses_next_step_at",
        "verification_analyses",
        ["next_step_at"],
    )
    op.create_table(
        "verification_trials",
        sa.Column("submission_id", sa.String(36), primary_key=True),
        sa.Column("analysis_id", sa.String(36), nullable=False),
        sa.Column("contest_id", sa.String(36), nullable=False),
        sa.Column("problem_id", sa.String(36), nullable=False),
        sa.Column("context_hash", sa.String(64), nullable=False),
        sa.Column("cache_key", sa.String(64), nullable=False, unique=True),
        sa.Column("artifact_id", sa.String(100), nullable=False),
        sa.Column("testcase_orders", sa.JSON()),
        sa.Column("testcase_count", sa.Integer(), nullable=False),
        sa.Column("probe", sa.JSON()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    for column in ("analysis_id", "contest_id", "problem_id"):
        op.create_index(
            f"ix_verification_trials_{column}", "verification_trials", [column]
        )


def downgrade():
    op.drop_table("verification_trials")
    op.drop_index("ix_verification_analyses_next_step_at", "verification_analyses")
    for column in ("next_step_at", "heartbeat_at", "agent_state", "engine_version"):
        op.drop_column("verification_analyses", column)
