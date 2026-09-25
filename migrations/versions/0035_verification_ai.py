"""Persistent verification runs, immutable context metadata, and shared AI reports."""

from alembic import op
import sqlalchemy as sa

revision = "0035_verification_ai"
down_revision = "0034_log_retention_indexes"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "verification_snapshots",
        sa.Column("context_hash", sa.String(64), primary_key=True),
        sa.Column("contest_id", sa.String(36), nullable=False),
        sa.Column("problem_id", sa.String(36), nullable=False),
        sa.Column("context", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_table(
        "verification_runs",
        sa.Column("submission_id", sa.String(36), primary_key=True),
        sa.Column("contest_id", sa.String(36), nullable=False),
        sa.Column("problem_id", sa.String(36), nullable=False),
        sa.Column("asset_id", sa.String(36), nullable=False),
        sa.Column("expected_status", sa.String(32), nullable=False),
        sa.Column("context_hash", sa.String(64)),
        sa.Column("analysis_id", sa.String(36)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_table(
        "verification_analyses",
        sa.Column("analysis_id", sa.String(36), primary_key=True),
        sa.Column("contest_id", sa.String(36), nullable=False),
        sa.Column("problem_id", sa.String(36), nullable=False),
        sa.Column("cache_key", sa.String(64), nullable=False, unique=True),
        sa.Column("status", sa.String(24), nullable=False),
        sa.Column("model", sa.String(100), nullable=False),
        sa.Column("context_hash", sa.String(64), nullable=False),
        sa.Column("evidence", sa.JSON(), nullable=False),
        sa.Column("report", sa.JSON()),
        sa.Column("coverage", sa.JSON()),
        sa.Column("error_message", sa.Text()),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("claim_token", sa.String(36)),
        sa.Column("usage", sa.JSON()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
    )
    for table, columns in {
        "verification_snapshots": ["contest_id", "problem_id"],
        "verification_runs": ["contest_id", "problem_id", "asset_id", "analysis_id"],
        "verification_analyses": [
            "contest_id",
            "problem_id",
            "status",
            "created_at",
            "started_at",
        ],
    }.items():
        for column in columns:
            op.create_index(f"ix_{table}_{column}", table, [column])
    op.create_index(
        "idx_verification_run_asset_created",
        "verification_runs",
        ["problem_id", "asset_id", "created_at"],
    )


def downgrade():
    for table in (
        "verification_analyses",
        "verification_runs",
        "verification_snapshots",
    ):
        op.drop_table(table)
