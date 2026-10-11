"""Preserve old and candidate results until an operator decides to apply them."""
from alembic import op
import sqlalchemy as sa

revision = "0040_staged_rejudging"
down_revision = "0039_restore_verification_runs"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table("rejudgings",
        sa.Column("rejudging_id", sa.String(36), primary_key=True),
        sa.Column("contest_id", sa.String(36), sa.ForeignKey("contests.contest_id"), nullable=False),
        sa.Column("problem_id", sa.String(36), sa.ForeignKey("problems.problem_id"), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("scope", sa.JSON(), nullable=False),
        sa.Column("judge_context", sa.JSON(), nullable=False),
        sa.Column("context_hash", sa.String(64), nullable=False),
        sa.Column("requested_by_name", sa.String(120), nullable=False),
        sa.Column("requested_by_email", sa.String(255), nullable=False),
        sa.Column("decided_by_email", sa.String(255), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("decided_at", sa.DateTime(timezone=True), nullable=True),
    )
    for name in ("contest_id", "problem_id", "status"):
        op.create_index(f"ix_rejudgings_{name}", "rejudgings", [name])
    op.create_table("rejudging_items",
        sa.Column("rejudging_item_id", sa.String(36), primary_key=True),
        sa.Column("rejudging_id", sa.String(36), sa.ForeignKey("rejudgings.rejudging_id"), nullable=False),
        sa.Column("submission_id", sa.String(36), sa.ForeignKey("submissions.submission_id"), nullable=False),
        sa.Column("judge_job_id", sa.String(36), sa.ForeignKey("judge_jobs.judge_job_id"), nullable=False),
        sa.Column("active_submission_id", sa.String(36), nullable=True),
        sa.Column("original_result", sa.JSON(), nullable=False),
        sa.Column("candidate_result", sa.JSON(), nullable=False),
        sa.Column("verification_context_hash", sa.String(64), nullable=True),
        sa.UniqueConstraint("judge_job_id"), sa.UniqueConstraint("active_submission_id"),
    )
    op.create_index("ix_rejudging_items_rejudging_id", "rejudging_items", ["rejudging_id"])
    op.create_index("ix_rejudging_items_batch_submission", "rejudging_items", ["rejudging_id", "submission_id"])


def downgrade():
    op.drop_table("rejudging_items")
    op.drop_table("rejudgings")
