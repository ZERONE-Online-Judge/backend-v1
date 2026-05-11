"""bundle warm queue

Revision ID: 0006_bundle_warm_queue
Revises: 0005_submission_judge_detail
Create Date: 2026-05-12
"""
from alembic import op
import sqlalchemy as sa


revision = "0006_bundle_warm_queue"
down_revision = "0005_submission_judge_detail"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "bundle_warm_queue",
        sa.Column("bundle_warm_queue_id", sa.String(length=36), nullable=False),
        sa.Column("contest_id", sa.String(length=36), nullable=False),
        sa.Column("problem_id", sa.String(length=36), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("bundle_warm_queue_id"),
    )
    op.create_index(op.f("ix_bundle_warm_queue_contest_id"), "bundle_warm_queue", ["contest_id"], unique=False)
    op.create_index(op.f("ix_bundle_warm_queue_problem_id"), "bundle_warm_queue", ["problem_id"], unique=False)
    op.create_index(op.f("ix_bundle_warm_queue_status"), "bundle_warm_queue", ["status"], unique=False)
    op.create_index(op.f("ix_bundle_warm_queue_created_at"), "bundle_warm_queue", ["created_at"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_bundle_warm_queue_created_at"), table_name="bundle_warm_queue")
    op.drop_index(op.f("ix_bundle_warm_queue_status"), table_name="bundle_warm_queue")
    op.drop_index(op.f("ix_bundle_warm_queue_problem_id"), table_name="bundle_warm_queue")
    op.drop_index(op.f("ix_bundle_warm_queue_contest_id"), table_name="bundle_warm_queue")
    op.drop_table("bundle_warm_queue")
