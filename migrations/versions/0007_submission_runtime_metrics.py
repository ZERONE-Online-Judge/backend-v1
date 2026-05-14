"""submission runtime metrics

Revision ID: 0007_submission_runtime_metrics
Revises: 0006_bundle_warm_queue
Create Date: 2026-05-14
"""
from alembic import op
import sqlalchemy as sa


revision = "0007_submission_runtime_metrics"
down_revision = "0006_bundle_warm_queue"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("submissions", sa.Column("runtime_ms", sa.Integer(), nullable=True))
    op.add_column("submissions", sa.Column("memory_kb", sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column("submissions", "memory_kb")
    op.drop_column("submissions", "runtime_ms")
