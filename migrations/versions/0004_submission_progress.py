"""submission judge progress fields

Revision ID: 0004_submission_progress
Revises: 0003_judge_dispatcher
Create Date: 2026-05-09
"""
from alembic import op
import sqlalchemy as sa


revision = "0004_submission_progress"
down_revision = "0003_judge_dispatcher"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("submissions", sa.Column("progress_current", sa.Integer(), nullable=True))
    op.add_column("submissions", sa.Column("progress_total", sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column("submissions", "progress_total")
    op.drop_column("submissions", "progress_current")
