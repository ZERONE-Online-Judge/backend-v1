"""submission judge detail fields

Revision ID: 0005_submission_judge_detail
Revises: 0004_submission_progress
Create Date: 2026-05-09
"""
from alembic import op
import sqlalchemy as sa


revision = "0005_submission_judge_detail"
down_revision = "0004_submission_progress"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("submissions", sa.Column("judge_message", sa.Text(), nullable=True))
    op.add_column("submissions", sa.Column("failed_testcase_order", sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column("submissions", "failed_testcase_order")
    op.drop_column("submissions", "judge_message")
