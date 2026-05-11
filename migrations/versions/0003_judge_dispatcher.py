"""judge dispatcher lease metadata

Revision ID: 0003_judge_dispatcher
Revises: 0002_contest_board
Create Date: 2026-05-09
"""
from alembic import op
import sqlalchemy as sa


revision = "0003_judge_dispatcher"
down_revision = "0002_contest_board"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("judge_jobs", sa.Column("leased_at", sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    op.drop_column("judge_jobs", "leased_at")
