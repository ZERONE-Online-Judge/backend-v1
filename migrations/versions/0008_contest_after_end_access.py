"""contest after-end resource access

Revision ID: 0008_contest_after_end_access
Revises: 0007_submission_runtime_metrics
Create Date: 2026-05-19 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "0008_contest_after_end_access"
down_revision = "0007_submission_runtime_metrics"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("contests", sa.Column("problem_access_after_end", sa.String(length=32), nullable=False, server_default="private"))
    op.add_column("contests", sa.Column("scoreboard_access_after_end", sa.String(length=32), nullable=False, server_default="private"))
    op.add_column("contests", sa.Column("submission_access_after_end", sa.String(length=32), nullable=False, server_default="private"))
    op.add_column("contests", sa.Column("board_access_after_end", sa.String(length=32), nullable=False, server_default="participants"))
    op.add_column("contests", sa.Column("notice_access_after_end", sa.String(length=32), nullable=False, server_default="public"))
    op.execute("UPDATE contests SET problem_access_after_end = CASE WHEN problem_public_after_end THEN 'public' ELSE 'private' END")
    op.execute("UPDATE contests SET scoreboard_access_after_end = CASE WHEN scoreboard_public_after_end THEN 'public' ELSE 'private' END")
    op.execute("UPDATE contests SET submission_access_after_end = CASE WHEN submission_public_after_end THEN 'public' ELSE 'private' END")


def downgrade() -> None:
    op.drop_column("contests", "notice_access_after_end")
    op.drop_column("contests", "board_access_after_end")
    op.drop_column("contests", "submission_access_after_end")
    op.drop_column("contests", "scoreboard_access_after_end")
    op.drop_column("contests", "problem_access_after_end")
