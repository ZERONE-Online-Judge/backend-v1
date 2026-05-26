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


def _has_column(table_name: str, column_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if table_name not in inspector.get_table_names():
        return False
    return column_name in {column["name"] for column in inspector.get_columns(table_name)}


def upgrade() -> None:
    op.add_column("contests", sa.Column("problem_access_after_end", sa.String(length=32), nullable=False, server_default="private"))
    op.add_column("contests", sa.Column("scoreboard_access_after_end", sa.String(length=32), nullable=False, server_default="private"))
    op.add_column("contests", sa.Column("submission_access_after_end", sa.String(length=32), nullable=False, server_default="private"))
    op.add_column("contests", sa.Column("board_access_after_end", sa.String(length=32), nullable=False, server_default="participants"))
    op.add_column("contests", sa.Column("notice_access_after_end", sa.String(length=32), nullable=False, server_default="public"))
    if _has_column("contests", "problem_public_after_end"):
        op.execute("UPDATE contests SET problem_access_after_end = CASE WHEN problem_public_after_end THEN 'public' ELSE 'private' END")
    if _has_column("contests", "scoreboard_public_after_end"):
        op.execute("UPDATE contests SET scoreboard_access_after_end = CASE WHEN scoreboard_public_after_end THEN 'public' ELSE 'private' END")
    if _has_column("contests", "submission_public_after_end"):
        op.execute("UPDATE contests SET submission_access_after_end = CASE WHEN submission_public_after_end THEN 'public' ELSE 'private' END")


def downgrade() -> None:
    op.drop_column("contests", "notice_access_after_end")
    op.drop_column("contests", "board_access_after_end")
    op.drop_column("contests", "submission_access_after_end")
    op.drop_column("contests", "scoreboard_access_after_end")
    op.drop_column("contests", "problem_access_after_end")
