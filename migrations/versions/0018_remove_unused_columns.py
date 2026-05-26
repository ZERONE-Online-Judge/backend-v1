"""remove unused auth and scoring columns

Revision ID: 0018_remove_unused_columns
Revises: 0017_access_logs
Create Date: 2026-05-26 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "0018_remove_unused_columns"
down_revision = "0017_access_logs"
branch_labels = None
depends_on = None


def _has_column(table_name: str, column_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if table_name not in inspector.get_table_names():
        return False
    return column_name in {column["name"] for column in inspector.get_columns(table_name)}


def upgrade() -> None:
    if _has_column("staff_accounts", "password_hash"):
        op.drop_column("staff_accounts", "password_hash")
    if _has_column("problems", "max_score"):
        op.drop_column("problems", "max_score")
    if _has_column("submissions", "awarded_score"):
        op.drop_column("submissions", "awarded_score")
    for column_name in (
        "problem_public_after_end",
        "scoreboard_public_after_end",
        "submission_public_after_end",
    ):
        if _has_column("contests", column_name):
            op.drop_column("contests", column_name)


def downgrade() -> None:
    if not _has_column("staff_accounts", "password_hash"):
        op.add_column("staff_accounts", sa.Column("password_hash", sa.Text(), nullable=True))
    if not _has_column("problems", "max_score"):
        op.add_column("problems", sa.Column("max_score", sa.Integer(), nullable=True))
    if not _has_column("submissions", "awarded_score"):
        op.add_column("submissions", sa.Column("awarded_score", sa.Integer(), nullable=True))
    if not _has_column("contests", "problem_public_after_end"):
        op.add_column("contests", sa.Column("problem_public_after_end", sa.Boolean(), nullable=True))
    if not _has_column("contests", "scoreboard_public_after_end"):
        op.add_column("contests", sa.Column("scoreboard_public_after_end", sa.Boolean(), nullable=True))
    if not _has_column("contests", "submission_public_after_end"):
        op.add_column("contests", sa.Column("submission_public_after_end", sa.Boolean(), nullable=True))
