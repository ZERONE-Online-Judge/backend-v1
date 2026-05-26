"""scoreboard freeze mode

Revision ID: 0009_scoreboard_freeze_mode
Revises: 0008_contest_after_end_access
Create Date: 2026-05-19 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "0009_scoreboard_freeze_mode"
down_revision = "0008_contest_after_end_access"
branch_labels = None
depends_on = None


def _has_column(table_name: str, column_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if table_name not in inspector.get_table_names():
        return False
    return column_name in {column["name"] for column in inspector.get_columns(table_name)}


def upgrade() -> None:
    if _has_column("contests", "problem_public_after_end"):
        op.execute("UPDATE contests SET problem_access_after_end = 'public' WHERE problem_public_after_end = TRUE")
    if _has_column("contests", "scoreboard_public_after_end"):
        op.execute("UPDATE contests SET scoreboard_access_after_end = 'public' WHERE scoreboard_public_after_end = TRUE")
    if _has_column("contests", "submission_public_after_end"):
        op.execute("UPDATE contests SET submission_access_after_end = 'public' WHERE submission_public_after_end = TRUE")
    op.add_column(
        "contests",
        sa.Column(
            "scoreboard_freeze_mode",
            sa.String(length=32),
            nullable=False,
            server_default="auto",
        ),
    )


def downgrade() -> None:
    op.drop_column("contests", "scoreboard_freeze_mode")
