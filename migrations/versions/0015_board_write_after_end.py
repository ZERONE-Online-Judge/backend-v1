"""board write after end

Revision ID: 0015_board_write_after_end
Revises: 0014_submission_list_indexes
Create Date: 2026-05-24 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "0015_board_write_after_end"
down_revision = "0014_submission_list_indexes"
branch_labels = None
depends_on = None


def _has_column(table_name: str, column_name: str) -> bool:
    bind = op.get_bind()
    return column_name in {column["name"] for column in sa.inspect(bind).get_columns(table_name)}


def upgrade() -> None:
    if not _has_column("contests", "board_write_after_end"):
        op.add_column(
            "contests",
            sa.Column("board_write_after_end", sa.Boolean(), nullable=False, server_default=sa.false()),
        )


def downgrade() -> None:
    if _has_column("contests", "board_write_after_end"):
        op.drop_column("contests", "board_write_after_end")
