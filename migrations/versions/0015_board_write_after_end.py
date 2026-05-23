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


def upgrade() -> None:
    op.add_column(
        "contests",
        sa.Column("board_write_after_end", sa.Boolean(), nullable=False, server_default=sa.false()),
    )


def downgrade() -> None:
    op.drop_column("contests", "board_write_after_end")
