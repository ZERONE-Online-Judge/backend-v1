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


def upgrade() -> None:
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
