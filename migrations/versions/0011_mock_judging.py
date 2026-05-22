"""mock judging option

Revision ID: 0011_mock_judging
Revises: 0010_mail_html_body
Create Date: 2026-05-22 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "0011_mock_judging"
down_revision = "0010_mail_html_body"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "contests",
        sa.Column("mock_judging_enabled", sa.Boolean(), nullable=False, server_default=sa.false()),
    )


def downgrade() -> None:
    op.drop_column("contests", "mock_judging_enabled")
