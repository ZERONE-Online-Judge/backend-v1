"""mail html body

Revision ID: 0010_mail_html_body
Revises: 0009_scoreboard_freeze_mode
Create Date: 2026-05-19 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "0010_mail_html_body"
down_revision = "0009_scoreboard_freeze_mode"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("mail_queue", sa.Column("body_html", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("mail_queue", "body_html")
