"""contact inquiries

Revision ID: 0012_contact_inquiries
Revises: 0011_mock_judging
Create Date: 2026-05-22 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "0012_contact_inquiries"
down_revision = "0011_mock_judging"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "contact_inquiries",
        sa.Column("contact_inquiry_id", sa.String(length=36), nullable=False),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.Column("sender_name", sa.String(length=120), nullable=False),
        sa.Column("sender_email", sa.String(length=255), nullable=False),
        sa.Column("body", sa.Text(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="pending"),
        sa.Column("answer_body", sa.Text(), nullable=True),
        sa.Column("answered_by_email", sa.String(length=255), nullable=True),
        sa.Column("answered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("contact_inquiry_id"),
    )
    op.create_index("ix_contact_inquiries_sender_email", "contact_inquiries", ["sender_email"])
    op.create_index("ix_contact_inquiries_status", "contact_inquiries", ["status"])
    op.create_index("ix_contact_inquiries_created_at", "contact_inquiries", ["created_at"])


def downgrade() -> None:
    op.drop_index("ix_contact_inquiries_created_at", table_name="contact_inquiries")
    op.drop_index("ix_contact_inquiries_status", table_name="contact_inquiries")
    op.drop_index("ix_contact_inquiries_sender_email", table_name="contact_inquiries")
    op.drop_table("contact_inquiries")
