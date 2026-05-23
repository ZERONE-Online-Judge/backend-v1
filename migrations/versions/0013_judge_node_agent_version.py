"""judge node agent version

Revision ID: 0013_judge_node_agent_version
Revises: 0012_contact_inquiries
Create Date: 2026-05-23 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "0013_judge_node_agent_version"
down_revision = "0012_contact_inquiries"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "judge_nodes",
        sa.Column("agent_version", sa.String(length=64), nullable=False, server_default="unknown"),
    )


def downgrade() -> None:
    op.drop_column("judge_nodes", "agent_version")
