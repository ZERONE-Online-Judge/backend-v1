"""add problem language resource limits

Revision ID: 0020_language_limits
Revises: 0019_team_session_revoke
Create Date: 2026-05-28 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "0020_language_limits"
down_revision = "0019_team_session_revoke"
branch_labels = None
depends_on = None


def _has_column(table_name: str, column_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if table_name not in inspector.get_table_names():
        return False
    return column_name in {column["name"] for column in inspector.get_columns(table_name)}


def upgrade() -> None:
    if not _has_column("problems", "language_resource_limits"):
        op.add_column(
            "problems",
            sa.Column("language_resource_limits", sa.JSON(), nullable=False, server_default=sa.text("'{}'")),
        )


def downgrade() -> None:
    if _has_column("problems", "language_resource_limits"):
        op.drop_column("problems", "language_resource_limits")
