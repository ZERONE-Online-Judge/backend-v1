"""add team member session revoke marker

Revision ID: 0019_team_member_session_revoke_marker
Revises: 0018_remove_unused_columns
Create Date: 2026-05-27 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "0019_team_member_session_revoke_marker"
down_revision = "0018_remove_unused_columns"
branch_labels = None
depends_on = None


def _has_column(table_name: str, column_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if table_name not in inspector.get_table_names():
        return False
    return column_name in {column["name"] for column in inspector.get_columns(table_name)}


def upgrade() -> None:
    if not _has_column("team_members", "session_revoked_at"):
        op.add_column(
            "team_members",
            sa.Column("session_revoked_at", sa.DateTime(timezone=True), nullable=True),
        )


def downgrade() -> None:
    if _has_column("team_members", "session_revoked_at"):
        op.drop_column("team_members", "session_revoked_at")
