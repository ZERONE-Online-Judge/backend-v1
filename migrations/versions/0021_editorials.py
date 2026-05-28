"""add contest editorial access and problem editorials

Revision ID: 0021_editorials
Revises: 0020_language_limits
Create Date: 2026-05-28 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "0021_editorials"
down_revision = "0020_language_limits"
branch_labels = None
depends_on = None


def _has_column(table_name: str, column_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if table_name not in inspector.get_table_names():
        return False
    return column_name in {column["name"] for column in inspector.get_columns(table_name)}


def upgrade() -> None:
    if not _has_column("contests", "editorial_access_after_end"):
        op.add_column(
            "contests",
            sa.Column(
                "editorial_access_after_end",
                sa.String(length=32),
                nullable=False,
                server_default="private",
            ),
        )
    if not _has_column("problems", "editorial"):
        op.add_column(
            "problems",
            sa.Column("editorial", sa.Text(), nullable=False, server_default=""),
        )


def downgrade() -> None:
    if _has_column("problems", "editorial"):
        op.drop_column("problems", "editorial")
    if _has_column("contests", "editorial_access_after_end"):
        op.drop_column("contests", "editorial_access_after_end")
