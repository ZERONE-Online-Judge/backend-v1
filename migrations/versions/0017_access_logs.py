"""access logs

Revision ID: 0017_access_logs
Revises: 0016_audit_progress_visibility
Create Date: 2026-05-26 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "0017_access_logs"
down_revision = "0016_audit_progress_visibility"
branch_labels = None
depends_on = None


def _has_table(table_name: str) -> bool:
    bind = op.get_bind()
    return table_name in sa.inspect(bind).get_table_names()


def _has_index(table_name: str, index_name: str) -> bool:
    bind = op.get_bind()
    if not _has_table(table_name):
        return False
    return index_name in {index["name"] for index in sa.inspect(bind).get_indexes(table_name)}


def _create_index_if_missing(index_name: str, table_name: str, columns: list[str]) -> None:
    if not _has_index(table_name, index_name):
        op.create_index(index_name, table_name, columns)


def _drop_index_if_exists(index_name: str, table_name: str) -> None:
    if _has_index(table_name, index_name):
        op.drop_index(index_name, table_name=table_name)


def upgrade() -> None:
    if not _has_table("access_logs"):
        op.create_table(
            "access_logs",
            sa.Column("access_log_id", sa.String(length=36), primary_key=True),
            sa.Column("event_type", sa.String(length=64), nullable=False),
            sa.Column("account_scope", sa.String(length=32), nullable=False),
            sa.Column("email", sa.String(length=255), nullable=True),
            sa.Column("display_name", sa.String(length=120), nullable=True),
            sa.Column("contest_id", sa.String(length=36), nullable=True),
            sa.Column("contest_title", sa.String(length=255), nullable=True),
            sa.Column("participant_team_id", sa.String(length=36), nullable=True),
            sa.Column("team_name", sa.String(length=120), nullable=True),
            sa.Column("team_member_id", sa.String(length=36), nullable=True),
            sa.Column("member_name", sa.String(length=120), nullable=True),
            sa.Column("actor_role", sa.String(length=64), nullable=True),
            sa.Column("session_id", sa.String(length=36), nullable=True),
            sa.Column("client_ip", sa.String(length=128), nullable=True),
            sa.Column("user_agent", sa.Text(), nullable=True),
            sa.Column("request_id", sa.String(length=80), nullable=True),
            sa.Column("details", sa.Text(), nullable=False, server_default="{}"),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        )

    _create_index_if_missing("idx_access_logs_scope_created", "access_logs", ["account_scope", "created_at", "access_log_id"])
    _create_index_if_missing("idx_access_logs_contest_created", "access_logs", ["contest_id", "created_at", "access_log_id"])
    _create_index_if_missing("idx_access_logs_email_created", "access_logs", ["email", "created_at", "access_log_id"])
    _create_index_if_missing("idx_access_logs_event_created", "access_logs", ["event_type", "created_at", "access_log_id"])


def downgrade() -> None:
    _drop_index_if_exists("idx_access_logs_event_created", "access_logs")
    _drop_index_if_exists("idx_access_logs_email_created", "access_logs")
    _drop_index_if_exists("idx_access_logs_contest_created", "access_logs")
    _drop_index_if_exists("idx_access_logs_scope_created", "access_logs")
    if _has_table("access_logs"):
        op.drop_table("access_logs")
