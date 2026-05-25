"""audit logs and progress visibility

Revision ID: 0016_audit_and_progress_visibility
Revises: 0015_board_write_after_end
Create Date: 2026-05-25 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "0016_audit_and_progress_visibility"
down_revision = "0015_board_write_after_end"
branch_labels = None
depends_on = None


def _has_table(table_name: str) -> bool:
    bind = op.get_bind()
    return table_name in sa.inspect(bind).get_table_names()


def _has_column(table_name: str, column_name: str) -> bool:
    bind = op.get_bind()
    if not _has_table(table_name):
        return False
    return column_name in {column["name"] for column in sa.inspect(bind).get_columns(table_name)}


def _has_index(table_name: str, index_name: str) -> bool:
    bind = op.get_bind()
    if not _has_table(table_name):
        return False
    return index_name in {index["name"] for index in sa.inspect(bind).get_indexes(table_name)}


def _add_column_if_missing(table_name: str, column: sa.Column) -> None:
    if not _has_column(table_name, column.name):
        op.add_column(table_name, column)


def _create_index_if_missing(index_name: str, table_name: str, columns: list[str]) -> None:
    if not _has_index(table_name, index_name):
        op.create_index(index_name, table_name, columns)


def _drop_index_if_exists(index_name: str, table_name: str) -> None:
    if _has_index(table_name, index_name):
        op.drop_index(index_name, table_name=table_name)


def upgrade() -> None:
    _add_column_if_missing(
        "contests",
        sa.Column("participant_progress_visible", sa.Boolean(), nullable=False, server_default=sa.true()),
    )
    _add_column_if_missing(
        "contests",
        sa.Column("mock_judging_progress_visible", sa.Boolean(), nullable=False, server_default=sa.false()),
    )

    if not _has_table("judge_agent_logs"):
        op.create_table(
            "judge_agent_logs",
            sa.Column("judge_agent_log_id", sa.String(length=36), primary_key=True),
            sa.Column("judge_node_id", sa.String(length=36), nullable=False),
            sa.Column("node_name", sa.String(length=120), nullable=False),
            sa.Column("level", sa.String(length=16), nullable=False, server_default="info"),
            sa.Column("message", sa.Text(), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        )
    _create_index_if_missing(
        "idx_judge_agent_logs_node_created",
        "judge_agent_logs",
        ["judge_node_id", "created_at", "judge_agent_log_id"],
    )

    if not _has_table("operational_audit_logs"):
        op.create_table(
            "operational_audit_logs",
            sa.Column("operational_audit_log_id", sa.String(length=36), primary_key=True),
            sa.Column("scope", sa.String(length=32), nullable=False),
            sa.Column("action", sa.String(length=255), nullable=False),
            sa.Column("method", sa.String(length=12), nullable=False),
            sa.Column("path", sa.Text(), nullable=False),
            sa.Column("status_code", sa.Integer(), nullable=False),
            sa.Column("actor_email", sa.String(length=255), nullable=True),
            sa.Column("actor_name", sa.String(length=120), nullable=True),
            sa.Column("actor_role", sa.String(length=64), nullable=True),
            sa.Column("contest_id", sa.String(length=36), nullable=True),
            sa.Column("client_ip", sa.String(length=128), nullable=True),
            sa.Column("user_agent", sa.Text(), nullable=True),
            sa.Column("request_id", sa.String(length=80), nullable=True),
            sa.Column("details", sa.Text(), nullable=False, server_default="{}"),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        )

    _create_index_if_missing(
        "idx_operational_audit_scope_created",
        "operational_audit_logs",
        ["scope", "created_at", "operational_audit_log_id"],
    )
    _create_index_if_missing(
        "idx_operational_audit_contest_created",
        "operational_audit_logs",
        ["contest_id", "created_at", "operational_audit_log_id"],
    )
    _create_index_if_missing(
        "idx_operational_audit_actor_created",
        "operational_audit_logs",
        ["actor_email", "created_at", "operational_audit_log_id"],
    )


def downgrade() -> None:
    _drop_index_if_exists("idx_operational_audit_actor_created", "operational_audit_logs")
    _drop_index_if_exists("idx_operational_audit_contest_created", "operational_audit_logs")
    _drop_index_if_exists("idx_operational_audit_scope_created", "operational_audit_logs")
    if _has_table("operational_audit_logs"):
        op.drop_table("operational_audit_logs")
    _drop_index_if_exists("idx_judge_agent_logs_node_created", "judge_agent_logs")
    if _has_table("judge_agent_logs"):
        op.drop_table("judge_agent_logs")
    if _has_column("contests", "mock_judging_progress_visible"):
        op.drop_column("contests", "mock_judging_progress_visible")
    if _has_column("contests", "participant_progress_visible"):
        op.drop_column("contests", "participant_progress_visible")
