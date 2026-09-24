"""Indexes for bounded log retention and chronological log pages."""
from alembic import op
import sqlalchemy as sa

revision = "0034_log_retention_indexes"
down_revision = "0033_mail_delivery_logs"
branch_labels = None
depends_on = None

INDEXES = [
    ("idx_judge_logs_created_id", "judge_agent_logs", "judge_agent_log_id"),
    ("idx_audit_logs_created_id", "operational_audit_logs", "operational_audit_log_id"),
    ("idx_access_logs_created_id", "access_logs", "access_log_id"),
]


def upgrade():
    for name, table, identity in INDEXES:
        if op.get_bind().dialect.name == "postgresql":
            valid = op.get_bind().execute(sa.text(
                "SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass(:name)"
            ), {"name": name}).scalar_one_or_none()
            if valid is True:
                continue
            with op.get_context().autocommit_block():
                # A cancelled concurrent build can leave an invalid index behind.
                if valid is False:
                    op.drop_index(name, table_name=table, postgresql_concurrently=True)
                op.create_index(name, table, ["created_at", identity], postgresql_concurrently=True)
        else:
            if name not in {index["name"] for index in sa.inspect(op.get_bind()).get_indexes(table)}:
                op.create_index(name, table, ["created_at", identity])


def downgrade():
    for name, table, _ in INDEXES:
        op.drop_index(name, table_name=table)
