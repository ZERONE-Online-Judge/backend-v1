"""Store standalone Editor verification without contest submissions."""
from alembic import op
import sqlalchemy as sa

revision = "0047_editor_judge_runs"
down_revision = "0046_account_passwords"
branch_labels = None
depends_on = None


def upgrade():
    # Development/test bootstraps can already create the current ORM table.
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if "editor_judge_runs" in inspector.get_table_names():
        columns = {c["name"] for c in inspector.get_columns("editor_judge_runs")}
        if not {"run_id", "dataset_key", "payload", "storage_keys", "lease_token"} <= columns:
            raise RuntimeError("The existing Editor judge table has an incompatible schema")
        return
    op.create_table(
        "editor_judge_runs",
        sa.Column("run_id", sa.String(36), primary_key=True),
        sa.Column("operation_key", sa.String(64), nullable=False, unique=True),
        sa.Column("owner_id", sa.String(36), nullable=False),
        sa.Column("dataset_key", sa.String(64), nullable=False),
        sa.Column("project_id", sa.String(32), nullable=False),
        sa.Column("fingerprint", sa.String(64), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("storage_keys", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("testcase_count", sa.Integer(), nullable=False),
        sa.Column("progress_current", sa.Integer(), nullable=False),
        sa.Column("assigned_node_id", sa.String(36), nullable=True),
        sa.Column("node_name", sa.String(120), nullable=True),
        sa.Column("agent_version", sa.String(64), nullable=True),
        sa.Column("lease_token", sa.String(36), nullable=True),
        sa.Column("leased_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("result", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
    )
    for column in ("owner_id", "dataset_key", "status", "created_at"):
        op.create_index("ix_editor_judge_runs_" + column, "editor_judge_runs", [column])


def downgrade():
    op.drop_table("editor_judge_runs")
