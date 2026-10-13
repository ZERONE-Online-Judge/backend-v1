"""Route complete Editor execution tools to authorized judge nodes."""
from alembic import op
import sqlalchemy as sa
revision = "0049_editor_execution_tasks"
down_revision = "0048_problem_statement_languages"
branch_labels = depends_on = None

def upgrade():
    bind = op.get_bind()
    columns = {c["name"] for c in sa.inspect(bind).get_columns("judge_nodes")}
    for column in [
        sa.Column("allow_zoj", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("allow_editor", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("execution_capabilities", sa.JSON(), nullable=False, server_default="[]"),
        sa.Column("execution_revision", sa.String(128), nullable=True),
    ]:
        if column.name not in columns:
            op.add_column("judge_nodes", column)
    if "editor_execution_tasks" not in sa.inspect(bind).get_table_names():
        op.create_table("editor_execution_tasks",
            sa.Column("task_id", sa.String(36), primary_key=True),
            sa.Column("operation_key", sa.String(64), nullable=False, unique=True),
            sa.Column("owner_id", sa.String(36), nullable=False),
            sa.Column("fingerprint", sa.String(64), nullable=False),
            sa.Column("payload", sa.JSON(), nullable=False),
            sa.Column("input_key", sa.Text(), nullable=False),
            sa.Column("result_key", sa.Text()),
            sa.Column("status", sa.String(32), nullable=False),
            sa.Column("assigned_node_id", sa.String(36)),
            sa.Column("node_name", sa.String(120)),
            sa.Column("agent_version", sa.String(64)),
            sa.Column("lease_token", sa.String(36)),
            sa.Column("leased_at", sa.DateTime(timezone=True)),
            sa.Column("attempts", sa.Integer(), nullable=False),
            sa.Column("error", sa.Text()),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("completed_at", sa.DateTime(timezone=True)),
        )
        for name in ["owner_id", "status", "assigned_node_id", "created_at"]:
            op.create_index("ix_editor_execution_tasks_" + name, "editor_execution_tasks", [name])

def downgrade():
    op.drop_table("editor_execution_tasks")
    with op.batch_alter_table("judge_nodes") as batch:
        for name in ["execution_revision", "execution_capabilities", "allow_editor", "allow_zoj"]:
            batch.drop_column(name)
