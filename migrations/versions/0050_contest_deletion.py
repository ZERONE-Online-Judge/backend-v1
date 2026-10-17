"""Retain deleted contest data while removing service access."""
from alembic import op
import sqlalchemy as sa

revision = "0050_contest_deletion"
down_revision = "0049_editor_execution_tasks"
branch_labels = depends_on = None


def upgrade():
    if "deleted_at" not in {column["name"] for column in sa.inspect(op.get_bind()).get_columns("contests")}:
        op.add_column("contests", sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True))


def downgrade():
    op.drop_column("contests", "deleted_at")
