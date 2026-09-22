"""Persist contest role selections and protected administrator assignments."""
from alembic import op
import sqlalchemy as sa

from app.database import backfill_assigned_contest_masters

revision = "0024_contest_roles"
down_revision = "0023_scoreboard_releases"
branch_labels = None
depends_on = None


def upgrade() -> None:
    columns = {column["name"] for column in sa.inspect(op.get_bind()).get_columns("staff_accounts")}
    if "contest_roles" not in columns:
        op.add_column("staff_accounts", sa.Column("contest_roles", sa.Text(), nullable=False, server_default="{}"))
    if "protected_master_contests" not in columns:
        op.add_column("staff_accounts", sa.Column("protected_master_contests", sa.Text(), nullable=False, server_default="[]"))
    backfill_assigned_contest_masters(op.get_bind())


def downgrade() -> None:
    op.drop_column("staff_accounts", "protected_master_contests")
    op.drop_column("staff_accounts", "contest_roles")
