"""Separate the single transferable contest owner from ordinary masters."""
from alembic import op
import sqlalchemy as sa

from app.services.contest_ownership import backfill_contest_owners

revision = "0029_contest_owner"
down_revision = "0028_usage_analytics"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    columns = {column["name"] for column in sa.inspect(bind).get_columns("contests")}
    if "owner_staff_account_id" not in columns:
        with op.batch_alter_table("contests") as batch:
            batch.add_column(sa.Column("owner_staff_account_id", sa.String(36), nullable=True))
            batch.create_foreign_key("fk_contest_owner", "staff_accounts", ["owner_staff_account_id"], ["staff_account_id"])
    backfill_contest_owners(bind)


def downgrade() -> None:
    import json
    bind = op.get_bind()
    for row in bind.execute(sa.text("SELECT staff_account_id, contest_scopes, contest_roles FROM staff_accounts")).mappings():
        roles = json.loads(row["contest_roles"] or "{}")
        scopes = json.loads(row["contest_scopes"] or "{}")
        for cid, selected in roles.items():
            if selected == ["owner"]:
                roles[cid] = ["master"]
                scopes[cid] = ["contest.*"]
        bind.execute(sa.text("UPDATE staff_accounts SET contest_scopes=:scopes, contest_roles=:roles WHERE staff_account_id=:id"), {"scopes": json.dumps(scopes), "roles": json.dumps(roles), "id": row["staff_account_id"]})
    with op.batch_alter_table("contests") as batch:
        batch.drop_constraint("fk_contest_owner", type_="foreignkey")
        batch.drop_column("owner_staff_account_id")
