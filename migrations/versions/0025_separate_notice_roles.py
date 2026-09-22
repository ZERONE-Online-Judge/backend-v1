"""Split notice management from board management while preserving existing grants."""
from alembic import op
import sqlalchemy as sa

from app.database import backfill_separate_notice_roles

revision = "0025_separate_notice_roles"
down_revision = "0024_contest_roles"
branch_labels = None
depends_on = None


def upgrade() -> None:
    backfill_separate_notice_roles(op.get_bind())


def downgrade() -> None:
    # The previous release had no standalone notices/audit role. Preserve the
    # effective permission lists; its existing server-side guards still use them.
    # Empty role metadata lets role inference describe any remaining legacy scope.
    import json

    connection = op.get_bind()
    for row in connection.execute(sa.text("SELECT staff_account_id, contest_roles FROM staff_accounts")).mappings():
        roles = json.loads(row["contest_roles"] or "{}")
        changed = False
        for contest_id, selected in list(roles.items()):
            if any(role in {"notices_manager", "audit_viewer"} for role in selected):
                roles.pop(contest_id)
                changed = True
        if changed:
            connection.execute(sa.text("UPDATE staff_accounts SET contest_roles = :roles WHERE staff_account_id = :account_id"), {"roles": json.dumps(roles), "account_id": row["staff_account_id"]})
