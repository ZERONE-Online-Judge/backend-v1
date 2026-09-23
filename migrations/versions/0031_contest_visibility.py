"""Separate contest discoverability from its lifecycle and resource access."""
from alembic import op
import sqlalchemy as sa

revision = "0031_contest_visibility"
down_revision = "0030_presentation_accounts"
branch_labels = None
depends_on = None


def upgrade() -> None:
    columns = {column["name"] for column in sa.inspect(op.get_bind()).get_columns("contests")}
    for name in ("visibility", "visibility_after_end"):
        if name not in columns:
            op.add_column("contests", sa.Column(name, sa.String(16), nullable=False, server_default="public"))


def downgrade() -> None:
    op.drop_column("contests", "visibility_after_end")
    op.drop_column("contests", "visibility")
