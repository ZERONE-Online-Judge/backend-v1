"""Isolated presentation credentials and shared login throttling."""
from alembic import op
import sqlalchemy as sa

revision = "0030_presentation_accounts"
down_revision = "0029_contest_owner"
branch_labels = None
depends_on = None


def upgrade() -> None:
    tables = set(sa.inspect(op.get_bind()).get_table_names())
    if "presentation_accounts" not in tables:
        op.create_table("presentation_accounts",
            sa.Column("contest_id", sa.String(36), sa.ForeignKey("contests.contest_id", ondelete="CASCADE"), primary_key=True),
            sa.Column("credential_id", sa.String(36), nullable=False),
            sa.Column("email", sa.String(64), nullable=False, unique=True),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("session_token_hash", sa.String(64), nullable=True))
    if "presentation_login_limits" not in tables:
        op.create_table("presentation_login_limits",
            sa.Column("client_key", sa.String(64), primary_key=True),
            sa.Column("window_start", sa.DateTime(timezone=True), nullable=False),
            sa.Column("attempts", sa.Integer(), nullable=False))
        op.create_index("ix_presentation_login_limits_window_start", "presentation_login_limits", ["window_start"])


def downgrade() -> None:
    op.drop_table("presentation_login_limits")
    op.drop_table("presentation_accounts")
