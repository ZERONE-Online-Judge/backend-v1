"""Add opt-in password credentials and single-use email challenges."""

from alembic import op
import sqlalchemy as sa

revision = "0046_account_passwords"
down_revision = "0045_auth_rate_limits"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "identity_accounts",
        sa.Column("account_id", sa.String(36), primary_key=True),
        sa.Column("email", sa.String(254), nullable=False),
        sa.Column("display_name", sa.String(120), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ix_identity_accounts_email", "identity_accounts", ["email"], unique=True
    )
    op.create_table(
        "account_passwords",
        sa.Column("email", sa.String(254), primary_key=True),
        sa.Column("password_hash", sa.String(255), nullable=False),
        sa.Column("updated_at", sa.Integer(), nullable=False),
    )
    op.create_table(
        "password_challenges",
        sa.Column("email", sa.String(254), primary_key=True),
        sa.Column("purpose", sa.String(16), primary_key=True),
        sa.Column("code_digest", sa.String(64), nullable=False),
        sa.Column("expected_password_hash", sa.String(255), nullable=False),
        sa.Column("requested_at", sa.Integer(), nullable=False),
        sa.Column("expires_at", sa.Integer(), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
    )
    op.create_index(
        "ix_password_challenges_expires_at", "password_challenges", ["expires_at"]
    )


def downgrade():
    op.drop_table("password_challenges")
    op.drop_table("account_passwords")
    op.drop_table("identity_accounts")
