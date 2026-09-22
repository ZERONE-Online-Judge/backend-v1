"""First-party page usage events for service administrators."""
from alembic import op
import sqlalchemy as sa

revision = "0028_usage_analytics"
down_revision = "0027_scoreboard_release_modes"
branch_labels = None
depends_on = None


def upgrade() -> None:
    if "usage_events" in sa.inspect(op.get_bind()).get_table_names():
        return
    op.create_table(
        "usage_events",
        sa.Column("event_id", sa.String(64), primary_key=True),
        sa.Column("visitor_key", sa.String(64), nullable=False),
        sa.Column("visit_key", sa.String(64), nullable=False),
        sa.Column("account_key", sa.String(64), nullable=True),
        sa.Column("contest_id", sa.String(36), nullable=True),
        sa.Column("service", sa.String(24), nullable=False),
        sa.Column("page_key", sa.String(64), nullable=False),
        sa.Column("audience", sa.String(24), nullable=False),
        sa.Column("device", sa.String(16), nullable=False),
        sa.Column("browser", sa.String(24), nullable=False),
        sa.Column("referrer_host", sa.String(253), nullable=False),
        sa.Column("active_seconds", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("idx_usage_created", "usage_events", ["created_at"])
    op.create_index("idx_usage_contest_created", "usage_events", ["contest_id", "created_at"])
    op.create_index("idx_usage_visitor_created", "usage_events", ["visitor_key", "created_at"])
    op.create_index("idx_usage_seen", "usage_events", ["last_seen_at"])


def downgrade() -> None:
    if "usage_events" in sa.inspect(op.get_bind()).get_table_names():
        op.drop_table("usage_events")
