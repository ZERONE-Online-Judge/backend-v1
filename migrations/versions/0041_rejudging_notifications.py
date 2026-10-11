"""Persist participant verdict-change notifications with the applied result."""
from alembic import op
import sqlalchemy as sa

revision = "0041_rejudging_notifications"
down_revision = "0040_staged_rejudging"
branch_labels = None
depends_on = None


def upgrade():
    op.create_index("ix_team_members_email_lower", "team_members", [sa.text("lower(email)")], if_not_exists=True)
    op.create_table(
        "rejudging_notifications",
        sa.Column("notification_id", sa.String(36), primary_key=True),
        sa.Column("rejudging_item_id", sa.String(36), sa.ForeignKey("rejudging_items.rejudging_item_id", ondelete="CASCADE"), nullable=False),
        sa.Column("team_member_id", sa.String(36), sa.ForeignKey("team_members.team_member_id", ondelete="CASCADE"), nullable=False),
        sa.Column("contest_id", sa.String(36), sa.ForeignKey("contests.contest_id", ondelete="CASCADE"), nullable=False),
        sa.Column("submission_id", sa.String(36), sa.ForeignKey("submissions.submission_id", ondelete="CASCADE"), nullable=False),
        sa.Column("problem_id", sa.String(36), sa.ForeignKey("problems.problem_id", ondelete="CASCADE"), nullable=False),
        sa.Column("contest_title", sa.String(255), nullable=False),
        sa.Column("problem_code", sa.String(16), nullable=False),
        sa.Column("old_status", sa.String(32), nullable=False),
        sa.Column("new_status", sa.String(32), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("read_at", sa.DateTime(timezone=True)),
        sa.Column("deleted_at", sa.DateTime(timezone=True)),
        sa.UniqueConstraint("rejudging_item_id", "team_member_id", name="uq_rejudge_notice_recipient"),
    )
    op.create_index("ix_rejudge_notice_feed", "rejudging_notifications", ["team_member_id", "deleted_at", "created_at"])


def downgrade():
    op.drop_table("rejudging_notifications")
    op.drop_index("ix_team_members_email_lower", table_name="team_members", if_exists=True)
