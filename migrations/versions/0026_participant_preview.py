"""Isolate participant preview sessions and private preview discussion threads."""
from alembic import op
import sqlalchemy as sa

revision = "0026_participant_preview"
down_revision = "0025_separate_notice_roles"
branch_labels = None
depends_on = None


def upgrade() -> None:
    tables = set(sa.inspect(op.get_bind()).get_table_names())
    if "participant_preview_sessions" not in tables:
        op.create_table(
            "participant_preview_sessions",
            sa.Column("general_session_id", sa.String(36), sa.ForeignKey("general_sessions.general_session_id", ondelete="CASCADE"), primary_key=True),
            sa.Column("contest_id", sa.String(36), sa.ForeignKey("contests.contest_id", ondelete="CASCADE"), primary_key=True),
            sa.Column("staff_account_id", sa.String(36), sa.ForeignKey("staff_accounts.staff_account_id", ondelete="CASCADE"), nullable=False),
            sa.Column("division_id", sa.String(36), sa.ForeignKey("contest_divisions.division_id", ondelete="CASCADE"), nullable=False),
        )
        op.create_index("ix_participant_preview_sessions_staff_account_id", "participant_preview_sessions", ["staff_account_id"])
    if "participant_preview_questions" not in tables:
        op.create_table(
            "participant_preview_questions",
            sa.Column("staff_account_id", sa.String(36), sa.ForeignKey("staff_accounts.staff_account_id", ondelete="CASCADE"), primary_key=True),
            sa.Column("division_id", sa.String(36), sa.ForeignKey("contest_divisions.division_id", ondelete="CASCADE"), primary_key=True),
            sa.Column("question_id", sa.String(36), primary_key=True),
            sa.Column("contest_id", sa.String(36), sa.ForeignKey("contests.contest_id", ondelete="CASCADE"), nullable=False),
            sa.Column("source_question_id", sa.String(36), nullable=True),
            sa.Column("payload", sa.Text(), nullable=False),
        )
        op.create_index("ix_participant_preview_questions_contest_id", "participant_preview_questions", ["contest_id"])


def downgrade() -> None:
    op.drop_table("participant_preview_questions")
    op.drop_table("participant_preview_sessions")
