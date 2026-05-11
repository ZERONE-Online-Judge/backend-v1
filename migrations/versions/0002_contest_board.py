"""contest notice and board

Revision ID: 0002_contest_board
Revises: 0001_initial_core
Create Date: 2026-05-09
"""
from alembic import op
import sqlalchemy as sa


revision = "0002_contest_board"
down_revision = "0001_initial_core"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "contest_notices",
        sa.Column("contest_notice_id", sa.String(length=36), primary_key=True),
        sa.Column("contest_id", sa.String(length=36), sa.ForeignKey("contests.contest_id"), nullable=False),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.Column("body", sa.Text(), nullable=False),
        sa.Column("pinned", sa.Boolean(), nullable=False),
        sa.Column("emergency", sa.Boolean(), nullable=False),
        sa.Column("visibility", sa.String(length=32), nullable=False),
        sa.Column("created_by_email", sa.String(length=255), nullable=True),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_contest_notices_contest_id", "contest_notices", ["contest_id"])
    op.create_index("ix_contest_notices_visibility", "contest_notices", ["visibility"])

    op.create_table(
        "contest_questions",
        sa.Column("contest_question_id", sa.String(length=36), primary_key=True),
        sa.Column("contest_id", sa.String(length=36), sa.ForeignKey("contests.contest_id"), nullable=False),
        sa.Column("participant_team_id", sa.String(length=36), sa.ForeignKey("participant_teams.participant_team_id"), nullable=False),
        sa.Column("team_member_id", sa.String(length=36), sa.ForeignKey("team_members.team_member_id"), nullable=False),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.Column("body", sa.Text(), nullable=False),
        sa.Column("visibility", sa.String(length=32), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_contest_questions_contest_id", "contest_questions", ["contest_id"])
    op.create_index("ix_contest_questions_participant_team_id", "contest_questions", ["participant_team_id"])
    op.create_index("ix_contest_questions_team_member_id", "contest_questions", ["team_member_id"])
    op.create_index("ix_contest_questions_visibility", "contest_questions", ["visibility"])

    op.create_table(
        "contest_question_answers",
        sa.Column("contest_answer_id", sa.String(length=36), primary_key=True),
        sa.Column("contest_question_id", sa.String(length=36), sa.ForeignKey("contest_questions.contest_question_id"), nullable=False),
        sa.Column("contest_id", sa.String(length=36), sa.ForeignKey("contests.contest_id"), nullable=False),
        sa.Column("body", sa.Text(), nullable=False),
        sa.Column("visibility", sa.String(length=32), nullable=False),
        sa.Column("created_by_email", sa.String(length=255), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_contest_question_answers_contest_id", "contest_question_answers", ["contest_id"])
    op.create_index("ix_contest_question_answers_contest_question_id", "contest_question_answers", ["contest_question_id"])
    op.create_index("ix_contest_question_answers_visibility", "contest_question_answers", ["visibility"])


def downgrade() -> None:
    op.drop_index("ix_contest_question_answers_visibility", table_name="contest_question_answers")
    op.drop_index("ix_contest_question_answers_contest_question_id", table_name="contest_question_answers")
    op.drop_index("ix_contest_question_answers_contest_id", table_name="contest_question_answers")
    op.drop_table("contest_question_answers")
    op.drop_index("ix_contest_questions_visibility", table_name="contest_questions")
    op.drop_index("ix_contest_questions_team_member_id", table_name="contest_questions")
    op.drop_index("ix_contest_questions_participant_team_id", table_name="contest_questions")
    op.drop_index("ix_contest_questions_contest_id", table_name="contest_questions")
    op.drop_table("contest_questions")
    op.drop_index("ix_contest_notices_visibility", table_name="contest_notices")
    op.drop_index("ix_contest_notices_contest_id", table_name="contest_notices")
    op.drop_table("contest_notices")
