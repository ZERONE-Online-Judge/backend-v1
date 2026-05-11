"""initial core schema

Revision ID: 0001_initial_core
Revises:
Create Date: 2026-05-03
"""
from alembic import op
import sqlalchemy as sa


revision = "0001_initial_core"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "contests",
        sa.Column("contest_id", sa.String(length=36), primary_key=True),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.Column("organization_name", sa.String(length=255), nullable=False),
        sa.Column("overview", sa.Text(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("start_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("end_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("freeze_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("problem_public_after_end", sa.Boolean(), nullable=False),
        sa.Column("scoreboard_public_after_end", sa.Boolean(), nullable=False),
        sa.Column("submission_public_after_end", sa.Boolean(), nullable=False),
        sa.Column("emergency_notice", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_contests_status", "contests", ["status"])

    op.create_table(
        "contest_divisions",
        sa.Column("division_id", sa.String(length=36), primary_key=True),
        sa.Column("contest_id", sa.String(length=36), sa.ForeignKey("contests.contest_id"), nullable=False),
        sa.Column("code", sa.String(length=64), nullable=False),
        sa.Column("name", sa.String(length=120), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("display_order", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("contest_id", "code", name="uq_contest_division_code"),
    )
    op.create_index("ix_contest_divisions_contest_id", "contest_divisions", ["contest_id"])

    op.create_table(
        "participant_teams",
        sa.Column("participant_team_id", sa.String(length=36), primary_key=True),
        sa.Column("contest_id", sa.String(length=36), sa.ForeignKey("contests.contest_id"), nullable=False),
        sa.Column("division_id", sa.String(length=36), sa.ForeignKey("contest_divisions.division_id"), nullable=False),
        sa.Column("team_name", sa.String(length=255), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("contest_id", "team_name", name="uq_participant_team_contest_name"),
    )
    op.create_index("ix_participant_teams_contest_id", "participant_teams", ["contest_id"])
    op.create_index("ix_participant_teams_division_id", "participant_teams", ["division_id"])

    op.create_table(
        "team_members",
        sa.Column("team_member_id", sa.String(length=36), primary_key=True),
        sa.Column("contest_id", sa.String(length=36), sa.ForeignKey("contests.contest_id"), nullable=False),
        sa.Column("participant_team_id", sa.String(length=36), sa.ForeignKey("participant_teams.participant_team_id"), nullable=False),
        sa.Column("role", sa.String(length=32), nullable=False),
        sa.Column("name", sa.String(length=120), nullable=False),
        sa.Column("email", sa.String(length=255), nullable=False),
        sa.Column("active_sessions", sa.Integer(), nullable=False),
        sa.Column("last_login_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("contest_id", "email", name="uq_team_member_contest_email"),
    )
    op.create_index("ix_team_members_contest_id", "team_members", ["contest_id"])
    op.create_index("ix_team_members_email", "team_members", ["email"])
    op.create_index("ix_team_members_participant_team_id", "team_members", ["participant_team_id"])

    op.create_table(
        "problems",
        sa.Column("problem_id", sa.String(length=36), primary_key=True),
        sa.Column("contest_id", sa.String(length=36), sa.ForeignKey("contests.contest_id"), nullable=False),
        sa.Column("division_id", sa.String(length=36), sa.ForeignKey("contest_divisions.division_id"), nullable=False),
        sa.Column("problem_code", sa.String(length=16), nullable=False),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.Column("statement", sa.Text(), nullable=False),
        sa.Column("time_limit_ms", sa.Integer(), nullable=False),
        sa.Column("memory_limit_mb", sa.Integer(), nullable=False),
        sa.Column("display_order", sa.Integer(), nullable=False),
        sa.Column("max_score", sa.Integer(), nullable=False),
        sa.UniqueConstraint("contest_id", "division_id", "problem_code", name="uq_problem_division_code"),
    )
    op.create_index("ix_problems_contest_id", "problems", ["contest_id"])
    op.create_index("ix_problems_division_id", "problems", ["division_id"])

    op.create_table(
        "problem_assets",
        sa.Column("asset_id", sa.String(length=36), primary_key=True),
        sa.Column("contest_id", sa.String(length=36), sa.ForeignKey("contests.contest_id"), nullable=False),
        sa.Column("problem_id", sa.String(length=36), sa.ForeignKey("problems.problem_id"), nullable=False),
        sa.Column("original_filename", sa.String(length=255), nullable=False),
        sa.Column("storage_key", sa.String(length=512), nullable=False),
        sa.Column("mime_type", sa.String(length=120), nullable=False),
        sa.Column("file_size", sa.Integer(), nullable=False),
        sa.Column("sha256", sa.String(length=64), nullable=False),
        sa.Column("asset_status", sa.String(length=32), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_problem_assets_contest_id", "problem_assets", ["contest_id"])
    op.create_index("ix_problem_assets_problem_id", "problem_assets", ["problem_id"])
    op.create_index("ix_problem_assets_storage_key", "problem_assets", ["storage_key"], unique=True)

    op.create_table(
        "testcase_sets",
        sa.Column("testcase_set_id", sa.String(length=36), primary_key=True),
        sa.Column("problem_id", sa.String(length=36), sa.ForeignKey("problems.problem_id"), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_testcase_sets_problem_id", "testcase_sets", ["problem_id"])

    op.create_table(
        "testcases",
        sa.Column("testcase_id", sa.String(length=36), primary_key=True),
        sa.Column("testcase_set_id", sa.String(length=36), sa.ForeignKey("testcase_sets.testcase_set_id"), nullable=False),
        sa.Column("display_order", sa.Integer(), nullable=False),
        sa.Column("input_storage_key", sa.String(length=512), nullable=False),
        sa.Column("output_storage_key", sa.String(length=512), nullable=False),
        sa.Column("input_sha256", sa.String(length=64), nullable=False),
        sa.Column("output_sha256", sa.String(length=64), nullable=False),
        sa.Column("time_limit_ms_override", sa.Integer(), nullable=True),
        sa.Column("memory_limit_mb_override", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_testcases_testcase_set_id", "testcases", ["testcase_set_id"])

    op.create_table(
        "submissions",
        sa.Column("submission_id", sa.String(length=36), primary_key=True),
        sa.Column("contest_id", sa.String(length=36), sa.ForeignKey("contests.contest_id"), nullable=False),
        sa.Column("division_id", sa.String(length=36), sa.ForeignKey("contest_divisions.division_id"), nullable=False),
        sa.Column("problem_id", sa.String(length=36), sa.ForeignKey("problems.problem_id"), nullable=False),
        sa.Column("participant_team_id", sa.String(length=36), sa.ForeignKey("participant_teams.participant_team_id"), nullable=False),
        sa.Column("team_member_id", sa.String(length=36), sa.ForeignKey("team_members.team_member_id"), nullable=False),
        sa.Column("language", sa.String(length=32), nullable=False),
        sa.Column("source_code", sa.Text(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("submitted_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status_updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("awarded_score", sa.Integer(), nullable=True),
        sa.Column("compile_message", sa.Text(), nullable=True),
    )
    op.create_index("ix_submissions_contest_id", "submissions", ["contest_id"])
    op.create_index("ix_submissions_division_id", "submissions", ["division_id"])
    op.create_index("ix_submissions_participant_team_id", "submissions", ["participant_team_id"])
    op.create_index("ix_submissions_problem_id", "submissions", ["problem_id"])

    op.create_table(
        "judge_jobs",
        sa.Column("judge_job_id", sa.String(length=36), primary_key=True),
        sa.Column("submission_id", sa.String(length=36), sa.ForeignKey("submissions.submission_id"), nullable=False),
        sa.Column("contest_id", sa.String(length=36), sa.ForeignKey("contests.contest_id"), nullable=False),
        sa.Column("division_id", sa.String(length=36), sa.ForeignKey("contest_divisions.division_id"), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("queue_position", sa.Integer(), nullable=False),
        sa.Column("assigned_node_id", sa.String(length=36), nullable=True),
        sa.Column("lease_token", sa.String(length=64), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_judge_jobs_contest_id", "judge_jobs", ["contest_id"])
    op.create_index("ix_judge_jobs_division_id", "judge_jobs", ["division_id"])
    op.create_index("ix_judge_jobs_queue_position", "judge_jobs", ["queue_position"])
    op.create_index("ix_judge_jobs_status", "judge_jobs", ["status"])
    op.create_index("ix_judge_jobs_submission_id", "judge_jobs", ["submission_id"])

    op.create_table(
        "service_notices",
        sa.Column("service_notice_id", sa.String(length=36), primary_key=True),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.Column("summary", sa.Text(), nullable=False),
        sa.Column("body", sa.Text(), nullable=False),
        sa.Column("emergency", sa.Boolean(), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=False),
    )

    op.create_table(
        "staff_accounts",
        sa.Column("staff_account_id", sa.String(length=36), primary_key=True),
        sa.Column("email", sa.String(length=255), nullable=False),
        sa.Column("display_name", sa.String(length=120), nullable=False),
        sa.Column("is_service_master", sa.Boolean(), nullable=False),
        sa.Column("password_hash", sa.Text(), nullable=False),
        sa.Column("permissions", sa.Text(), nullable=False),
        sa.Column("contest_scopes", sa.Text(), nullable=False),
    )
    op.create_index("ix_staff_accounts_email", "staff_accounts", ["email"], unique=True)

    op.create_table(
        "staff_sessions",
        sa.Column("staff_session_id", sa.String(length=36), primary_key=True),
        sa.Column("staff_account_id", sa.String(length=36), sa.ForeignKey("staff_accounts.staff_account_id"), nullable=False),
        sa.Column("access_token_hash", sa.String(length=64), nullable=False),
        sa.Column("refresh_token_hash", sa.String(length=64), nullable=False),
        sa.Column("issued_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("access_expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("refresh_expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_staff_sessions_staff_account_id", "staff_sessions", ["staff_account_id"])
    op.create_index("ix_staff_sessions_access_token_hash", "staff_sessions", ["access_token_hash"], unique=True)
    op.create_index("ix_staff_sessions_refresh_token_hash", "staff_sessions", ["refresh_token_hash"], unique=True)

    op.create_table(
        "general_sessions",
        sa.Column("general_session_id", sa.String(length=36), primary_key=True),
        sa.Column("email", sa.String(length=255), nullable=False),
        sa.Column("access_token_hash", sa.String(length=64), nullable=False),
        sa.Column("refresh_token_hash", sa.String(length=64), nullable=False),
        sa.Column("issued_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("access_expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("refresh_expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_general_sessions_email", "general_sessions", ["email"])
    op.create_index("ix_general_sessions_access_token_hash", "general_sessions", ["access_token_hash"], unique=True)
    op.create_index("ix_general_sessions_refresh_token_hash", "general_sessions", ["refresh_token_hash"], unique=True)

    op.create_table(
        "mail_queue",
        sa.Column("mail_queue_id", sa.String(length=36), primary_key=True),
        sa.Column("mail_type", sa.String(length=64), nullable=False),
        sa.Column("recipient_email", sa.String(length=255), nullable=False),
        sa.Column("subject", sa.String(length=255), nullable=False),
        sa.Column("body_text", sa.Text(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_mail_queue_recipient_email", "mail_queue", ["recipient_email"])
    op.create_index("ix_mail_queue_status", "mail_queue", ["status"])

    op.create_table(
        "judge_nodes",
        sa.Column("judge_node_id", sa.String(length=36), primary_key=True),
        sa.Column("node_name", sa.String(length=120), nullable=False),
        sa.Column("node_secret_hash", sa.Text(), nullable=False),
        sa.Column("total_slots", sa.Integer(), nullable=False),
        sa.Column("free_slots", sa.Integer(), nullable=False),
        sa.Column("running_job_count", sa.Integer(), nullable=False),
        sa.Column("last_heartbeat_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("schedulable", sa.Boolean(), nullable=False),
        sa.UniqueConstraint("node_name", name="uq_judge_nodes_node_name"),
    )

    op.create_table(
        "otp_codes",
        sa.Column("email", sa.String(length=255), primary_key=True),
        sa.Column("contest_id", sa.String(length=36), nullable=False),
        sa.Column("code", sa.String(length=16), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("verified_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_otp_codes_contest_id", "otp_codes", ["contest_id"])

    op.create_table(
        "team_sessions",
        sa.Column("team_session_id", sa.String(length=36), primary_key=True),
        sa.Column("contest_id", sa.String(length=36), sa.ForeignKey("contests.contest_id"), nullable=False),
        sa.Column("division_id", sa.String(length=36), sa.ForeignKey("contest_divisions.division_id"), nullable=False),
        sa.Column("participant_team_id", sa.String(length=36), sa.ForeignKey("participant_teams.participant_team_id"), nullable=False),
        sa.Column("team_member_id", sa.String(length=36), sa.ForeignKey("team_members.team_member_id"), nullable=False),
        sa.Column("access_token_hash", sa.String(length=64), nullable=False),
        sa.Column("issued_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_team_sessions_contest_id", "team_sessions", ["contest_id"])
    op.create_index("ix_team_sessions_division_id", "team_sessions", ["division_id"])
    op.create_index("ix_team_sessions_participant_team_id", "team_sessions", ["participant_team_id"])
    op.create_index("ix_team_sessions_team_member_id", "team_sessions", ["team_member_id"])
    op.create_index("ix_team_sessions_access_token_hash", "team_sessions", ["access_token_hash"], unique=True)


def downgrade() -> None:
    op.drop_table("team_sessions")
    op.drop_table("otp_codes")
    op.drop_table("judge_nodes")
    op.drop_table("mail_queue")
    op.drop_table("general_sessions")
    op.drop_table("staff_sessions")
    op.drop_table("staff_accounts")
    op.drop_table("service_notices")
    op.drop_table("judge_jobs")
    op.drop_table("submissions")
    op.drop_table("testcases")
    op.drop_table("testcase_sets")
    op.drop_table("problem_assets")
    op.drop_table("problems")
    op.drop_table("team_members")
    op.drop_table("participant_teams")
    op.drop_table("contest_divisions")
    op.drop_table("contests")
