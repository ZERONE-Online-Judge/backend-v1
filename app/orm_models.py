from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base
from app.models import new_id, now_utc


class ContestRow(Base):
    __tablename__ = "contests"

    contest_id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    title: Mapped[str] = mapped_column(String(255))
    organization_name: Mapped[str] = mapped_column(String(255))
    overview: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(32), index=True)
    start_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    end_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    freeze_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    problem_public_after_end: Mapped[bool] = mapped_column(Boolean, default=False)
    scoreboard_public_after_end: Mapped[bool] = mapped_column(Boolean, default=False)
    submission_public_after_end: Mapped[bool] = mapped_column(Boolean, default=False)
    emergency_notice: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)

    divisions: Mapped[list["ContestDivisionRow"]] = relationship(back_populates="contest")


class ContestDivisionRow(Base):
    __tablename__ = "contest_divisions"
    __table_args__ = (UniqueConstraint("contest_id", "code", name="uq_contest_division_code"),)

    division_id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    contest_id: Mapped[str] = mapped_column(ForeignKey("contests.contest_id"), index=True)
    code: Mapped[str] = mapped_column(String(64))
    name: Mapped[str] = mapped_column(String(120))
    description: Mapped[str] = mapped_column(Text, default="")
    display_order: Mapped[int] = mapped_column(Integer, default=1)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)

    contest: Mapped[ContestRow] = relationship(back_populates="divisions")


class TeamMemberRow(Base):
    __tablename__ = "team_members"
    __table_args__ = (UniqueConstraint("contest_id", "email", name="uq_team_member_contest_email"),)

    team_member_id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    contest_id: Mapped[str] = mapped_column(ForeignKey("contests.contest_id"), index=True)
    participant_team_id: Mapped[str] = mapped_column(ForeignKey("participant_teams.participant_team_id"), index=True)
    role: Mapped[str] = mapped_column(String(32))
    name: Mapped[str] = mapped_column(String(120))
    email: Mapped[str] = mapped_column(String(255), index=True)
    active_sessions: Mapped[int] = mapped_column(Integer, default=0)
    last_login_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class ParticipantTeamRow(Base):
    __tablename__ = "participant_teams"
    __table_args__ = (UniqueConstraint("contest_id", "team_name", name="uq_participant_team_contest_name"),)

    participant_team_id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    contest_id: Mapped[str] = mapped_column(ForeignKey("contests.contest_id"), index=True)
    division_id: Mapped[str] = mapped_column(ForeignKey("contest_divisions.division_id"), index=True)
    team_name: Mapped[str] = mapped_column(String(255))
    status: Mapped[str] = mapped_column(String(32), default="invited")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)

    members: Mapped[list[TeamMemberRow]] = relationship(cascade="all, delete-orphan", lazy="selectin")


class ProblemRow(Base):
    __tablename__ = "problems"
    __table_args__ = (UniqueConstraint("contest_id", "division_id", "problem_code", name="uq_problem_division_code"),)

    problem_id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    contest_id: Mapped[str] = mapped_column(ForeignKey("contests.contest_id"), index=True)
    division_id: Mapped[str] = mapped_column(ForeignKey("contest_divisions.division_id"), index=True)
    problem_code: Mapped[str] = mapped_column(String(16))
    title: Mapped[str] = mapped_column(String(255))
    statement: Mapped[str] = mapped_column(Text)
    time_limit_ms: Mapped[int] = mapped_column(Integer)
    memory_limit_mb: Mapped[int] = mapped_column(Integer)
    display_order: Mapped[int] = mapped_column(Integer)
    max_score: Mapped[int] = mapped_column(Integer, default=100)


class ProblemAssetRow(Base):
    __tablename__ = "problem_assets"

    asset_id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    contest_id: Mapped[str] = mapped_column(ForeignKey("contests.contest_id"), index=True)
    problem_id: Mapped[str] = mapped_column(ForeignKey("problems.problem_id"), index=True)
    original_filename: Mapped[str] = mapped_column(String(255))
    storage_key: Mapped[str] = mapped_column(String(512), unique=True)
    mime_type: Mapped[str] = mapped_column(String(120))
    file_size: Mapped[int] = mapped_column(Integer)
    sha256: Mapped[str] = mapped_column(String(64))
    asset_status: Mapped[str] = mapped_column(String(32), default="active")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)


class TestcaseSetRow(Base):
    __tablename__ = "testcase_sets"

    testcase_set_id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    problem_id: Mapped[str] = mapped_column(ForeignKey("problems.problem_id"), index=True)
    version: Mapped[int] = mapped_column(Integer)
    is_active: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)


class TestcaseRow(Base):
    __tablename__ = "testcases"

    testcase_id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    testcase_set_id: Mapped[str] = mapped_column(ForeignKey("testcase_sets.testcase_set_id"), index=True)
    display_order: Mapped[int] = mapped_column(Integer)
    input_storage_key: Mapped[str] = mapped_column(String(512))
    output_storage_key: Mapped[str] = mapped_column(String(512))
    input_sha256: Mapped[str] = mapped_column(String(64))
    output_sha256: Mapped[str] = mapped_column(String(64))
    time_limit_ms_override: Mapped[int | None] = mapped_column(Integer, nullable=True)
    memory_limit_mb_override: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)


class SubmissionRow(Base):
    __tablename__ = "submissions"

    submission_id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    contest_id: Mapped[str] = mapped_column(ForeignKey("contests.contest_id"), index=True)
    division_id: Mapped[str] = mapped_column(ForeignKey("contest_divisions.division_id"), index=True)
    problem_id: Mapped[str] = mapped_column(ForeignKey("problems.problem_id"), index=True)
    participant_team_id: Mapped[str] = mapped_column(ForeignKey("participant_teams.participant_team_id"), index=True)
    team_member_id: Mapped[str] = mapped_column(ForeignKey("team_members.team_member_id"), index=True)
    language: Mapped[str] = mapped_column(String(32))
    source_code: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(32), index=True)
    submitted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)
    status_updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)
    awarded_score: Mapped[int | None] = mapped_column(Integer, nullable=True)
    compile_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    judge_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    failed_testcase_order: Mapped[int | None] = mapped_column(Integer, nullable=True)
    progress_current: Mapped[int | None] = mapped_column(Integer, nullable=True)
    progress_total: Mapped[int | None] = mapped_column(Integer, nullable=True)
    runtime_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    memory_kb: Mapped[int | None] = mapped_column(Integer, nullable=True)


class JudgeJobRow(Base):
    __tablename__ = "judge_jobs"

    judge_job_id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    submission_id: Mapped[str] = mapped_column(ForeignKey("submissions.submission_id"), index=True)
    contest_id: Mapped[str] = mapped_column(ForeignKey("contests.contest_id"), index=True)
    division_id: Mapped[str] = mapped_column(ForeignKey("contest_divisions.division_id"), index=True)
    status: Mapped[str] = mapped_column(String(32), index=True)
    queue_position: Mapped[int] = mapped_column(Integer, index=True)
    assigned_node_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    lease_token: Mapped[str | None] = mapped_column(String(64), nullable=True)
    leased_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)


class ServiceNoticeRow(Base):
    __tablename__ = "service_notices"

    service_notice_id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    title: Mapped[str] = mapped_column(String(255))
    summary: Mapped[str] = mapped_column(Text)
    body: Mapped[str] = mapped_column(Text)
    emergency: Mapped[bool] = mapped_column(Boolean, default=False)
    published_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)


class ContestNoticeRow(Base):
    __tablename__ = "contest_notices"

    contest_notice_id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    contest_id: Mapped[str] = mapped_column(ForeignKey("contests.contest_id"), index=True)
    title: Mapped[str] = mapped_column(String(255))
    body: Mapped[str] = mapped_column(Text)
    pinned: Mapped[bool] = mapped_column(Boolean, default=False)
    emergency: Mapped[bool] = mapped_column(Boolean, default=False)
    visibility: Mapped[str] = mapped_column(String(32), default="public", index=True)
    created_by_email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    published_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)


class ContestQuestionRow(Base):
    __tablename__ = "contest_questions"

    contest_question_id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    contest_id: Mapped[str] = mapped_column(ForeignKey("contests.contest_id"), index=True)
    participant_team_id: Mapped[str] = mapped_column(ForeignKey("participant_teams.participant_team_id"), index=True)
    team_member_id: Mapped[str] = mapped_column(ForeignKey("team_members.team_member_id"), index=True)
    title: Mapped[str] = mapped_column(String(255))
    body: Mapped[str] = mapped_column(Text)
    visibility: Mapped[str] = mapped_column(String(32), default="public", index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)

    answers: Mapped[list["ContestQuestionAnswerRow"]] = relationship(cascade="all, delete-orphan", lazy="selectin")


class ContestQuestionAnswerRow(Base):
    __tablename__ = "contest_question_answers"

    contest_answer_id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    contest_question_id: Mapped[str] = mapped_column(ForeignKey("contest_questions.contest_question_id"), index=True)
    contest_id: Mapped[str] = mapped_column(ForeignKey("contests.contest_id"), index=True)
    body: Mapped[str] = mapped_column(Text)
    visibility: Mapped[str] = mapped_column(String(32), default="public", index=True)
    created_by_email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)


class StaffAccountRow(Base):
    __tablename__ = "staff_accounts"

    staff_account_id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    email: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    display_name: Mapped[str] = mapped_column(String(120))
    is_service_master: Mapped[bool] = mapped_column(Boolean, default=False)
    password_hash: Mapped[str] = mapped_column(Text)
    permissions: Mapped[str] = mapped_column(Text, default="")
    contest_scopes: Mapped[str] = mapped_column(Text, default="{}")


class StaffSessionRow(Base):
    __tablename__ = "staff_sessions"

    staff_session_id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    staff_account_id: Mapped[str] = mapped_column(ForeignKey("staff_accounts.staff_account_id"), index=True)
    access_token_hash: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    refresh_token_hash: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    issued_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)
    access_expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    refresh_expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class GeneralSessionRow(Base):
    __tablename__ = "general_sessions"

    general_session_id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    email: Mapped[str] = mapped_column(String(255), index=True)
    access_token_hash: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    refresh_token_hash: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    issued_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)
    access_expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    refresh_expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class MailQueueItemRow(Base):
    __tablename__ = "mail_queue"

    mail_queue_id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    mail_type: Mapped[str] = mapped_column(String(64))
    recipient_email: Mapped[str] = mapped_column(String(255), index=True)
    subject: Mapped[str] = mapped_column(String(255))
    body_text: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(32), default="pending", index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)


class BundleWarmQueueItemRow(Base):
    __tablename__ = "bundle_warm_queue"

    bundle_warm_queue_id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    contest_id: Mapped[str] = mapped_column(String(36), index=True)
    problem_id: Mapped[str] = mapped_column(String(36), index=True)
    status: Mapped[str] = mapped_column(String(32), default="pending", index=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc, index=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class JudgeNodeRow(Base):
    __tablename__ = "judge_nodes"

    judge_node_id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    node_name: Mapped[str] = mapped_column(String(120), unique=True)
    node_secret_hash: Mapped[str] = mapped_column(Text)
    total_slots: Mapped[int] = mapped_column(Integer, default=10)
    free_slots: Mapped[int] = mapped_column(Integer, default=10)
    running_job_count: Mapped[int] = mapped_column(Integer, default=0)
    last_heartbeat_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)
    schedulable: Mapped[bool] = mapped_column(Boolean, default=True)


class OtpCodeRow(Base):
    __tablename__ = "otp_codes"

    email: Mapped[str] = mapped_column(String(255), primary_key=True)
    contest_id: Mapped[str] = mapped_column(String(36), index=True)
    code: Mapped[str] = mapped_column(String(16))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class TeamSessionRow(Base):
    __tablename__ = "team_sessions"

    team_session_id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    contest_id: Mapped[str] = mapped_column(ForeignKey("contests.contest_id"), index=True)
    division_id: Mapped[str] = mapped_column(ForeignKey("contest_divisions.division_id"), index=True)
    participant_team_id: Mapped[str] = mapped_column(ForeignKey("participant_teams.participant_team_id"), index=True)
    team_member_id: Mapped[str] = mapped_column(ForeignKey("team_members.team_member_id"), index=True)
    access_token_hash: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    issued_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_utc)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
