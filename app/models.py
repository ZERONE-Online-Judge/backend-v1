from datetime import datetime, timedelta, timezone
from enum import StrEnum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, EmailStr, Field


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def new_id() -> str:
    return str(uuid4())


class ContestStatus(StrEnum):
    DRAFT = "draft"
    SCHEDULED = "scheduled"
    OPEN = "open"
    RUNNING = "running"
    ENDED = "ended"
    FINALIZED = "finalized"
    ARCHIVED = "archived"


class SubmissionStatus(StrEnum):
    WAITING = "waiting"
    PREPARING = "preparing"
    JUDGING = "judging"
    ACCEPTED = "accepted"
    WRONG_ANSWER = "wrong_answer"
    COMPILE_ERROR = "compile_error"
    RUNTIME_ERROR = "runtime_error"
    TIME_LIMIT_EXCEEDED = "time_limit_exceeded"
    MEMORY_LIMIT_EXCEEDED = "memory_limit_exceeded"
    OUTPUT_LIMIT_EXCEEDED = "output_limit_exceeded"
    SYSTEM_ERROR = "system_error"


class JudgeJobStatus(StrEnum):
    PENDING = "pending"
    ASSIGNED = "assigned"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class TeamMemberRole(StrEnum):
    LEADER = "leader"
    MEMBER = "member"


class StaffAccount(BaseModel):
    staff_account_id: str = Field(default_factory=new_id)
    email: EmailStr
    display_name: str
    is_service_master: bool = False
    permissions: list[str] = Field(default_factory=list)
    contest_scopes: dict[str, list[str]] = Field(default_factory=dict)


class TeamMember(BaseModel):
    team_member_id: str = Field(default_factory=new_id)
    role: TeamMemberRole
    name: str
    email: EmailStr
    active_sessions: int = 0
    last_login_at: datetime | None = None


class ParticipantTeam(BaseModel):
    participant_team_id: str = Field(default_factory=new_id)
    contest_id: str
    division_id: str
    team_name: str
    status: str = "invited"
    members: list[TeamMember]
    created_at: datetime = Field(default_factory=now_utc)


class Contest(BaseModel):
    contest_id: str = Field(default_factory=new_id)
    title: str
    organization_name: str
    overview: str
    status: ContestStatus
    start_at: datetime
    end_at: datetime
    freeze_at: datetime
    problem_public_after_end: bool = False
    scoreboard_public_after_end: bool = False
    submission_public_after_end: bool = False
    emergency_notice: str | None = None
    created_at: datetime = Field(default_factory=now_utc)


class ContestDivision(BaseModel):
    division_id: str = Field(default_factory=new_id)
    contest_id: str
    code: str
    name: str
    description: str = ""
    display_order: int = 1
    created_at: datetime = Field(default_factory=now_utc)


class Problem(BaseModel):
    problem_id: str = Field(default_factory=new_id)
    contest_id: str
    division_id: str
    problem_code: str
    title: str
    statement: str
    time_limit_ms: int
    memory_limit_mb: int
    display_order: int
    max_score: int = 100


class ProblemAsset(BaseModel):
    asset_id: str = Field(default_factory=new_id)
    contest_id: str
    problem_id: str
    original_filename: str
    storage_key: str
    mime_type: str
    file_size: int
    sha256: str
    asset_status: str = "active"
    created_at: datetime = Field(default_factory=now_utc)


class TestcaseSet(BaseModel):
    testcase_set_id: str = Field(default_factory=new_id)
    problem_id: str
    version: int
    is_active: bool = False
    created_at: datetime = Field(default_factory=now_utc)


class Testcase(BaseModel):
    testcase_id: str = Field(default_factory=new_id)
    testcase_set_id: str
    display_order: int
    input_storage_key: str
    output_storage_key: str
    input_sha256: str
    output_sha256: str
    time_limit_ms_override: int | None = None
    memory_limit_mb_override: int | None = None
    created_at: datetime = Field(default_factory=now_utc)


class Submission(BaseModel):
    submission_id: str = Field(default_factory=new_id)
    contest_id: str
    division_id: str
    problem_id: str
    participant_team_id: str
    team_member_id: str
    language: str
    source_code: str
    status: SubmissionStatus = SubmissionStatus.WAITING
    submitted_at: datetime = Field(default_factory=now_utc)
    status_updated_at: datetime = Field(default_factory=now_utc)
    awarded_score: int | None = None
    compile_message: str | None = None
    judge_message: str | None = None
    failed_testcase_order: int | None = None
    progress_current: int | None = None
    progress_total: int | None = None


class JudgeJob(BaseModel):
    judge_job_id: str = Field(default_factory=new_id)
    submission_id: str
    contest_id: str
    division_id: str
    status: JudgeJobStatus = JudgeJobStatus.PENDING
    queue_position: int
    assigned_node_id: str | None = None
    lease_token: str | None = None
    leased_at: datetime | None = None
    created_at: datetime = Field(default_factory=now_utc)


class ServiceNotice(BaseModel):
    service_notice_id: str = Field(default_factory=new_id)
    title: str
    summary: str
    body: str
    emergency: bool = False
    published_at: datetime = Field(default_factory=now_utc)


class ContestNotice(BaseModel):
    contest_notice_id: str = Field(default_factory=new_id)
    contest_id: str
    title: str
    body: str
    pinned: bool = False
    emergency: bool = False
    visibility: str = "public"
    created_by_email: EmailStr | None = None
    published_at: datetime = Field(default_factory=now_utc)
    updated_at: datetime = Field(default_factory=now_utc)


class ContestQuestionAnswer(BaseModel):
    contest_answer_id: str = Field(default_factory=new_id)
    contest_question_id: str
    contest_id: str
    body: str
    visibility: str = "public"
    created_by_email: EmailStr | None = None
    created_at: datetime = Field(default_factory=now_utc)
    updated_at: datetime = Field(default_factory=now_utc)


class ContestQuestion(BaseModel):
    contest_question_id: str = Field(default_factory=new_id)
    contest_id: str
    participant_team_id: str
    team_member_id: str
    title: str
    body: str
    visibility: str = "public"
    created_at: datetime = Field(default_factory=now_utc)
    updated_at: datetime = Field(default_factory=now_utc)
    team_name: str | None = None
    author_name: str | None = None
    answers: list[ContestQuestionAnswer] = Field(default_factory=list)


class MailQueueItem(BaseModel):
    mail_queue_id: str = Field(default_factory=new_id)
    mail_type: str
    recipient_email: EmailStr
    subject: str
    body_text: str
    status: str = "pending"
    created_at: datetime = Field(default_factory=now_utc)


class JudgeNode(BaseModel):
    judge_node_id: str = Field(default_factory=new_id)
    node_name: str
    total_slots: int = 10
    free_slots: int = 10
    running_job_count: int = 0
    last_heartbeat_at: datetime = Field(default_factory=now_utc)
    schedulable: bool = True


def demo_times() -> tuple[datetime, datetime, datetime]:
    start = now_utc() - timedelta(hours=1)
    end = now_utc() + timedelta(hours=3)
    freeze = end - timedelta(hours=1)
    return start, end, freeze


class ApiResponse(BaseModel):
    data: Any
    request_id: str = Field(default_factory=new_id)
