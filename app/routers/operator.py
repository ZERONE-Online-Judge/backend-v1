import asyncio
import re
import hashlib
import zipfile
from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path
from uuid import uuid4

from fastapi import APIRouter, BackgroundTasks, File, Request, UploadFile
from pydantic import BaseModel, EmailStr

from app.models import ContestStatus, ProblemAsset, TeamMemberRole, now_utc
from app.services.authz import require_contest_staff, require_staff
from app.services.errors import AppError, not_found
from app.services.package_builder import PackageBuildError, build_problem_package, package_role
from app.services.responses import ok, page
from app.services.store import store
from app.services.storage import object_storage
from app.services.testcase_verifier import UploadedTestcase, build_verified_testcase_set, verify_active_testcases_with_candidate_asset

router = APIRouter(tags=["operator"])


def _page_slice(items: list, limit: int, cursor: str | None) -> tuple[list, str | None]:
    safe_limit = max(1, min(limit, 300))
    start = 0
    if cursor:
        try:
            start = max(0, int(cursor))
        except ValueError:
            start = 0
    end = start + safe_limit
    next_cursor = str(end) if end < len(items) else None
    return items[start:end], next_cursor


class TeamMemberPayload(BaseModel):
    name: str
    email: EmailStr


class ParticipantCreateRequest(BaseModel):
    team_name: str
    division_id: str
    leader: TeamMemberPayload
    members: list[TeamMemberPayload]


class ParticipantBulkCreateRequest(BaseModel):
    teams: list[ParticipantCreateRequest]


class ParticipantTeamUpdateRequest(BaseModel):
    team_name: str | None = None
    division_id: str | None = None
    status: str | None = None


class TeamMemberCreateRequest(BaseModel):
    name: str
    email: EmailStr
    role: TeamMemberRole = TeamMemberRole.MEMBER


class TeamMemberUpdateRequest(BaseModel):
    name: str | None = None
    email: EmailStr | None = None


class ContestSettingsUpdateRequest(BaseModel):
    title: str | None = None
    organization_name: str | None = None
    overview: str | None = None
    status: ContestStatus | None = None
    start_at: datetime | None = None
    end_at: datetime | None = None
    freeze_at: datetime | None = None
    problem_public_after_end: bool | None = None
    scoreboard_public_after_end: bool | None = None
    submission_public_after_end: bool | None = None
    emergency_notice: str | None = None


class ContestOperatorCreateRequest(BaseModel):
    email: EmailStr
    display_name: str | None = None


class ContestOperatorUpdateRequest(BaseModel):
    display_name: str


class ContestNoticeCreateRequest(BaseModel):
    title: str
    body: str
    pinned: bool = False
    emergency: bool = False
    visibility: str = "public"


class ContestNoticeUpdateRequest(BaseModel):
    title: str | None = None
    body: str | None = None
    pinned: bool | None = None
    emergency: bool | None = None
    visibility: str | None = None


class ContestAnswerCreateRequest(BaseModel):
    body: str
    visibility: str = "public"


class OperatorTestSubmissionRequest(BaseModel):
    language: str
    source_code: str


class DivisionCreateRequest(BaseModel):
    code: str | None = None
    name: str
    description: str = ""
    display_order: int | None = None


class DivisionUpdateRequest(BaseModel):
    name: str | None = None
    description: str | None = None
    display_order: int | None = None


class ProblemCreateRequest(BaseModel):
    division_id: str
    problem_code: str
    title: str
    statement: str
    time_limit_ms: int
    memory_limit_mb: int
    display_order: int


class ProblemUpdateRequest(BaseModel):
    division_id: str | None = None
    problem_code: str | None = None
    title: str | None = None
    statement: str | None = None
    time_limit_ms: int | None = None
    memory_limit_mb: int | None = None
    display_order: int | None = None


class ProblemAssetCreateRequest(BaseModel):
    original_filename: str
    storage_key: str
    mime_type: str
    file_size: int
    sha256: str


class TestcaseSetCreateRequest(BaseModel):
    is_active: bool = False


class TestcaseSetUpdateRequest(BaseModel):
    is_active: bool | None = None


class TestcaseCreateRequest(BaseModel):
    display_order: int
    input_storage_key: str
    output_storage_key: str
    input_sha256: str
    output_sha256: str
    time_limit_ms_override: int | None = None
    memory_limit_mb_override: int | None = None


class PackageBuildRequest(BaseModel):
    script_text: str | None = None


class VerifiedTestcasePayload(BaseModel):
    display_order: int
    input_storage_key: str
    output_storage_key: str
    input_sha256: str | None = None
    output_sha256: str | None = None


class VerifiedTestcaseSetCreateRequest(BaseModel):
    cases: list[VerifiedTestcasePayload]


class PresignUploadRequest(BaseModel):
    category: str
    filename: str
    content_type: str = "application/octet-stream"


def _format_datetime_for_notice(value: datetime) -> str:
    return value.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")


def _time_update_notice_body(old_start: datetime, old_freeze: datetime, old_end: datetime, new_start: datetime, new_freeze: datetime, new_end: datetime) -> str:
    return "\n".join(
        [
            "대회 운영 시간이 변경되었습니다.",
            "",
            f"- 시작: {_format_datetime_for_notice(old_start)} -> {_format_datetime_for_notice(new_start)}",
            f"- 스코어보드 프리즈: {_format_datetime_for_notice(old_freeze)} -> {_format_datetime_for_notice(new_freeze)}",
            f"- 종료: {_format_datetime_for_notice(old_end)} -> {_format_datetime_for_notice(new_end)}",
            "",
            "변경된 시간 기준으로 제출 가능 여부와 대회 상태가 자동 조정됩니다.",
        ]
    )


def _problem_package_status(contest_id: str, problem_id: str) -> dict:
    assets = store.problem_assets_for_problem(contest_id, problem_id)
    testcase_sets = store.testcase_sets_for_problem(contest_id, problem_id)
    role_assets: dict[str, list] = {}
    for asset in assets:
        role = package_role(asset)
        if role:
            role_assets.setdefault(role, []).append(asset)
    active_set = next((item for item in testcase_sets if item.get("is_active")), None)
    active_count = len(active_set.get("testcases", [])) if active_set else 0
    required_roles = [
        ("package-resource", "testlib.h"),
        ("validator", "validator.cpp"),
        ("checker", "checker.cpp"),
    ]
    support_files = []
    warnings = []
    for role, label in required_roles:
        files = sorted(role_assets.get(role, []), key=lambda item: item.created_at)
        if not files:
            warnings.append(f"{label} 파일이 없습니다.")
        support_files.append(
            {
                "role": role,
                "label": label,
                "required": True,
                "count": len(files),
                "latest_filename": files[-1].original_filename if files else None,
                "status": "ready" if files else "missing",
            }
        )
    if not active_set:
        warnings.append("활성 테스트케이스 세트가 없습니다.")
    elif active_count <= 0:
        warnings.append(f"활성 테스트케이스 세트 v{active_set.get('version')}에 케이스가 없습니다.")
    return {
        "ready": not warnings,
        "warnings": warnings,
        "support_files": support_files,
        "active_testcase_set": active_set,
        "active_testcase_count": active_count,
        "testcase_set_count": len(testcase_sets),
    }


def _testcase_pairs_from_zip(archive: bytes) -> list[tuple[str, bytes, bytes]]:
    try:
        zip_file = zipfile.ZipFile(BytesIO(archive))
    except zipfile.BadZipFile as error:
        raise AppError(422, "invalid_archive", "zip 파일을 열 수 없습니다.") from error
    pairs: dict[str, dict[str, bytes]] = {}
    for info in zip_file.infolist():
        if info.is_dir():
            continue
        path = Path(info.filename)
        filename = path.name
        if filename.startswith(".") or "__MACOSX" in path.parts:
            continue
        suffix = path.suffix.lower()
        if suffix not in {".in", ".out"}:
            continue
        stem = str(path.with_suffix("")).replace("\\", "/")
        if info.file_size > 16 * 1024 * 1024:
            raise AppError(422, "archive_file_too_large", f"{info.filename} 파일이 너무 큽니다.")
        pairs.setdefault(stem, {})[suffix[1:]] = zip_file.read(info)
    result = []
    missing = []
    for stem in sorted(pairs):
        pair = pairs[stem]
        if "in" not in pair or "out" not in pair:
            missing.append(stem)
            continue
        result.append((stem, pair["in"], pair["out"]))
    if missing:
        raise AppError(422, "testcase_pair_missing", f".in/.out 쌍이 맞지 않습니다: {', '.join(missing[:5])}")
    if not result:
        raise AppError(422, "testcase_pair_missing", "zip 안에 .in/.out 테스트케이스 쌍이 없습니다.")
    return result


def _division_code_from_name(name: str) -> str:
    code = re.sub(r"[^a-z0-9]+", "-", name.strip().lower()).strip("-")
    return code or "division"


def _require_contest_mutation_open(contest_id: str):
    contest = store.contests.get(contest_id)
    if not contest:
        raise not_found()
    now = now_utc()
    in_time_window = contest.status != ContestStatus.SCHEDULE_TBD and contest.start_at <= now < contest.end_at and contest.status not in {
        ContestStatus.ENDED,
        ContestStatus.FINALIZED,
        ContestStatus.ARCHIVED,
    }
    if contest.status == ContestStatus.RUNNING or in_time_window:
        raise AppError(409, "contest_locked", "Contest is in progress. Operation-changing updates are locked during contest time.")
    return contest


def _settings_update_changes_operation(updates: dict) -> bool:
    return any(
        key in updates
        for key in {
            "title",
            "organization_name",
            "overview",
            "status",
            "problem_public_after_end",
            "scoreboard_public_after_end",
            "submission_public_after_end",
        }
    )


def _schedule_bundle_warm(background_tasks: BackgroundTasks, contest_id: str, problem_id: str) -> None:
    store.enqueue_bundle_warm(contest_id, problem_id)


@router.get("/operator/contests")
async def operator_contests(request: Request):
    account = require_staff(request)
    contests = store.accessible_contests_for_staff(account)
    return page(request, [contest.model_dump(mode="json") for contest in contests])


@router.get("/operator/contests/{contest_id}/dashboard")
async def operator_dashboard(contest_id: str, request: Request):
    require_contest_staff(request, contest_id)
    contest = store.contests.get(contest_id)
    if not contest:
        raise not_found()
    return ok(
        request,
        {
            "contest": contest.model_dump(mode="json"),
            "divisions": [division.model_dump(mode="json") for division in store.contest_divisions(contest_id)],
            "participant_count": len([team for team in store.teams.values() if team.contest_id == contest_id]),
            "submission_count": len([s for s in store.submissions.values() if s.contest_id == contest_id]),
            "pending_jobs": len([j for j in store.judge_jobs.values() if j.contest_id == contest_id and j.status == "pending"]),
            "operators": [account.model_dump(mode="json") for account in store.contest_operator_accounts(contest_id)],
            "participant_count_by_division": {
                division.division_id: len(
                    [team for team in store.teams.values() if team.contest_id == contest_id and team.division_id == division.division_id]
                )
                for division in store.contest_divisions(contest_id)
            },
        },
    )


@router.get("/operator/contests/{contest_id}/divisions")
async def divisions(contest_id: str, request: Request):
    require_contest_staff(request, contest_id)
    if contest_id not in store.contests:
        raise not_found()
    return page(request, [division.model_dump(mode="json") for division in store.contest_divisions(contest_id)])


@router.post("/operator/contests/{contest_id}/divisions")
async def create_division(contest_id: str, payload: DivisionCreateRequest, request: Request):
    require_contest_staff(request, contest_id)
    _require_contest_mutation_open(contest_id)
    try:
        division = store.create_contest_division(
            contest_id=contest_id,
            code=payload.code or _division_code_from_name(payload.name),
            name=payload.name,
            description=payload.description,
            display_order=payload.display_order or 1,
        )
    except ValueError as error:
        message = str(error)
        if message == "contest not found":
            raise not_found()
        raise AppError(422, "validation_error", message)
    return ok(request, division.model_dump(mode="json"))


@router.patch("/operator/contests/{contest_id}/divisions/{division_id}")
async def update_division(contest_id: str, division_id: str, payload: DivisionUpdateRequest, request: Request):
    require_contest_staff(request, contest_id)
    _require_contest_mutation_open(contest_id)
    try:
        division = store.update_contest_division(contest_id, division_id, **payload.model_dump(exclude_unset=True))
    except ValueError as error:
        raise AppError(422, "validation_error", str(error))
    if not division:
        raise not_found()
    return ok(request, division.model_dump(mode="json"))


@router.patch("/operator/contests/{contest_id}/settings")
async def update_contest_settings(contest_id: str, payload: ContestSettingsUpdateRequest, request: Request):
    account = require_contest_staff(request, contest_id)
    contest = store.contests.get(contest_id)
    if not contest:
        raise not_found()
    updates = payload.model_dump(exclude_unset=True)
    if _settings_update_changes_operation(updates):
        _require_contest_mutation_open(contest_id)

    explicit_start = "start_at" in updates
    explicit_end = "end_at" in updates
    explicit_freeze = "freeze_at" in updates

    start_at = updates.get("start_at", contest.start_at)
    end_at = updates.get("end_at", contest.end_at)
    freeze_at = updates.get("freeze_at", contest.freeze_at)

    # Allow practical schedule edits while contest is running/ended.
    # If only one side of time range is edited and range becomes invalid,
    # keep existing duration when possible instead of hard-failing.
    if start_at >= end_at:
        current_duration = max(contest.end_at - contest.start_at, timedelta(hours=1))
        if explicit_start and not explicit_end:
            end_at = start_at + current_duration
            updates["end_at"] = end_at
        elif explicit_end and not explicit_start:
            start_at = end_at - current_duration
            updates["start_at"] = start_at
        else:
            raise AppError(422, "validation_error", "start_at must be before end_at.")

    if not (start_at <= freeze_at <= end_at):
        if explicit_freeze:
            raise AppError(422, "validation_error", "freeze_at must be between start_at and end_at.")
        # If freeze wasn't explicitly edited, auto-clamp to a sane point.
        default_freeze = end_at - timedelta(hours=1)
        if default_freeze < start_at:
            default_freeze = start_at
        if default_freeze > end_at:
            default_freeze = end_at
        freeze_at = default_freeze
        updates["freeze_at"] = freeze_at
    time_fields = {"start_at", "end_at", "freeze_at"}
    time_changed = any(key in updates and getattr(contest, key) != updates[key] for key in time_fields)
    if time_changed:
        auto_notice = _time_update_notice_body(contest.start_at, contest.freeze_at, contest.end_at, start_at, freeze_at, end_at)
        manual_notice = updates.get("emergency_notice")
        updates["emergency_notice"] = f"{auto_notice}\n\n{manual_notice}".strip() if manual_notice else auto_notice
    updated = store.update_contest_settings(contest_id, **updates)
    if not updated:
        raise not_found()
    changed_lines = []
    for key, new_value in updates.items():
        old_value = getattr(contest, key)
        if old_value == new_value:
            continue
        changed_lines.append(f"- {key}: {old_value} -> {new_value}")
    if changed_lines:
        if time_changed:
            store.create_contest_notice(
                contest_id,
                "대회 운영 시간이 변경되었습니다",
                updates["emergency_notice"],
                pinned=True,
                emergency=True,
                visibility="participants",
                created_by_email=str(account.email),
            )
        store.notify_contest_operators(
            contest_id,
            "contest_settings_updated",
            f"[Zerone OJ] {updated.title} settings updated",
            "\n".join(
                [
                    f"Contest: {updated.title}",
                    f"Changed by: {account.display_name} <{account.email}>",
                    "",
                    "Updated fields:",
                    *changed_lines,
                ]
            ),
        )
    return ok(request, updated.model_dump(mode="json"))


@router.get("/operator/contests/{contest_id}/operators")
async def contest_operators(contest_id: str, request: Request):
    require_contest_staff(request, contest_id)
    if contest_id not in store.contests:
        raise not_found()
    return page(request, [account.model_dump(mode="json") for account in store.contest_operator_accounts(contest_id)])


@router.post("/operator/contests/{contest_id}/operators")
async def create_contest_operator(contest_id: str, payload: ContestOperatorCreateRequest, request: Request):
    account = require_contest_staff(request, contest_id)
    try:
        operator = store.upsert_contest_operator(contest_id, str(payload.email), payload.display_name or str(payload.email))
    except ValueError as exc:
        message = str(exc)
        if message.startswith("operator email cannot be participant email:"):
            raise AppError(422, "validation_error", message, {"field": "operator_email_conflict"})
        raise AppError(404, "not_found", message)
    contest = store.contests.get(contest_id)
    if contest:
        store.enqueue_mail(
            "contest_operator_assigned",
            str(payload.email),
            f"[Zerone OJ] {contest.title} operator assignment",
            "\n".join(
                [
                    f"Contest: {contest.title}",
                    f"Assigned by: {account.display_name} <{account.email}>",
                    f"Status: {contest.status}",
                    f"Start: {contest.start_at.isoformat()}",
                ]
            ),
        )
    return ok(request, operator.model_dump(mode="json"))


@router.patch("/operator/contests/{contest_id}/operators/{operator_email}")
async def update_contest_operator(contest_id: str, operator_email: str, payload: ContestOperatorUpdateRequest, request: Request):
    require_contest_staff(request, contest_id)
    operator = store.update_contest_operator(contest_id, operator_email, payload.display_name)
    if not operator:
        raise not_found()
    return ok(request, operator.model_dump(mode="json"))


@router.delete("/operator/contests/{contest_id}/operators/{operator_email}")
async def delete_contest_operator(contest_id: str, operator_email: str, request: Request):
    account = require_contest_staff(request, contest_id)
    current_operators = store.contest_operator_accounts(contest_id)
    if len(current_operators) <= 1:
        raise AppError(409, "last_operator", "마지막 대회 운영자는 제거할 수 없습니다.")
    if str(account.email) == operator_email:
        raise AppError(409, "self_remove_denied", "현재 로그인한 운영자 자신은 제거할 수 없습니다.")
    removed = store.remove_contest_operator(contest_id, operator_email)
    if not removed:
        raise not_found()
    return ok(request, removed.model_dump(mode="json"))


@router.get("/operator/contests/{contest_id}/notices")
async def operator_notices(contest_id: str, request: Request):
    require_contest_staff(request, contest_id)
    if contest_id not in store.contests:
        raise not_found()
    return page(request, [notice.model_dump(mode="json") for notice in store.contest_notices_for_view(contest_id, operator=True)])


@router.post("/operator/contests/{contest_id}/notices")
async def create_notice(contest_id: str, payload: ContestNoticeCreateRequest, request: Request):
    account = require_contest_staff(request, contest_id)
    if payload.visibility not in {"public", "participants"}:
        raise AppError(422, "validation_error", "Unsupported notice visibility.")
    notice = store.create_contest_notice(
        contest_id,
        payload.title,
        payload.body,
        payload.pinned,
        payload.emergency,
        payload.visibility,
        str(account.email),
    )
    return ok(request, notice.model_dump(mode="json"))


@router.patch("/operator/contests/{contest_id}/notices/{notice_id}")
async def update_notice(contest_id: str, notice_id: str, payload: ContestNoticeUpdateRequest, request: Request):
    require_contest_staff(request, contest_id)
    updates = payload.model_dump(exclude_unset=True)
    if "visibility" in updates and updates["visibility"] not in {"public", "participants"}:
        raise AppError(422, "validation_error", "Unsupported notice visibility.")
    notice = store.update_contest_notice(contest_id, notice_id, **updates)
    if not notice:
        raise not_found()
    return ok(request, notice.model_dump(mode="json"))


@router.get("/operator/contests/{contest_id}/boards")
async def operator_board(contest_id: str, request: Request):
    require_contest_staff(request, contest_id)
    if contest_id not in store.contests:
        raise not_found()
    return page(request, [question.model_dump(mode="json") for question in store.questions_for_view(contest_id, operator=True)])


@router.post("/operator/contests/{contest_id}/boards/{question_id}/answers")
async def create_answer(contest_id: str, question_id: str, payload: ContestAnswerCreateRequest, request: Request):
    account = require_contest_staff(request, contest_id)
    if payload.visibility not in {"public", "questioner"}:
        raise AppError(422, "validation_error", "Unsupported answer visibility.")
    answer = store.create_answer(contest_id, question_id, payload.body, payload.visibility, str(account.email))
    if not answer:
        raise not_found()
    question = store.get_contest_question(contest_id, question_id)
    contest = store.contests.get(contest_id)
    if question and contest:
        recipient_emails = store.participant_team_member_emails(contest_id, question.participant_team_id)
        subject = f"[Zerone OJ] 질문 답변 등록 · {contest.title}"
        body_lines = [
            f"Contest: {contest.title}",
            f"Question title: {question.title}",
            f"Answer visibility: {answer.visibility}",
            f"Answered by: {account.display_name} <{account.email}>",
            "",
            answer.body,
        ]
        for email in recipient_emails:
            store.enqueue_mail("contest_question_answered", email, subject, "\n".join(body_lines))
    return ok(request, answer.model_dump(mode="json"))


@router.get("/operator/contests/{contest_id}/participants")
async def participants(contest_id: str, request: Request):
    require_contest_staff(request, contest_id)
    items = []
    for team in sorted(store.teams.values(), key=lambda item: (item.team_name or "").lower()):
        if team.contest_id != contest_id:
            continue
        division = store.get_division(contest_id, team.division_id)
        items.append({**team.model_dump(mode="json"), "division": division.model_dump(mode="json") if division else None})
    return page(request, items)


@router.post("/operator/contests/{contest_id}/participants")
async def create_participant(contest_id: str, payload: ParticipantCreateRequest, request: Request):
    require_contest_staff(request, contest_id)
    if not store.get_division(contest_id, payload.division_id):
        raise not_found("Contest division is not configured.")
    try:
        team = store.create_participant_team(
            contest_id=contest_id,
            division_id=payload.division_id,
            team_name=payload.team_name,
            leader_name=payload.leader.name,
            leader_email=str(payload.leader.email),
            members=[(member.name, str(member.email)) for member in payload.members],
        )
    except ValueError as error:
        raise AppError(422, "validation_error", str(error), {"field": "email_conflict"})
    return ok(request, team.model_dump(mode="json"))


@router.post("/operator/contests/{contest_id}/participants:bulk-create")
async def bulk_create_participants(contest_id: str, payload: ParticipantBulkCreateRequest, request: Request):
    require_contest_staff(request, contest_id)
    created = []
    errors = []
    for index, item in enumerate(payload.teams, start=1):
        if not store.get_division(contest_id, item.division_id):
            errors.append({"row": index, "team_name": item.team_name, "message": "Contest division is not configured."})
            continue
        try:
            team = store.create_participant_team(
                contest_id=contest_id,
                division_id=item.division_id,
                team_name=item.team_name,
                leader_name=item.leader.name,
                leader_email=str(item.leader.email),
                members=[(member.name, str(member.email)) for member in item.members],
            )
            division = store.get_division(contest_id, team.division_id)
            created.append({**team.model_dump(mode="json"), "division": division.model_dump(mode="json") if division else None})
        except ValueError as error:
            message = str(error)
            if message.startswith("participant email already registered:"):
                message = "participant email already registered"
            errors.append({"row": index, "team_name": item.team_name, "message": message})
    return ok(request, {"created": created, "errors": errors})


@router.patch("/operator/contests/{contest_id}/participants/{participant_team_id}")
async def update_participant(contest_id: str, participant_team_id: str, payload: ParticipantTeamUpdateRequest, request: Request):
    require_contest_staff(request, contest_id)
    _require_contest_mutation_open(contest_id)
    if payload.status is not None and payload.status not in {"invited", "active", "disabled", "disqualified"}:
        raise AppError(422, "validation_error", "Unsupported participant team status.")
    try:
        team = store.update_participant_team(
            contest_id=contest_id,
            participant_team_id=participant_team_id,
            team_name=payload.team_name,
            division_id=payload.division_id,
            status=payload.status,
        )
    except ValueError:
        raise not_found("Contest division is not configured.")
    if not team:
        raise not_found()
    division = store.get_division(contest_id, team.division_id)
    return ok(request, {**team.model_dump(mode="json"), "division": division.model_dump(mode="json") if division else None})


@router.delete("/operator/contests/{contest_id}/participants/{participant_team_id}")
async def delete_participant(contest_id: str, participant_team_id: str, request: Request):
    require_contest_staff(request, contest_id)
    deleted, reason = store.delete_participant_team(contest_id, participant_team_id)
    if not deleted:
        if reason == "has_submission":
            raise AppError(409, "participant_has_submission", "제출 이력이 있는 참가팀은 삭제할 수 없습니다.")
        if reason == "has_question":
            raise AppError(409, "participant_has_question", "질문 이력이 있는 참가팀은 삭제할 수 없습니다.")
        raise not_found()
    return ok(request, {"participant_team_id": participant_team_id, "deleted": True})


@router.post("/operator/contests/{contest_id}/participants/{participant_team_id}/members")
async def add_participant_member(contest_id: str, participant_team_id: str, payload: TeamMemberCreateRequest, request: Request):
    require_contest_staff(request, contest_id)
    try:
        member = store.add_team_member(
            contest_id=contest_id,
            participant_team_id=participant_team_id,
            name=payload.name,
            email=str(payload.email),
            role=payload.role,
        )
    except ValueError as error:
        raise AppError(422, "validation_error", str(error), {"field": "email_conflict"})
    if not member:
        raise not_found()
    return ok(request, member.model_dump(mode="json"))


@router.patch("/operator/contests/{contest_id}/participants/{participant_team_id}/members/{team_member_id}")
async def update_participant_member(contest_id: str, participant_team_id: str, team_member_id: str, payload: TeamMemberUpdateRequest, request: Request):
    require_contest_staff(request, contest_id)
    try:
        member = store.update_team_member(
            contest_id=contest_id,
            participant_team_id=participant_team_id,
            team_member_id=team_member_id,
            name=payload.name,
            email=str(payload.email) if payload.email is not None else None,
        )
    except ValueError as error:
        raise AppError(422, "validation_error", str(error), {"field": "email_conflict"})
    if not member:
        raise not_found()
    return ok(request, member.model_dump(mode="json"))


@router.post("/operator/contests/{contest_id}/participants/{participant_team_id}/members/{team_member_id}/sessions:revoke")
async def revoke_participant_member_sessions(contest_id: str, participant_team_id: str, team_member_id: str, request: Request):
    require_contest_staff(request, contest_id)
    member = store.revoke_team_member_sessions(contest_id, participant_team_id, team_member_id)
    if not member:
        raise not_found()
    return ok(request, member.model_dump(mode="json"))


@router.get("/operator/contests/{contest_id}/submissions")
async def operator_submissions(contest_id: str, request: Request, limit: int = 100, cursor: str | None = None, include_source: bool = False):
    require_contest_staff(request, contest_id)
    teams = {team.participant_team_id: team for team in store.teams.values() if team.contest_id == contest_id}
    members = {
        member.team_member_id: member
        for team in teams.values()
        for member in team.members
    }
    items = []
    for submission in store.submissions.values():
        if submission.contest_id != contest_id:
            continue
        payload = submission.model_dump(mode="json")
        team = teams.get(submission.participant_team_id)
        member = members.get(submission.team_member_id)
        payload["team_name"] = team.team_name if team else None
        payload["member_name"] = member.name if member else None
        payload["member_email"] = str(member.email) if member else None
        if not include_source:
            payload["source_code"] = None
        items.append(payload)
    items.sort(key=lambda item: item.get("submitted_at", ""), reverse=True)
    sliced, next_cursor = _page_slice(items, limit, cursor)
    return page(
        request,
        sliced,
        next_cursor=next_cursor,
        limit=max(1, min(limit, 300)),
        total_count=len(items),
        current_cursor=cursor,
    )


@router.get("/operator/contests/{contest_id}/submissions/{submission_id}")
async def operator_submission_detail(contest_id: str, submission_id: str, request: Request):
    require_contest_staff(request, contest_id)
    submission = store.submissions.get(submission_id)
    if not submission or submission.contest_id != contest_id:
        raise not_found()
    team = store.teams.get(submission.participant_team_id)
    member = next((item for item in (team.members if team else []) if item.team_member_id == submission.team_member_id), None)
    payload = submission.model_dump(mode="json")
    payload["team_name"] = team.team_name if team else None
    payload["member_name"] = member.name if member else None
    payload["member_email"] = str(member.email) if member else None
    return ok(request, payload)


@router.get("/operator/contests/{contest_id}/submissions/{submission_id}/status:wait")
async def operator_wait_submission_status(
    contest_id: str,
    submission_id: str,
    request: Request,
    wait_seconds: float = 2.0,
    poll_interval_seconds: float = 0.25,
):
    require_contest_staff(request, contest_id)
    submission = store.submissions.get(submission_id)
    if not submission or submission.contest_id != contest_id:
        raise not_found()
    wait_budget = max(0.0, min(wait_seconds, 10.0))
    poll = max(0.1, min(poll_interval_seconds, 1.0))
    loops = max(1, int(wait_budget / poll))
    for _ in range(loops):
        updated = store.submissions.get(submission_id)
        if not updated:
            raise not_found()
        if updated.status not in {"waiting", "preparing", "judging"}:
            payload = updated.model_dump(mode="json")
            payload["source_code"] = None
            return ok(request, payload)
        await asyncio.sleep(poll)
    latest = store.submissions.get(submission_id)
    if not latest:
        raise not_found()
    payload = latest.model_dump(mode="json")
    payload["source_code"] = None
    return ok(request, payload)


@router.post("/operator/contests/{contest_id}/problems/{problem_id}/test-submissions")
async def create_operator_test_submission(contest_id: str, problem_id: str, payload: OperatorTestSubmissionRequest, request: Request):
    require_contest_staff(request, contest_id)
    if payload.language not in {"c99", "cpp17", "python313", "java8"}:
        raise AppError(422, "validation_error", "Unsupported language.", {"fields": [{"path": "body.language", "code": "invalid_enum"}]})
    if not payload.source_code.strip():
        raise AppError(422, "validation_error", "Source code is required.")
    try:
        submission = store.create_operator_test_submission(contest_id, problem_id, payload.language, payload.source_code)
    except ValueError:
        raise not_found()
    return ok(request, submission.model_dump(mode="json"))


@router.get("/operator/contests/{contest_id}/test-submissions/{submission_id}")
async def operator_test_submission_detail(contest_id: str, submission_id: str, request: Request):
    require_contest_staff(request, contest_id)
    submission = store.submissions.get(submission_id)
    if not submission or submission.contest_id != contest_id:
        raise not_found()
    return ok(request, submission.model_dump(mode="json"))


@router.get("/operator/contests/{contest_id}/test-submissions/{submission_id}/status:wait")
async def operator_wait_test_submission_status(
    contest_id: str,
    submission_id: str,
    request: Request,
    wait_seconds: float = 2.0,
    poll_interval_seconds: float = 0.25,
):
    require_contest_staff(request, contest_id)
    submission = store.submissions.get(submission_id)
    if not submission or submission.contest_id != contest_id:
        raise not_found()
    wait_budget = max(0.0, min(wait_seconds, 10.0))
    poll = max(0.1, min(poll_interval_seconds, 1.0))
    loops = max(1, int(wait_budget / poll))
    for _ in range(loops):
        updated = store.submissions.get(submission_id)
        if not updated:
            raise not_found()
        if updated.status not in {"waiting", "preparing", "judging"}:
            return ok(request, updated.model_dump(mode="json"))
        await asyncio.sleep(poll)
    latest = store.submissions.get(submission_id)
    if not latest:
        raise not_found()
    return ok(request, latest.model_dump(mode="json"))


@router.get("/operator/contests/{contest_id}/judge-history")
async def judge_history(contest_id: str, request: Request, limit: int = 100, cursor: str | None = None):
    require_contest_staff(request, contest_id)
    jobs = [job.model_dump(mode="json") for job in store.judge_jobs.values() if job.contest_id == contest_id]
    jobs.sort(key=lambda item: item.get("created_at", ""), reverse=True)
    sliced, next_cursor = _page_slice(jobs, limit, cursor)
    return page(
        request,
        sliced,
        next_cursor=next_cursor,
        limit=max(1, min(limit, 300)),
        total_count=len(jobs),
        current_cursor=cursor,
    )


@router.get("/operator/contests/{contest_id}/scoreboard/internal")
async def internal_scoreboard(contest_id: str, request: Request):
    require_contest_staff(request, contest_id)
    board = store.scoreboard_rows(contest_id, public_view=False)
    if not board:
        raise not_found()
    rows = [{**row, "visible_to_team": False} for row in board["rows"]]
    return ok(request, {"frozen_public_view": board["frozen"], "operator_live_view": True, "rows": rows})


@router.get("/operator/contests/{contest_id}/divisions/{division_id}/scoreboard/internal")
async def division_internal_scoreboard(contest_id: str, division_id: str, request: Request):
    require_contest_staff(request, contest_id)
    division = store.get_division(contest_id, division_id)
    if not division:
        raise not_found()
    board = store.scoreboard_rows(contest_id, division_id, public_view=False)
    if not board:
        raise not_found()
    rows = [{**row, "visible_to_team": False} for row in board["rows"]]
    return ok(request, {"division": division.model_dump(mode="json"), "frozen_public_view": board["frozen"], "operator_live_view": True, "rows": rows})


@router.get("/operator/contests/{contest_id}/problems")
async def operator_problems(contest_id: str, request: Request):
    require_contest_staff(request, contest_id)
    problems = [p for p in store.problems.values() if p.contest_id == contest_id]
    problems.sort(key=lambda item: (item.display_order, item.problem_code, item.title, item.problem_id))
    return page(request, [p.model_dump(mode="json") for p in problems])


@router.post("/operator/contests/{contest_id}/storage/presign-upload")
async def presign_upload(contest_id: str, payload: PresignUploadRequest, request: Request):
    require_contest_staff(request, contest_id)
    _require_contest_mutation_open(contest_id)
    storage_key = object_storage.storage_key(contest_id, payload.category, payload.filename)
    return ok(
        request,
        {
            "method": "PUT",
            "storage_key": storage_key,
            "upload_url": object_storage.presigned_put_url(storage_key),
            "content_type": payload.content_type,
        },
    )


@router.post("/operator/contests/{contest_id}/problems")
async def create_problem(contest_id: str, payload: ProblemCreateRequest, request: Request):
    require_contest_staff(request, contest_id)
    _require_contest_mutation_open(contest_id)
    try:
        problem = store.create_problem(
            contest_id=contest_id,
            division_id=payload.division_id,
            problem_code=payload.problem_code,
            title=payload.title,
            statement=payload.statement,
            time_limit_ms=payload.time_limit_ms,
            memory_limit_mb=payload.memory_limit_mb,
            display_order=payload.display_order,
            max_score=100,
        )
    except ValueError:
        raise not_found("Contest division is not configured.")
    return ok(request, problem.model_dump(mode="json"))


@router.patch("/operator/contests/{contest_id}/problems/{problem_id}")
async def update_problem(contest_id: str, problem_id: str, payload: ProblemUpdateRequest, request: Request):
    require_contest_staff(request, contest_id)
    _require_contest_mutation_open(contest_id)
    try:
        problem = store.update_problem(contest_id, problem_id, **payload.model_dump(exclude_unset=True))
    except ValueError as error:
        raise AppError(422, "validation_error", str(error))
    if not problem:
        raise not_found()
    return ok(request, problem.model_dump(mode="json"))


@router.get("/operator/contests/{contest_id}/problems/{problem_id}/assets")
async def problem_assets(contest_id: str, problem_id: str, request: Request):
    require_contest_staff(request, contest_id)
    try:
        assets = store.problem_assets_for_problem(contest_id, problem_id)
    except ValueError:
        raise not_found()
    return page(
        request,
        [{**asset.model_dump(mode="json"), "download_url": object_storage.presigned_get_url(asset.storage_key)} for asset in assets],
    )


@router.post("/operator/contests/{contest_id}/problems/{problem_id}/assets")
async def create_problem_asset(contest_id: str, problem_id: str, payload: ProblemAssetCreateRequest, request: Request, background_tasks: BackgroundTasks):
    require_contest_staff(request, contest_id)
    _require_contest_mutation_open(contest_id)
    candidate_asset = ProblemAsset(
        contest_id=contest_id,
        problem_id=problem_id,
        original_filename=payload.original_filename,
        storage_key=payload.storage_key,
        mime_type=payload.mime_type,
        file_size=payload.file_size,
        sha256=payload.sha256,
    )
    try:
        verify_active_testcases_with_candidate_asset(contest_id, problem_id, candidate_asset)
    except PackageBuildError as error:
        try:
            object_storage.delete(payload.storage_key)
        except Exception:
            pass
        raise AppError(422, "package_asset_verification_failed", str(error))
    try:
        asset = store.create_problem_asset(
            contest_id=contest_id,
            problem_id=problem_id,
            original_filename=payload.original_filename,
            storage_key=payload.storage_key,
            mime_type=payload.mime_type,
            file_size=payload.file_size,
            sha256=payload.sha256,
        )
    except ValueError:
        raise not_found()
    _schedule_bundle_warm(background_tasks, contest_id, problem_id)
    return ok(request, asset.model_dump(mode="json"))


@router.delete("/operator/contests/{contest_id}/problems/{problem_id}/assets/{asset_id}")
async def delete_problem_asset(contest_id: str, problem_id: str, asset_id: str, request: Request, background_tasks: BackgroundTasks):
    require_contest_staff(request, contest_id)
    _require_contest_mutation_open(contest_id)
    item = store.delete_problem_asset(contest_id, problem_id, asset_id)
    if not item:
        raise not_found()
    _schedule_bundle_warm(background_tasks, contest_id, problem_id)
    return ok(request, item.model_dump(mode="json"))


@router.get("/operator/contests/{contest_id}/problems/{problem_id}/testcase-sets")
async def testcase_sets(contest_id: str, problem_id: str, request: Request):
    require_contest_staff(request, contest_id)
    try:
        items = store.testcase_sets_for_problem(contest_id, problem_id)
    except ValueError:
        raise not_found()
    return page(request, items)


@router.get("/operator/contests/{contest_id}/problems/{problem_id}/package-status")
async def package_status(contest_id: str, problem_id: str, request: Request):
    require_contest_staff(request, contest_id)
    try:
        return ok(request, _problem_package_status(contest_id, problem_id))
    except ValueError:
        raise not_found()


@router.post("/operator/contests/{contest_id}/problems/{problem_id}/testcase-sets")
async def create_testcase_set(contest_id: str, problem_id: str, payload: TestcaseSetCreateRequest, request: Request, background_tasks: BackgroundTasks):
    require_contest_staff(request, contest_id)
    _require_contest_mutation_open(contest_id)
    try:
        item = store.create_testcase_set(contest_id, problem_id, payload.is_active)
    except ValueError:
        raise not_found()
    _schedule_bundle_warm(background_tasks, contest_id, problem_id)
    return ok(request, item.model_dump(mode="json"))


@router.patch("/operator/contests/{contest_id}/problems/{problem_id}/testcase-sets/{testcase_set_id}")
async def update_testcase_set(
    contest_id: str,
    problem_id: str,
    testcase_set_id: str,
    payload: TestcaseSetUpdateRequest,
    request: Request,
    background_tasks: BackgroundTasks,
):
    require_contest_staff(request, contest_id)
    _require_contest_mutation_open(contest_id)
    item = store.update_testcase_set(contest_id, problem_id, testcase_set_id, **payload.model_dump(exclude_unset=True))
    if not item:
        raise not_found()
    _schedule_bundle_warm(background_tasks, contest_id, problem_id)
    return ok(request, item.model_dump(mode="json"))


@router.post("/operator/contests/{contest_id}/problems/{problem_id}/testcase-sets/{testcase_set_id}/testcases")
async def create_testcase(
    contest_id: str,
    problem_id: str,
    testcase_set_id: str,
    payload: TestcaseCreateRequest,
    request: Request,
    background_tasks: BackgroundTasks,
):
    require_contest_staff(request, contest_id)
    _require_contest_mutation_open(contest_id)
    try:
        item = store.add_testcase(
            contest_id=contest_id,
            problem_id=problem_id,
            testcase_set_id=testcase_set_id,
            display_order=payload.display_order,
            input_storage_key=payload.input_storage_key,
            output_storage_key=payload.output_storage_key,
            input_sha256=payload.input_sha256,
            output_sha256=payload.output_sha256,
            time_limit_ms_override=payload.time_limit_ms_override,
            memory_limit_mb_override=payload.memory_limit_mb_override,
        )
    except ValueError:
        raise not_found()
    _schedule_bundle_warm(background_tasks, contest_id, problem_id)
    return ok(request, item.model_dump(mode="json"))


@router.delete("/operator/contests/{contest_id}/problems/{problem_id}/testcase-sets/{testcase_set_id}")
async def delete_testcase_set(
    contest_id: str,
    problem_id: str,
    testcase_set_id: str,
    request: Request,
    background_tasks: BackgroundTasks,
):
    require_contest_staff(request, contest_id)
    _require_contest_mutation_open(contest_id)
    item = store.delete_testcase_set(contest_id, problem_id, testcase_set_id)
    if not item:
        raise not_found()
    _schedule_bundle_warm(background_tasks, contest_id, problem_id)
    return ok(request, item.model_dump(mode="json"))


@router.delete("/operator/contests/{contest_id}/problems/{problem_id}/testcase-sets/{testcase_set_id}/testcases/{testcase_id}")
async def delete_testcase(
    contest_id: str,
    problem_id: str,
    testcase_set_id: str,
    testcase_id: str,
    request: Request,
    background_tasks: BackgroundTasks,
):
    require_contest_staff(request, contest_id)
    _require_contest_mutation_open(contest_id)
    item = store.delete_testcase(contest_id, problem_id, testcase_set_id, testcase_id)
    if not item:
        raise not_found()
    _schedule_bundle_warm(background_tasks, contest_id, problem_id)
    return ok(request, item.model_dump(mode="json"))


@router.post("/operator/contests/{contest_id}/problems/{problem_id}/verified-testcase-sets:zip")
async def create_verified_testcase_set_from_zip(
    contest_id: str,
    problem_id: str,
    request: Request,
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
):
    require_contest_staff(request, contest_id)
    _require_contest_mutation_open(contest_id)
    archive = await file.read()
    if len(archive) > 128 * 1024 * 1024:
        raise AppError(422, "archive_too_large", "zip 파일은 128MB 이하만 업로드할 수 있습니다.")
    batch_id = uuid4().hex[:12]
    cases = []
    for index, (stem, input_bytes, output_bytes) in enumerate(_testcase_pairs_from_zip(archive), start=1):
        safe_stem = re.sub(r"[^A-Za-z0-9_.-]+", "-", stem).strip("-") or f"{index:03d}"
        input_key = f"contests/{contest_id}/problems/{problem_id}/testcases/imports/{batch_id}/{index:03d}-{safe_stem}.in"
        output_key = f"contests/{contest_id}/problems/{problem_id}/testcases/imports/{batch_id}/{index:03d}-{safe_stem}.out"
        object_storage.write_bytes(input_key, input_bytes, "text/plain")
        object_storage.write_bytes(output_key, output_bytes, "text/plain")
        cases.append(
            UploadedTestcase(
                display_order=index,
                input_storage_key=input_key,
                output_storage_key=output_key,
                input_sha256=hashlib.sha256(input_bytes).hexdigest(),
                output_sha256=hashlib.sha256(output_bytes).hexdigest(),
            )
        )
    try:
        result = build_verified_testcase_set(contest_id, problem_id, cases)
    except PackageBuildError as error:
        raise AppError(422, "testcase_verification_failed", str(error))
    except ValueError:
        raise not_found()
    result["imported_archive"] = {"filename": file.filename, "case_count": len(cases), "format": "paired .in/.out by same path stem"}
    _schedule_bundle_warm(background_tasks, contest_id, problem_id)
    return ok(request, result)


@router.post("/operator/contests/{contest_id}/problems/{problem_id}/verified-testcase-sets")
async def create_verified_testcase_set(
    contest_id: str,
    problem_id: str,
    payload: VerifiedTestcaseSetCreateRequest,
    request: Request,
    background_tasks: BackgroundTasks,
):
    require_contest_staff(request, contest_id)
    _require_contest_mutation_open(contest_id)
    try:
        result = build_verified_testcase_set(
            contest_id,
            problem_id,
            [
                UploadedTestcase(
                    display_order=item.display_order,
                    input_storage_key=item.input_storage_key,
                    output_storage_key=item.output_storage_key,
                    input_sha256=item.input_sha256,
                    output_sha256=item.output_sha256,
                )
                for item in payload.cases
            ],
        )
    except PackageBuildError as error:
        raise AppError(422, "testcase_verification_failed", str(error))
    except ValueError:
        raise not_found()
    except Exception as error:
        raise AppError(422, "testcase_verification_failed", f"unexpected verifier error: {error}")
    _schedule_bundle_warm(background_tasks, contest_id, problem_id)
    return ok(request, result)


@router.post("/operator/contests/{contest_id}/problems/{problem_id}/package-builds")
async def build_package(
    contest_id: str,
    problem_id: str,
    payload: PackageBuildRequest,
    request: Request,
    background_tasks: BackgroundTasks,
):
    require_contest_staff(request, contest_id)
    _require_contest_mutation_open(contest_id)
    try:
        result = build_problem_package(contest_id, problem_id, payload.script_text)
    except PackageBuildError as error:
        raise AppError(422, "package_build_failed", str(error))
    except ValueError:
        raise not_found()
    _schedule_bundle_warm(background_tasks, contest_id, problem_id)
    return ok(request, result)
