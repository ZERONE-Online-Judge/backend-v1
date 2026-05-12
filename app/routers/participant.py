import asyncio

from fastapi import APIRouter, Request
from pydantic import BaseModel, EmailStr, Field

from app.models import ContestStatus, now_utc
from app.services.authz import bearer_token, require_participant
from app.services.errors import AppError, invalid_state, not_found
from app.services.responses import ok, page
from app.services.store import store
from app.services.storage import object_storage
from app.settings import settings

router = APIRouter(tags=["participant"])


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


class OtpRequest(BaseModel):
    email: EmailStr


class OtpVerifyRequest(BaseModel):
    email: EmailStr
    otp_code: str = ""


class SubmissionCreateRequest(BaseModel):
    language: str
    source_code: str = Field(min_length=1, max_length=524288)


class QuestionCreateRequest(BaseModel):
    title: str = Field(min_length=1, max_length=255)
    body: str = Field(min_length=1)
    visibility: str = "public"


def _is_ended(contest) -> bool:
    return contest.status in {ContestStatus.ENDED, ContestStatus.ARCHIVED} or now_utc() >= contest.end_at


def _has_started(contest) -> bool:
    return now_utc() >= contest.start_at


def _optional_participant(request: Request, contest_id: str) -> dict | None:
    token = bearer_token(request)
    return store.get_participant_by_access_token(contest_id, token) if token else None


def _allow_problem_view(request: Request, contest_id: str, division_id: str | None = None) -> tuple[dict | None, object]:
    contest = store.get_public_contest(contest_id)
    if not contest:
        raise not_found()
    participant = _optional_participant(request, contest_id)
    if participant:
        if division_id and participant["division"].division_id != division_id:
            raise not_found("Division is not available for this participant.")
        if not _has_started(contest):
            raise not_found()
        return participant, contest
    if _is_ended(contest) and contest.problem_public_after_end:
        return None, contest
    raise not_found()


def _allow_scoreboard_view(request: Request, contest_id: str, division_id: str | None = None) -> tuple[dict | None, object]:
    contest = store.get_public_contest(contest_id)
    if not contest:
        raise not_found()
    participant = _optional_participant(request, contest_id)
    if participant:
        if division_id and participant["division"].division_id != division_id:
            raise not_found("Division is not available for this participant.")
        return participant, contest
    if _is_ended(contest) and contest.scoreboard_public_after_end:
        return None, contest
    raise not_found()


def _allow_submission_list_view(request: Request, contest_id: str) -> tuple[dict | None, object]:
    contest = store.get_public_contest(contest_id)
    if not contest:
        raise not_found()
    participant = _optional_participant(request, contest_id)
    if participant:
        return participant, contest
    if _is_ended(contest) and contest.submission_public_after_end:
        return None, contest
    raise not_found()


def _participant_submission_payload(submission, include_source: bool) -> dict:
    item = submission.model_dump(mode="json")
    # Participants should not receive internal judge diagnostics/logs.
    item["compile_message"] = None
    item["judge_message"] = None
    if not include_source:
        item["source_code"] = ""
    return item


@router.post("/contests/{contest_id}/participant-login/otp/request")
async def request_otp(contest_id: str, payload: OtpRequest, request: Request):
    contest = store.get_public_contest(contest_id)
    if not contest:
        raise not_found()
    team = store.get_team_by_email(contest_id, str(payload.email))
    if not team:
        raise not_found("Participant email is not registered.")
    retry_after = store.participant_otp_retry_after_seconds(contest_id, str(payload.email))
    if retry_after > 0:
        raise AppError(
            429,
            "otp_request_rate_limited",
            f"Please wait {retry_after} seconds before requesting another verification code.",
            {"retry_after_seconds": retry_after},
        )
    code = store.create_otp(contest_id, str(payload.email))
    data = {"sent": True, "delivery": "email", "cooldown_seconds": settings.otp_request_cooldown_seconds}
    if settings.allow_empty_otp:
        data["demo_otp"] = code
    return ok(request, data)


@router.post("/contests/{contest_id}/participant-login/otp/verify")
async def verify_otp(contest_id: str, payload: OtpVerifyRequest, request: Request):
    verified = store.verify_otp(contest_id, str(payload.email), payload.otp_code)
    if not verified:
        raise AppError(401, "invalid_credentials", "Invalid OTP.")
    team, member, division, access_token = verified
    return ok(
        request,
        {
            "access_token": access_token,
            "team": team.model_dump(mode="json"),
            "member": member.model_dump(mode="json"),
            "division": division.model_dump(mode="json"),
            "workspace_path": f"/contests/{contest_id}/divisions/{team.division_id}/workspace",
        },
    )


@router.get("/contests/{contest_id}/participant-session/me")
async def participant_me(contest_id: str, request: Request):
    token = bearer_token(request)
    session = store.get_participant_by_access_token(contest_id, token) if token else None
    if not session:
        raise AppError(401, "authentication_required", "Participant access token is required.")
    return ok(
        request,
        {
            "team": session["team"].model_dump(mode="json"),
            "member": session["member"].model_dump(mode="json"),
            "division": session["division"].model_dump(mode="json"),
        },
    )


@router.get("/contests/{contest_id}/workspace")
async def workspace(contest_id: str, request: Request, team_member_email: str | None = None):
    participant, contest = _allow_problem_view(request, contest_id)
    divisions = store.contest_divisions(contest_id)
    division = participant["division"] if participant else (divisions[0] if divisions else None)
    if not division:
        raise not_found("Contest division is not configured.")
    problems = [p for p in store.problems.values() if p.contest_id == contest_id and p.division_id == division.division_id]
    return ok(
        request,
        {
            "contest": contest.model_dump(mode="json"),
            "division": division.model_dump(mode="json"),
            "divisions": [item.model_dump(mode="json") for item in divisions],
            "problems": [p.model_dump(mode="json") for p in problems],
            "emergency_notice": contest.emergency_notice,
        },
    )


@router.get("/contests/{contest_id}/divisions/{division_id}/workspace")
async def division_workspace(contest_id: str, division_id: str, request: Request):
    _, contest = _allow_problem_view(request, contest_id, division_id)
    division = store.get_division(contest_id, division_id)
    if not division:
        raise not_found()
    problems = [p for p in store.problems.values() if p.contest_id == contest_id and p.division_id == division_id]
    return ok(
        request,
        {
            "contest": contest.model_dump(mode="json"),
            "division": division.model_dump(mode="json"),
            "problems": [p.model_dump(mode="json") for p in problems],
            "emergency_notice": contest.emergency_notice,
        },
    )


@router.get("/contests/{contest_id}/problems")
async def problems(contest_id: str, request: Request):
    participant, _ = _allow_problem_view(request, contest_id)
    divisions = store.contest_divisions(contest_id)
    division_id = participant["division"].division_id if participant else (divisions[0].division_id if divisions else None)
    return page(
        request,
        [p.model_dump(mode="json") for p in store.problems.values() if p.contest_id == contest_id and p.division_id == division_id],
    )


@router.get("/contests/{contest_id}/divisions/{division_id}/problems")
async def division_problems(contest_id: str, division_id: str, request: Request):
    _allow_problem_view(request, contest_id, division_id)
    if not store.get_division(contest_id, division_id):
        raise not_found()
    return page(
        request,
        [p.model_dump(mode="json") for p in store.problems.values() if p.contest_id == contest_id and p.division_id == division_id],
    )


@router.get("/contests/{contest_id}/problems/{problem_id}")
async def problem_detail(contest_id: str, problem_id: str, request: Request):
    problem = store.problems.get(problem_id)
    if not problem or problem.contest_id != contest_id:
        raise not_found()
    _allow_problem_view(request, contest_id, problem.division_id)
    return ok(request, problem.model_dump(mode="json"))


@router.get("/contests/{contest_id}/problems/{problem_id}/assets")
async def problem_assets(contest_id: str, problem_id: str, request: Request):
    problem = store.problems.get(problem_id)
    if not problem or problem.contest_id != contest_id:
        raise not_found()
    _allow_problem_view(request, contest_id, problem.division_id)
    try:
        assets = [
            asset
            for asset in store.problem_assets_for_problem(contest_id, problem_id)
            if "/package-files/" not in asset.storage_key
        ]
    except ValueError:
        raise not_found()
    return page(
        request,
        [{**asset.model_dump(mode="json"), "download_url": object_storage.presigned_get_url(asset.storage_key)} for asset in assets],
    )


@router.post("/contests/{contest_id}/problems/{problem_id}/submissions")
async def create_submission(contest_id: str, problem_id: str, payload: SubmissionCreateRequest, request: Request):
    participant = require_participant(request, contest_id)
    contest = store.contests.get(contest_id)
    if not contest:
        raise not_found()
    if contest.status != ContestStatus.RUNNING or now_utc() >= contest.end_at:
        raise invalid_state("Contest is not accepting submissions.")
    if payload.language not in {"c99", "cpp17", "python313", "java8"}:
        raise AppError(422, "validation_error", "Unsupported language.", {"fields": [{"path": "body.language", "code": "invalid_enum"}]})
    try:
        submission = store.create_submission(contest_id, problem_id, str(participant["member"].email), payload.language, payload.source_code)
    except ValueError as error:
        if "division mismatch" in str(error):
            raise not_found("Problem is not available for this participant division.")
        raise not_found("Participant email or problem is not registered.")
    return ok(request, submission.model_dump(mode="json"))


@router.get("/contests/{contest_id}/notices")
async def contest_notices(contest_id: str, request: Request):
    contest = store.get_public_contest(contest_id)
    if not contest:
        raise not_found()
    participant = _optional_participant(request, contest_id)
    return page(request, [notice.model_dump(mode="json") for notice in store.contest_notices_for_view(contest_id, participant)])


@router.get("/contests/{contest_id}/boards")
async def contest_board(contest_id: str, request: Request):
    contest = store.get_public_contest(contest_id)
    if not contest:
        raise not_found()
    participant = _optional_participant(request, contest_id)
    return page(request, [question.model_dump(mode="json") for question in store.questions_for_view(contest_id, participant)])


@router.post("/contests/{contest_id}/boards")
async def create_question(contest_id: str, payload: QuestionCreateRequest, request: Request):
    participant = require_participant(request, contest_id)
    if payload.visibility not in {"public", "private"}:
        raise AppError(422, "validation_error", "Unsupported question visibility.")
    question = store.create_question(contest_id, participant, payload.title, payload.body, payload.visibility)
    contest = store.contests.get(contest_id)
    if contest:
        operator_accounts = [
            account
            for account in store.contest_operator_accounts(contest_id)
            if not account.is_service_master
        ]
        subject = f"[Zerone OJ] 새 질문 · {contest.title}"
        body_lines = [
            f"Contest: {contest.title}",
            f"Division: {participant['division'].name}",
            f"Team: {participant['team'].team_name}",
            f"Author: {participant['member'].name} <{participant['member'].email}>",
            f"Visibility: {question.visibility}",
            f"Title: {question.title}",
            "",
            question.body,
        ]
        notified = set()
        for account in operator_accounts:
            email = str(account.email).strip().lower()
            if not email or email in notified:
                continue
            notified.add(email)
            store.enqueue_mail("contest_question_created", email, subject, "\n".join(body_lines))
    return ok(request, question.model_dump(mode="json"))


@router.get("/contests/{contest_id}/submissions")
async def submissions(contest_id: str, request: Request, limit: int = 100, cursor: str | None = None):
    participant, _ = _allow_submission_list_view(request, contest_id)
    if not participant:
        items = []
        for submission in store.submissions.values():
            if submission.contest_id == contest_id:
                items.append(_participant_submission_payload(submission, include_source=False))
        items.sort(key=lambda item: item.get("submitted_at", ""), reverse=True)
        sliced, next_cursor = _page_slice(items, limit, cursor)
        return page(request, sliced, next_cursor=next_cursor, limit=max(1, min(limit, 300)))
    items = [
        _participant_submission_payload(s, include_source=True)
        for s in store.submissions.values()
        if s.contest_id == contest_id and s.participant_team_id == participant["team"].participant_team_id
    ]
    items.sort(key=lambda item: item.get("submitted_at", ""), reverse=True)
    sliced, next_cursor = _page_slice(items, limit, cursor)
    return page(request, sliced, next_cursor=next_cursor, limit=max(1, min(limit, 300)))


@router.get("/contests/{contest_id}/submissions/{submission_id}")
async def submission_detail(contest_id: str, submission_id: str, request: Request):
    participant = require_participant(request, contest_id)
    submission = store.submissions.get(submission_id)
    if not submission or submission.contest_id != contest_id or submission.participant_team_id != participant["team"].participant_team_id:
        raise not_found()
    return ok(request, _participant_submission_payload(submission, include_source=True))


@router.get("/contests/{contest_id}/submissions/{submission_id}/status:wait")
async def wait_submission_status(
    contest_id: str,
    submission_id: str,
    request: Request,
    wait_seconds: float = 2.0,
    poll_interval_seconds: float = 0.25,
):
    participant = require_participant(request, contest_id)
    submission = store.submissions.get(submission_id)
    if not submission or submission.contest_id != contest_id or submission.participant_team_id != participant["team"].participant_team_id:
        raise not_found()
    wait_budget = max(0.0, min(wait_seconds, 10.0))
    poll = max(0.1, min(poll_interval_seconds, 1.0))
    loops = max(1, int(wait_budget / poll))
    for _ in range(loops):
        updated = store.submissions.get(submission_id)
        if not updated:
            raise not_found()
        if updated.status not in {"waiting", "preparing", "judging"}:
            return ok(request, _participant_submission_payload(updated, include_source=True))
        await asyncio.sleep(poll)
    latest = store.submissions.get(submission_id)
    if not latest:
        raise not_found()
    return ok(request, _participant_submission_payload(latest, include_source=True))


@router.get("/contests/{contest_id}/scoreboard")
async def scoreboard(contest_id: str, request: Request):
    participant, _ = _allow_scoreboard_view(request, contest_id)
    divisions = store.contest_divisions(contest_id)
    division = participant["division"] if participant else (divisions[0] if divisions else None)
    if not division:
        raise not_found("Contest division is not configured.")
    board = store.scoreboard_rows(contest_id, division.division_id, public_view=True)
    if not board:
        raise not_found()
    return ok(request, {"division": division.model_dump(mode="json"), **board})


@router.get("/contests/{contest_id}/scoreboard:wait")
async def wait_scoreboard(contest_id: str, request: Request, wait_seconds: float = 2.0):
    await asyncio.sleep(max(0, min(wait_seconds, 10.0)))
    return await scoreboard(contest_id, request)


@router.get("/contests/{contest_id}/divisions/{division_id}/scoreboard")
async def division_scoreboard(contest_id: str, division_id: str, request: Request):
    _allow_scoreboard_view(request, contest_id, division_id)
    division = store.get_division(contest_id, division_id)
    if not division:
        raise not_found()
    board = store.scoreboard_rows(contest_id, division_id, public_view=True)
    if not board:
        raise not_found()
    return ok(request, {"division": division.model_dump(mode="json"), **board})


@router.get("/contests/{contest_id}/divisions/{division_id}/scoreboard:wait")
async def wait_division_scoreboard(contest_id: str, division_id: str, request: Request, wait_seconds: int = 5):
    await asyncio.sleep(max(0, min(wait_seconds, 10)))
    return await division_scoreboard(contest_id, division_id, request)
