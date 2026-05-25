import asyncio

from fastapi import APIRouter, Request
from pydantic import BaseModel, EmailStr, Field

from app.models import ContestResourceAccess, ContestStatus, SubmissionStatus, now_utc
from app.services.authz import bearer_token, require_participant
from app.services.errors import AppError, authentication_required, invalid_state, not_found
from app.services.mail_templates import absolute_url, render_branded_email
from app.services.responses import ok, page
from app.services.store import OPERATOR_TEST_TEAM_PREFIX, store
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


def _sort_problems(items: list):
    return sorted(items, key=lambda item: (item.display_order, item.problem_code, item.title, item.problem_id))


def _problem_solve_statuses(contest_id: str, participant: dict | None) -> dict[str, str]:
    if not participant:
        return {}
    team_id = participant["team"].participant_team_id
    statuses: dict[str, str] = {}
    pending = {SubmissionStatus.WAITING, SubmissionStatus.PREPARING, SubmissionStatus.JUDGING}
    wrong = {
        SubmissionStatus.WRONG_ANSWER,
        SubmissionStatus.TIME_LIMIT_EXCEEDED,
        SubmissionStatus.MEMORY_LIMIT_EXCEEDED,
        SubmissionStatus.OUTPUT_LIMIT_EXCEEDED,
    }
    for submission in store.submissions.values():
        if submission.contest_id != contest_id or submission.participant_team_id != team_id:
            continue
        current = statuses.get(submission.problem_id)
        if current == "accepted":
            continue
        if submission.status == SubmissionStatus.ACCEPTED:
            statuses[submission.problem_id] = "accepted"
        elif submission.status in wrong:
            statuses[submission.problem_id] = "wrong"
        elif submission.status in pending and current is None:
            statuses[submission.problem_id] = "unsolved"
    return statuses


def _problem_payload(problem, solve_statuses: dict[str, str]) -> dict:
    item = problem.model_dump(mode="json")
    item["solve_status"] = solve_statuses.get(problem.problem_id, "unsolved")
    return item


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


class QuestionAnswerCreateRequest(BaseModel):
    body: str = Field(min_length=1)


def _is_ended(contest) -> bool:
    if contest.status == ContestStatus.SCHEDULE_TBD:
        return False
    return contest.status in {ContestStatus.ENDED, ContestStatus.FINALIZED, ContestStatus.ARCHIVED} or now_utc() >= contest.end_at


def _has_started(contest) -> bool:
    return now_utc() >= contest.start_at


def _optional_participant(request: Request, contest_id: str) -> dict | None:
    token = bearer_token(request)
    if not token:
        return None
    return (
        store.get_participant_by_access_token(contest_id, token)
        or store.get_participant_by_general_access_token(contest_id, token)
    )


def _require_contest_participant(request: Request, contest_id: str) -> dict:
    participant = _optional_participant(request, contest_id)
    if not participant:
        raise authentication_required("Participant access token is required.")
    return participant


def _allow_after_end_resource(contest, access: ContestResourceAccess, participant: dict | None) -> bool:
    if access == ContestResourceAccess.PUBLIC:
        return True
    if access == ContestResourceAccess.PARTICIPANTS:
        return participant is not None
    return False


def _allow_board_write_after_end(contest, participant: dict | None) -> bool:
    if not contest.board_write_after_end:
        return False
    return _allow_after_end_resource(contest, contest.board_access_after_end, participant)


def _allow_started_resource(contest, access: ContestResourceAccess, participant: dict | None) -> bool:
    if not _has_started(contest):
        return False
    if access == ContestResourceAccess.PUBLIC:
        return True
    return participant is not None


def _allow_visible_resource(access: ContestResourceAccess, participant: dict | None) -> bool:
    if access == ContestResourceAccess.PUBLIC:
        return True
    if access == ContestResourceAccess.PARTICIPANTS:
        return participant is not None
    return participant is not None


def _allow_problem_view(request: Request, contest_id: str, division_id: str | None = None) -> tuple[dict | None, object]:
    contest = store.get_public_contest(contest_id)
    if not contest:
        raise not_found()
    participant = _optional_participant(request, contest_id)
    if _is_ended(contest):
        if _allow_after_end_resource(contest, contest.problem_access_after_end, participant):
            return participant, contest
        raise not_found()
    if participant and division_id and participant["division"].division_id != division_id:
        raise not_found("Division is not available for this participant.")
    if _allow_started_resource(contest, contest.problem_access_after_end, participant):
        return participant, contest
    raise not_found()


def _allow_scoreboard_view(request: Request, contest_id: str, division_id: str | None = None) -> tuple[dict | None, object]:
    contest = store.get_public_contest(contest_id)
    if not contest:
        raise not_found()
    participant = _optional_participant(request, contest_id)
    if _is_ended(contest):
        if _allow_after_end_resource(contest, contest.scoreboard_access_after_end, participant):
            return participant, contest
        raise not_found()
    if participant and division_id and participant["division"].division_id != division_id:
        raise not_found("Division is not available for this participant.")
    if _allow_started_resource(contest, contest.scoreboard_access_after_end, participant):
        return participant, contest
    raise not_found()


def _allow_submission_list_view(request: Request, contest_id: str) -> tuple[dict | None, object]:
    contest = store.get_public_contest(contest_id)
    if not contest:
        raise not_found()
    participant = _optional_participant(request, contest_id)
    if _is_ended(contest):
        if _allow_after_end_resource(contest, contest.submission_access_after_end, participant):
            return participant, contest
        raise not_found()
    if _allow_started_resource(contest, contest.submission_access_after_end, participant):
        return participant, contest
    raise not_found()


def _participant_submission_payload(
    submission,
    include_source: bool,
    queue_position: int | None = None,
    source_code_length: int | None = None,
) -> dict:
    item = submission.model_dump(mode="json")
    item["queue_position"] = queue_position
    if item["queue_position"] is None:
        item["queue_position"] = store.pending_queue_ranks().get(submission.submission_id)
    source_code = item.get("source_code") or ""
    item["source_code_length"] = source_code_length if source_code_length is not None else len(source_code.encode("utf-8"))
    progress_current = item.get("progress_current")
    progress_total = item.get("progress_total")
    item["progress_percent"] = None
    if isinstance(progress_current, int) and isinstance(progress_total, int) and progress_total > 0:
        item["progress_percent"] = max(0, min(100, round((progress_current / progress_total) * 100)))
    item["progress_current"] = None
    item["progress_total"] = None
    # Participants should not receive internal judge diagnostics/logs.
    item["compile_message"] = None
    item["judge_message"] = None
    if not include_source:
        item["source_code"] = ""
    return item


def _is_operator_test_submission(submission) -> bool:
    team = store.teams.get(submission.participant_team_id)
    return bool(team and team.team_name.startswith(OPERATOR_TEST_TEAM_PREFIX))


def _mock_submission_payload(submission) -> dict:
    item = _participant_submission_payload(submission, include_source=False)
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
    problems = _sort_problems([p for p in store.problems.values() if p.contest_id == contest_id and p.division_id == division.division_id])
    solve_statuses = _problem_solve_statuses(contest_id, participant)
    return ok(
        request,
        {
            "contest": contest.model_dump(mode="json"),
            "division": division.model_dump(mode="json"),
            "divisions": [item.model_dump(mode="json") for item in divisions],
            "problems": [_problem_payload(p, solve_statuses) for p in problems],
            "emergency_notice": contest.emergency_notice,
        },
    )


@router.get("/contests/{contest_id}/divisions/{division_id}/workspace")
async def division_workspace(contest_id: str, division_id: str, request: Request):
    participant, contest = _allow_problem_view(request, contest_id, division_id)
    division = store.get_division(contest_id, division_id)
    if not division:
        raise not_found()
    problems = _sort_problems([p for p in store.problems.values() if p.contest_id == contest_id and p.division_id == division_id])
    solve_statuses = _problem_solve_statuses(contest_id, participant)
    return ok(
        request,
        {
            "contest": contest.model_dump(mode="json"),
            "division": division.model_dump(mode="json"),
            "problems": [_problem_payload(p, solve_statuses) for p in problems],
            "emergency_notice": contest.emergency_notice,
        },
    )


@router.get("/contests/{contest_id}/problems")
async def problems(contest_id: str, request: Request):
    participant, _ = _allow_problem_view(request, contest_id)
    divisions = store.contest_divisions(contest_id)
    division_id = participant["division"].division_id if participant else (divisions[0].division_id if divisions else None)
    solve_statuses = _problem_solve_statuses(contest_id, participant)
    return page(
        request,
        [_problem_payload(p, solve_statuses) for p in _sort_problems([p for p in store.problems.values() if p.contest_id == contest_id and p.division_id == division_id])],
    )


@router.get("/contests/{contest_id}/divisions/{division_id}/problems")
async def division_problems(contest_id: str, division_id: str, request: Request):
    participant, _ = _allow_problem_view(request, contest_id, division_id)
    if not store.get_division(contest_id, division_id):
        raise not_found()
    solve_statuses = _problem_solve_statuses(contest_id, participant)
    return page(
        request,
        [_problem_payload(p, solve_statuses) for p in _sort_problems([p for p in store.problems.values() if p.contest_id == contest_id and p.division_id == division_id])],
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


@router.post("/contests/{contest_id}/problems/{problem_id}/mock-submissions")
async def create_mock_submission(contest_id: str, problem_id: str, payload: SubmissionCreateRequest, request: Request):
    problem = store.problems.get(problem_id)
    if not problem or problem.contest_id != contest_id:
        raise not_found()
    participant, contest = _allow_problem_view(request, contest_id, problem.division_id)
    if not _is_ended(contest) or not contest.mock_judging_enabled:
        raise not_found()
    if contest.problem_access_after_end == ContestResourceAccess.PRIVATE:
        raise not_found()
    if contest.problem_access_after_end == ContestResourceAccess.PARTICIPANTS and not participant:
        raise authentication_required("Participant access token is required.")
    if payload.language not in {"c99", "cpp17", "python313", "java8"}:
        raise AppError(422, "validation_error", "Unsupported language.", {"fields": [{"path": "body.language", "code": "invalid_enum"}]})
    if not payload.source_code.strip():
        raise AppError(422, "validation_error", "Source code is required.")
    try:
        submission = store.create_operator_test_submission(contest_id, problem_id, payload.language, payload.source_code)
    except ValueError:
        raise not_found()
    return ok(request, _mock_submission_payload(submission))


@router.get("/contests/{contest_id}/mock-submissions/{submission_id}/status:wait")
async def wait_mock_submission_status(
    contest_id: str,
    submission_id: str,
    request: Request,
    wait_seconds: float = 2.0,
    poll_interval_seconds: float = 0.25,
):
    submission = store.get_submission(submission_id, include_source=False)
    if not submission or submission.contest_id != contest_id or not _is_operator_test_submission(submission):
        raise not_found()
    problem = store.problems.get(submission.problem_id)
    if not problem:
        raise not_found()
    participant, contest = _allow_problem_view(request, contest_id, problem.division_id)
    if not _is_ended(contest) or not contest.mock_judging_enabled:
        raise not_found()
    if contest.problem_access_after_end == ContestResourceAccess.PARTICIPANTS and not participant:
        raise authentication_required("Participant access token is required.")
    wait_budget = max(0.0, min(wait_seconds, 10.0))
    poll = max(0.1, min(poll_interval_seconds, 1.0))
    loops = max(1, int(wait_budget / poll))
    for _ in range(loops):
        updated = store.get_submission(submission_id, include_source=False)
        if not updated:
            raise not_found()
        if updated.status not in {"waiting", "preparing", "judging"}:
            full = store.get_submission(submission_id)
            if not full:
                raise not_found()
            return ok(request, _mock_submission_payload(full))
        await asyncio.sleep(poll)
    latest = store.get_submission(submission_id)
    if not latest:
        raise not_found()
    return ok(request, _mock_submission_payload(latest))


@router.get("/contests/{contest_id}/notices")
async def contest_notices(contest_id: str, request: Request):
    contest = store.get_public_contest(contest_id)
    if not contest:
        raise not_found()
    participant = _optional_participant(request, contest_id)
    ended = _is_ended(contest)
    if ended and not _allow_visible_resource(contest.notice_access_after_end, participant):
        raise not_found()
    include_participant_visible = (
        ended
        and contest.notice_access_after_end == ContestResourceAccess.PUBLIC
    )
    return page(
        request,
        [
            notice.model_dump(mode="json")
            for notice in store.contest_notices_for_view(
                contest_id,
                participant,
                include_participant_visible=include_participant_visible,
            )
        ],
    )


@router.get("/contests/{contest_id}/boards")
async def contest_board(contest_id: str, request: Request):
    contest = store.get_public_contest(contest_id)
    if not contest:
        raise not_found()
    participant = _optional_participant(request, contest_id)
    if _is_ended(contest) and not _allow_visible_resource(contest.board_access_after_end, participant):
        raise not_found()
    return page(request, [question.model_dump(mode="json") for question in store.questions_for_view(contest_id, participant)])


@router.post("/contests/{contest_id}/boards")
async def create_question(contest_id: str, payload: QuestionCreateRequest, request: Request):
    participant = require_participant(request, contest_id)
    contest = store.get_public_contest(contest_id)
    if not contest:
        raise not_found()
    if _is_ended(contest) and not _allow_board_write_after_end(contest, participant):
        raise not_found()
    if payload.visibility not in {"public", "private"}:
        raise AppError(422, "validation_error", "Unsupported question visibility.")
    question = store.create_question(contest_id, participant, payload.title, payload.body, payload.visibility)
    if contest:
        operator_accounts = [
            account
            for account in store.contest_operator_accounts(contest_id)
            if not account.is_service_master
        ]
        subject = f"[ZOJ] 새 질문 · {contest.title}"
        question_url = absolute_url(f"/operator/contests/{contest_id}/board?questionId={question.contest_question_id}")
        body_lines = [
            f"대회: {contest.title}",
            f"유형: {participant['division'].name}",
            f"팀: {participant['team'].team_name}",
            f"작성자: {participant['member'].name} <{participant['member'].email}>",
            f"공개 범위: {question.visibility}",
            f"제목: {question.title}",
            "",
            "질문 본문:",
            question.body,
            "",
            f"바로가기: {question_url}",
        ]
        notified = set()
        for account in operator_accounts:
            email = str(account.email).strip().lower()
            if not email or email in notified:
                continue
            notified.add(email)
            store.enqueue_mail(
                "contest_question_created",
                email,
                subject,
                "\n".join(body_lines),
                render_branded_email(
                    title="새 질문이 등록되었습니다",
                    preheader=question.title,
                    body=[f"{contest.title}에 새 질문이 등록되었습니다."],
                    meta=[
                        ("대회", contest.title),
                        ("유형", participant["division"].name),
                        ("팀", participant["team"].team_name),
                        ("작성자", f"{participant['member'].name} <{participant['member'].email}>"),
                        ("공개 범위", question.visibility),
                        ("제목", question.title),
                    ],
                    sections=[("질문 본문", question.body)],
                    button_label="질문 확인하기",
                    button_url=question_url,
                ),
            )
    return ok(request, question.model_dump(mode="json"))


@router.post("/contests/{contest_id}/boards/{question_id}/answers")
async def create_question_answer(
    contest_id: str,
    question_id: str,
    payload: QuestionAnswerCreateRequest,
    request: Request,
):
    participant = require_participant(request, contest_id)
    contest = store.get_public_contest(contest_id)
    if not contest:
        raise not_found()
    if _is_ended(contest) and not _allow_board_write_after_end(contest, participant):
        raise not_found()

    question = store.get_contest_question(contest_id, question_id)
    if not question:
        raise not_found()
    if question.visibility == "private" and question.participant_team_id != participant["team"].participant_team_id:
        raise not_found()

    body = payload.body.strip()
    if not body:
        raise AppError(422, "validation_error", "Answer body is required.")

    answer = store.create_answer(
        contest_id,
        question_id,
        body,
        "public",
        str(participant["member"].email),
    )
    if not answer:
        raise not_found()
    return ok(request, answer.model_dump(mode="json"))


@router.get("/contests/{contest_id}/submissions")
async def submissions(
    contest_id: str,
    request: Request,
    limit: int = 100,
    cursor: str | None = None,
    include_source: bool = False,
    division_id: str | None = None,
    problem_id: str | None = None,
):
    participant, contest = _allow_submission_list_view(request, contest_id)
    queue_ranks = store.pending_queue_ranks(contest_id=contest_id)
    if not participant or _is_ended(contest):
        submissions, next_cursor, total_count = store.list_submissions(
            contest_id=contest_id,
            division_id=division_id,
            problem_id=problem_id,
            exclude_operator_tests=True,
            include_source=False,
            limit=limit,
            cursor=cursor,
        )
        source_lengths = store.submission_source_lengths([submission.submission_id for submission in submissions])
        items = [
            _participant_submission_payload(
                submission,
                include_source=False,
                queue_position=queue_ranks.get(submission.submission_id),
                source_code_length=source_lengths.get(submission.submission_id),
            )
            for submission in submissions
        ]
        return page(
            request,
            items,
            next_cursor=next_cursor,
            limit=max(1, min(limit, 300)),
            total_count=total_count,
            current_cursor=cursor,
        )
    submissions, next_cursor, total_count = store.list_submissions(
        contest_id=contest_id,
        problem_id=problem_id,
        participant_team_id=participant["team"].participant_team_id,
        include_source=include_source,
        limit=limit,
        cursor=cursor,
    )
    source_lengths = store.submission_source_lengths([submission.submission_id for submission in submissions])
    items = [
        _participant_submission_payload(
            submission,
            include_source=include_source,
            queue_position=queue_ranks.get(submission.submission_id),
            source_code_length=source_lengths.get(submission.submission_id),
        )
        for submission in submissions
    ]
    return page(
        request,
        items,
        next_cursor=next_cursor,
        limit=max(1, min(limit, 300)),
        total_count=total_count,
        current_cursor=cursor,
    )


@router.get("/contests/{contest_id}/submissions/{submission_id}")
async def submission_detail(contest_id: str, submission_id: str, request: Request):
    participant = _require_contest_participant(request, contest_id)
    submission = store.get_submission(submission_id)
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
    participant = _require_contest_participant(request, contest_id)
    submission = store.get_submission(submission_id, include_source=False)
    if not submission or submission.contest_id != contest_id or submission.participant_team_id != participant["team"].participant_team_id:
        raise not_found()
    wait_budget = max(0.0, min(wait_seconds, 10.0))
    poll = max(0.1, min(poll_interval_seconds, 1.0))
    loops = max(1, int(wait_budget / poll))
    for _ in range(loops):
        updated = store.get_submission(submission_id, include_source=False)
        if not updated:
            raise not_found()
        if updated.status not in {"waiting", "preparing", "judging"}:
            full = store.get_submission(submission_id)
            if not full:
                raise not_found()
            return ok(request, _participant_submission_payload(full, include_source=True))
        await asyncio.sleep(poll)
    latest = store.get_submission(submission_id)
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
