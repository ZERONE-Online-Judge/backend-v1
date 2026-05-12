from datetime import datetime, timedelta

from fastapi import APIRouter, Request
from pydantic import BaseModel, EmailStr

from app.models import ContestStatus, JudgeNode, now_utc
from app.settings import settings
from app.services.authz import require_service_master
from app.services.errors import AppError, not_found
from app.services.responses import ok, page
from app.services.store import store

router = APIRouter(tags=["admin"])


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


def _node_with_activity(node: JudgeNode) -> dict:
    active_since = now_utc() - timedelta(seconds=max(5, settings.judge_node_active_window_seconds))
    heartbeat_age = max(0, int((now_utc() - node.last_heartbeat_at).total_seconds()))
    payload = node.model_dump(mode="json")
    payload["is_active"] = node.last_heartbeat_at >= active_since
    payload["heartbeat_age_seconds"] = heartbeat_age
    return payload


class ContestCreateRequest(BaseModel):
    title: str | None = None
    organization_name: str
    overview: str | None = None
    status: ContestStatus = ContestStatus.SCHEDULE_TBD
    start_at: datetime | None = None
    end_at: datetime | None = None
    freeze_at: datetime | None = None
    operator_email: EmailStr | None = None


class ContestDivisionCreateRequest(BaseModel):
    code: str
    name: str
    description: str = ""
    display_order: int = 1


class ContestOperatorCreateRequest(BaseModel):
    email: EmailStr
    display_name: str | None = None


class ServiceNoticeCreateRequest(BaseModel):
    title: str
    summary: str
    body: str
    emergency: bool = False


class ServiceNoticeUpdateRequest(BaseModel):
    title: str | None = None
    summary: str | None = None
    body: str | None = None
    emergency: bool | None = None


@router.get("/admin/dashboard")
async def dashboard(request: Request):
    require_service_master(request)
    node_payloads = [_node_with_activity(node) for node in store.judge_nodes.values()]
    return ok(
        request,
        {
            "contest_count": len(store.contests),
            "pending_jobs": len([job for job in store.judge_jobs.values() if job.status == "pending"]),
            "mail_queue_pending": len([mail for mail in store.mail_queue.values() if mail.status == "pending"]),
            "judge_node_count": len(node_payloads),
            "active_judge_node_count": len([node for node in node_payloads if node["is_active"]]),
        },
    )


@router.get("/admin/contests")
async def admin_contests(request: Request):
    require_service_master(request)
    return page(request, [contest.model_dump(mode="json") for contest in store.contests.values()])


@router.post("/admin/contests")
async def create_contest(payload: ContestCreateRequest, request: Request):
    require_service_master(request)
    contest = store.create_contest(
        payload.title,
        payload.organization_name,
        payload.overview,
        payload.start_at,
        payload.end_at,
        payload.freeze_at,
        payload.status,
    )
    if payload.operator_email:
        try:
            store.upsert_contest_operator(contest.contest_id, str(payload.operator_email), str(payload.operator_email))
        except ValueError as exc:
            message = str(exc)
            if message.startswith("operator email cannot be participant email:"):
                raise AppError(422, "validation_error", message, {"field": "operator_email_conflict"})
            raise AppError(404, "not_found", message)
        store.enqueue_mail(
            "contest_operator_assigned",
            str(payload.operator_email),
            f"[Zerone OJ] {contest.title} operator assignment",
            "\n".join(
                [
                    f"You have been assigned as an operator for {contest.title}.",
                    f"Organization: {contest.organization_name}",
                    f"Status: {contest.status}",
                    f"Open at: {contest.start_at.isoformat()}",
                    "",
                    "Log in to the operator console to configure divisions, problems, and teams.",
                ]
            ),
        )
    return ok(request, contest.model_dump(mode="json"))


@router.post("/admin/contests/{contest_id}/divisions")
async def create_contest_division(contest_id: str, payload: ContestDivisionCreateRequest, request: Request):
    require_service_master(request)
    try:
        division = store.create_contest_division(contest_id, payload.code, payload.name, payload.description, payload.display_order)
    except ValueError as exc:
        raise AppError(404, "not_found", str(exc))
    return ok(request, division.model_dump(mode="json"))


@router.post("/admin/contests/{contest_id}/operators")
async def create_contest_operator(contest_id: str, payload: ContestOperatorCreateRequest, request: Request):
    require_service_master(request)
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
                    f"You have been assigned as an operator for {contest.title}.",
                    f"Organization: {contest.organization_name}",
                    f"Status: {contest.status}",
                    f"Open at: {contest.start_at.isoformat()}",
                ]
            ),
        )
    return ok(request, operator.model_dump(mode="json"))


@router.get("/admin/service-managers")
async def service_managers(request: Request):
    require_service_master(request)
    return page(request, [staff.model_dump(mode="json") for staff in store.staff_accounts.values()])


@router.get("/admin/service-notices")
async def admin_service_notices(request: Request):
    require_service_master(request)
    notices = [notice.model_dump(mode="json") for notice in store.service_notices.values()]
    notices.sort(key=lambda item: item.get("published_at", ""), reverse=True)
    return page(request, notices)


@router.post("/admin/service-notices")
async def create_service_notice(payload: ServiceNoticeCreateRequest, request: Request):
    require_service_master(request)
    notice = store.create_service_notice(payload.title, payload.summary, payload.body, payload.emergency)
    return ok(request, notice.model_dump(mode="json"))


@router.patch("/admin/service-notices/{notice_id}")
async def update_service_notice(notice_id: str, payload: ServiceNoticeUpdateRequest, request: Request):
    require_service_master(request)
    notice = store.update_service_notice(notice_id, **payload.model_dump(exclude_unset=True))
    if not notice:
        raise AppError(404, "not_found", "Service notice was not found.")
    return ok(request, notice.model_dump(mode="json"))


@router.get("/admin/judge/dashboard")
async def judge_dashboard(
    request: Request,
    include_queue: bool = False,
    status_filter: str = "pending,running",
    limit: int = 50,
    cursor: str | None = None,
):
    require_service_master(request)
    node_payloads = [_node_with_activity(node) for node in store.judge_nodes.values()]
    jobs = list(store.judge_jobs.values())
    queue_payload: list[dict] = []
    queue_page = {"limit": max(1, min(limit, 300)), "next_cursor": None}
    if include_queue:
        allowed = {token.strip() for token in status_filter.split(",") if token.strip()}
        if not allowed:
            allowed = {"pending", "running"}
        filtered = [job for job in jobs if job.status in allowed]
        filtered.sort(key=lambda item: item.created_at, reverse=True)
        queue_slice, next_cursor = _page_slice(filtered, limit, cursor)
        queue_payload = [job.model_dump(mode="json") for job in queue_slice]
        queue_page["next_cursor"] = next_cursor
    return ok(
        request,
        {
            "nodes": node_payloads,
            "queue": queue_payload,
            "queue_page": queue_page,
            "queue_stats": {
                "pending_count": len([job for job in jobs if job.status == "pending"]),
                "running_count": len([job for job in jobs if job.status == "running"]),
                "succeeded_count": len([job for job in jobs if job.status == "succeeded"]),
            },
        },
    )


@router.get("/admin/judge/submissions")
async def judge_submissions(request: Request, limit: int = 100, cursor: str | None = None, include_source: bool = False):
    require_service_master(request)
    contests = store.contests
    problems = store.problems
    divisions = store.divisions
    teams = store.teams
    jobs = store.judge_jobs
    nodes = store.judge_nodes
    testcase_sets = store.testcase_sets
    testcases = store.testcases

    testcase_by_set: dict[str, list] = {}
    for testcase in testcases.values():
        testcase_by_set.setdefault(testcase.testcase_set_id, []).append(testcase)
    for bucket in testcase_by_set.values():
        bucket.sort(key=lambda item: item.display_order)

    latest_all = sorted(
        store.submissions.values(),
        key=lambda item: item.submitted_at,
        reverse=True,
    )
    latest, next_cursor = _page_slice(latest_all, limit, cursor)

    job_by_submission_id = {job.submission_id: job for job in jobs.values()}
    data = []
    for submission in latest:
        problem = problems.get(submission.problem_id)
        contest = contests.get(submission.contest_id)
        division = divisions.get(submission.division_id)
        team = teams.get(submission.participant_team_id)
        member = next((item for item in (team.members if team else []) if item.team_member_id == submission.team_member_id), None)
        job = job_by_submission_id.get(submission.submission_id)
        node = nodes.get(job.assigned_node_id) if job and job.assigned_node_id else None
        active_set = next((item for item in testcase_sets.values() if item.problem_id == submission.problem_id and item.is_active), None)
        case_count = len(testcase_by_set.get(active_set.testcase_set_id, [])) if active_set else 0
        submission_payload = submission.model_dump(mode="json")
        if not include_source:
            submission_payload["source_code"] = None
        data.append(
            {
                "submission": submission_payload,
                "contest": {"contest_id": contest.contest_id, "title": contest.title} if contest else None,
                "division": {"division_id": division.division_id, "name": division.name} if division else None,
                "problem": {
                    "problem_id": problem.problem_id,
                    "problem_code": problem.problem_code,
                    "title": problem.title,
                    "time_limit_ms": problem.time_limit_ms,
                    "memory_limit_mb": problem.memory_limit_mb,
                    "max_score": problem.max_score,
                } if problem else None,
                "team": {"participant_team_id": team.participant_team_id, "team_name": team.team_name} if team else None,
                "member": {"team_member_id": member.team_member_id, "name": member.name, "email": member.email} if member else None,
                "judge_job": job.model_dump(mode="json") if job else None,
                "judge_node": node.model_dump(mode="json") if node else None,
                "active_testcase_count": case_count,
                "queue_position": job.queue_position if job else None,
            }
        )
    return page(
        request,
        data,
        next_cursor=next_cursor,
        limit=max(1, min(limit, 300)),
        total_count=len(latest_all),
        current_cursor=cursor,
    )


@router.get("/admin/judge/submissions/{submission_id}")
async def judge_submission_detail(submission_id: str, request: Request):
    require_service_master(request)
    submission = store.submissions.get(submission_id)
    if not submission:
        raise not_found()
    contests = store.contests
    problems = store.problems
    divisions = store.divisions
    teams = store.teams
    jobs = store.judge_jobs
    nodes = store.judge_nodes
    testcase_sets = store.testcase_sets
    testcases = store.testcases

    testcase_by_set: dict[str, list] = {}
    for testcase in testcases.values():
        testcase_by_set.setdefault(testcase.testcase_set_id, []).append(testcase)

    problem = problems.get(submission.problem_id)
    contest = contests.get(submission.contest_id)
    division = divisions.get(submission.division_id)
    team = teams.get(submission.participant_team_id)
    member = next((item for item in (team.members if team else []) if item.team_member_id == submission.team_member_id), None)
    job = next((item for item in jobs.values() if item.submission_id == submission.submission_id), None)
    node = nodes.get(job.assigned_node_id) if job and job.assigned_node_id else None
    active_set = next((item for item in testcase_sets.values() if item.problem_id == submission.problem_id and item.is_active), None)
    case_count = len(testcase_by_set.get(active_set.testcase_set_id, [])) if active_set else 0
    return ok(
        request,
        {
            "submission": submission.model_dump(mode="json"),
            "contest": {"contest_id": contest.contest_id, "title": contest.title} if contest else None,
            "division": {"division_id": division.division_id, "name": division.name} if division else None,
            "problem": {
                "problem_id": problem.problem_id,
                "problem_code": problem.problem_code,
                "title": problem.title,
                "time_limit_ms": problem.time_limit_ms,
                "memory_limit_mb": problem.memory_limit_mb,
                "max_score": problem.max_score,
            } if problem else None,
            "team": {"participant_team_id": team.participant_team_id, "team_name": team.team_name} if team else None,
            "member": {"team_member_id": member.team_member_id, "name": member.name, "email": member.email} if member else None,
            "judge_job": job.model_dump(mode="json") if job else None,
            "judge_node": node.model_dump(mode="json") if node else None,
            "active_testcase_count": case_count,
            "queue_position": job.queue_position if job else None,
        },
    )


@router.get("/admin/mail-queue")
async def mail_queue(request: Request):
    require_service_master(request)
    return page(request, [mail.model_dump(mode="json") for mail in store.mail_queue.values()])
