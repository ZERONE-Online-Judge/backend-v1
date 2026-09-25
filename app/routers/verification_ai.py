from fastapi import APIRouter, Request
from fastapi.responses import Response
from pydantic import BaseModel, Field
from app.services.authz import has_contest_permission
from app.services.authz import require_contest_staff
from app.services.responses import ok
from app.services import verification_ai as service

router = APIRouter(tags=["operator"])


def require_access(request, cid, *, write=False):
    # Reports contain hidden tests and source details, so ordinary problem
    # reviewers/participants must never receive them through shared caching.
    staff = require_contest_staff(request, cid, "contest.problem.resource.view")
    if write:
        require_contest_staff(request, cid, "contest.problem.test")
    return staff


class VerificationTaskRequest(BaseModel):
    goal: str = Field(min_length=5, max_length=4000)
    source_asset_id: str | None = Field(default=None, max_length=36)
    parent_task_id: str | None = Field(default=None, max_length=36)


@router.get("/operator/contests/{contest_id}/problems/{problem_id}/verification-tasks")
def tasks(contest_id: str, problem_id: str, request: Request):
    from app.services import verification_tasks as tasks_service

    staff = require_access(request, contest_id)
    result = tasks_service.list_tasks(contest_id, problem_id)
    result["can_run"] = has_contest_permission(
        staff, contest_id, "contest.problem.test"
    )
    return ok(request, result)


@router.post("/operator/contests/{contest_id}/problems/{problem_id}/verification-tasks")
def create_task(
    contest_id: str, problem_id: str, payload: VerificationTaskRequest, request: Request
):
    from app.services import verification_tasks as tasks_service

    staff = require_access(request, contest_id, write=True)
    return ok(
        request,
        tasks_service.create(
            contest_id,
            problem_id,
            **payload.model_dump(),
            created_by=staff.staff_account_id
        ),
    )


@router.get(
    "/operator/contests/{contest_id}/problems/{problem_id}/verification-tasks/{task_id}"
)
def task_detail(contest_id: str, problem_id: str, task_id: str, request: Request):
    from app.services import verification_tasks as tasks_service

    require_access(request, contest_id)
    return ok(request, tasks_service.detail(contest_id, problem_id, task_id))


@router.post(
    "/operator/contests/{contest_id}/problems/{problem_id}/verification-tasks/{task_id}/stop"
)
def stop_task(contest_id: str, problem_id: str, task_id: str, request: Request):
    from app.services import verification_tasks as tasks_service

    require_access(request, contest_id, write=True)
    return ok(request, tasks_service.cancel(contest_id, problem_id, task_id))


@router.get(
    "/operator/contests/{contest_id}/problems/{problem_id}/verification-tasks/{task_id}/workspace.zip"
)
def task_workspace(contest_id: str, problem_id: str, task_id: str, request: Request):
    from app.services import verification_tasks as tasks_service

    require_access(request, contest_id)
    return Response(
        tasks_service.archive(contest_id, problem_id, task_id),
        media_type="application/zip",
        headers={
            "Content-Disposition": 'attachment; filename="verification-workspace.zip"'
        },
    )


@router.get("/operator/contests/{contest_id}/problems/{problem_id}/verification-runs")
def runs(contest_id: str, problem_id: str, request: Request):
    require_access(request, contest_id)
    return ok(request, service.list_runs(contest_id, problem_id))


@router.get(
    "/operator/contests/{contest_id}/problems/{problem_id}/verification-runs/{submission_id}/analysis"
)
def analysis(contest_id: str, problem_id: str, submission_id: str, request: Request):
    require_access(request, contest_id)
    return ok(request, service.analysis_detail(contest_id, problem_id, submission_id))


@router.post(
    "/operator/contests/{contest_id}/problems/{problem_id}/verification-runs/{submission_id}/analysis"
)
def request_analysis(
    contest_id: str, problem_id: str, submission_id: str, request: Request
):
    require_access(request, contest_id, write=True)
    return ok(request, service.request_analysis(contest_id, problem_id, submission_id))


@router.get(
    "/operator/contests/{contest_id}/problems/{problem_id}/verification-runs/{submission_id}/workspace.zip"
)
def workspace(contest_id: str, problem_id: str, submission_id: str, request: Request):
    from fastapi.responses import Response

    require_access(request, contest_id)
    return Response(
        service.workspace_archive(contest_id, problem_id, submission_id),
        media_type="application/zip",
        headers={
            "Content-Disposition": 'attachment; filename="verification-workspace.zip"'
        },
    )
