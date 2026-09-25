from fastapi import APIRouter, Request
from app.services.authz import require_contest_staff
from app.services.responses import ok
from app.services import verification_ai as service

router = APIRouter(tags=["operator"])


def require_access(request, cid, *, write=False):
    # Reports contain hidden tests and source details, so ordinary problem
    # reviewers/participants must never receive them through shared caching.
    require_contest_staff(request, cid, "contest.problem.resource.view")
    if write:
        require_contest_staff(request, cid, "contest.problem.test")


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
