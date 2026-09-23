from fastapi import APIRouter, Request, Response
from pydantic import BaseModel, EmailStr

from app.services import presentation_access
from app.services.access_logging import write_access_log
from app.services.authz import bearer_token, require_contest_staff
from app.services.presentation_board import presentation_board
from app.services.responses import ok

router = APIRouter(tags=["presentation"])


class PresentationLoginRequest(BaseModel):
    email: EmailStr


def private_response(response: Response):
    response.headers["Cache-Control"] = "no-store"


@router.get("/operator/contests/{contest_id}/scoreboard/presentation-account")
async def get_account(contest_id: str, request: Request, response: Response):
    require_contest_staff(request, contest_id, "contest.scoreboard.manage")
    private_response(response)
    return ok(request, presentation_access.get_account(contest_id))


@router.put("/operator/contests/{contest_id}/scoreboard/presentation-account")
async def issue_account(contest_id: str, request: Request, response: Response):
    require_contest_staff(request, contest_id, "contest.scoreboard.manage")
    private_response(response)
    return ok(request, presentation_access.issue_account(contest_id))


@router.delete("/operator/contests/{contest_id}/scoreboard/presentation-account")
async def revoke_account(contest_id: str, request: Request):
    require_contest_staff(request, contest_id, "contest.scoreboard.manage")
    presentation_access.revoke_account(contest_id)
    return ok(request, {"revoked": True})


@router.post("/auth/presentation/login")
async def presentation_login(payload: PresentationLoginRequest, request: Request, response: Response):
    private_response(response)
    session = presentation_access.login(str(payload.email), request.headers.get("x-real-ip") or (request.client.host if request.client else "unknown"))
    # Never log the alias: it is the credential, not an ordinary account email.
    write_access_log(request, event_type="presentation_login", account_scope="presentation",
                     contest_id=session["contest_id"], actor_role="presentation")
    return ok(request, session)


@router.get("/presentation/contests/{contest_id}/scoreboard")
async def display_scoreboard(contest_id: str, request: Request, response: Response):
    private_response(response)
    presentation_access.require_presentation(bearer_token(request), contest_id)
    return ok(request, presentation_board(contest_id, display_only=True))
