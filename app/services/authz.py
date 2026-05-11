from fastapi import Request

from app.models import StaffAccount
from app.services.errors import authentication_required, permission_denied, scope_denied
from app.services.store import store


def bearer_token(request: Request) -> str | None:
    authorization = request.headers.get("authorization", "")
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token:
        return None
    return token


def require_staff(request: Request) -> StaffAccount:
    token = bearer_token(request)
    account = store.get_staff_by_access_token(token) if token else None
    if not account:
        raise authentication_required("Staff access token is required.")
    return account


def require_service_master(request: Request) -> StaffAccount:
    account = require_staff(request)
    if not account.is_service_master:
        raise permission_denied("Service master permission is required.")
    return account


def require_contest_staff(request: Request, contest_id: str) -> StaffAccount:
    account = require_staff(request)
    if account.is_service_master:
        return account
    scopes = account.contest_scopes.get(contest_id, [])
    if "contest.*" in scopes:
        return account
    raise scope_denied()


def require_participant(request: Request, contest_id: str) -> dict:
    token = bearer_token(request)
    session = store.get_participant_by_access_token(contest_id, token) if token else None
    if not session:
        raise authentication_required("Participant access token is required.")
    return session
