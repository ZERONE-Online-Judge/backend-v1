from typing import Any

from fastapi import Request

from app.services.store import store


def client_ip(request: Request) -> str | None:
    forwarded_for = request.headers.get("x-forwarded-for")
    if forwarded_for:
        return forwarded_for.split(",", 1)[0].strip()
    return request.client.host if request.client else None


def general_role(session: dict | None) -> str | None:
    if not session:
        return None
    operator_session = session.get("operator_session")
    if isinstance(operator_session, dict):
        staff = operator_session.get("staff")
        if isinstance(staff, dict):
            return "service_master" if staff.get("is_service_master") else "operator"
    if session.get("participant_contests"):
        return "participant"
    return "general"


def write_access_log(
    request: Request,
    *,
    event_type: str,
    account_scope: str,
    email: str | None = None,
    display_name: str | None = None,
    contest_id: str | None = None,
    participant_team_id: str | None = None,
    team_name: str | None = None,
    team_member_id: str | None = None,
    member_name: str | None = None,
    actor_role: str | None = None,
    details: dict[str, Any] | None = None,
) -> None:
    try:
        contest = store.contests.get(contest_id) if contest_id else None
        store.append_access_log(
            event_type=event_type,
            account_scope=account_scope,
            email=email,
            display_name=display_name,
            contest_id=contest_id,
            contest_title=contest.title if contest else None,
            participant_team_id=participant_team_id,
            team_name=team_name,
            team_member_id=team_member_id,
            member_name=member_name,
            actor_role=actor_role,
            client_ip=client_ip(request),
            user_agent=request.headers.get("user-agent"),
            request_id=getattr(request.state, "request_id", None),
            details=details,
        )
    except Exception:
        return
