"""Durable, recipient-only inbox. Creation participates in the apply transaction."""
from sqlalchemy import func, select

from app.models import now_utc
from app.orm_models import (
    GeneralSessionRow,
    TeamMemberRow as Member, TeamSessionRow,
)
from app.services.errors import authentication_required
from app.services.security import decode_session_token, token_hash
from app.services.store import _aware


def create_for_change(db, batch, item, submission, context):
    from app.services.notifications import create_rejudging
    create_rejudging(db, batch, item, submission, context)


def _owned_member_ids(db, token, contest_id):
    """Check persisted revocation/expiry and current membership on every request."""
    if token and decode_session_token(token, "general_access"):
        session = db.scalar(select(GeneralSessionRow).where(
            GeneralSessionRow.access_token_hash == token_hash(token), GeneralSessionRow.revoked_at.is_(None),
        ))
        if session and _aware(session.access_expires_at) > now_utc():
            owned = select(Member.team_member_id).where(func.lower(Member.email) == session.email.lower())
            return owned.where(Member.contest_id == contest_id) if contest_id else owned
    elif token and contest_id and decode_session_token(token, "participant_access"):
        session = db.scalar(select(TeamSessionRow).where(
            TeamSessionRow.access_token_hash == token_hash(token), TeamSessionRow.contest_id == contest_id,
            TeamSessionRow.revoked_at.is_(None),
        ))
        member = db.get(Member, session.team_member_id) if session else None
        if (session and member and _aware(session.expires_at) > now_utc()
            and member.contest_id == contest_id and member.participant_team_id == session.participant_team_id
            and (not member.session_revoked_at or _aware(session.issued_at) > _aware(member.session_revoked_at))):
            return select(Member.team_member_id).where(Member.team_member_id == member.team_member_id)
    raise authentication_required("An active participant or general session is required.")


def list_notifications(store, token, contest_id=None, offset=0, limit=50):
    from app.services.notifications import list_notifications as unified
    return unified(store, token, contest_id, offset, limit, kind="rejudging")


def update_notification(store, token, notification_id, action, contest_id=None):
    from app.services.notifications import update_notification as unified
    return unified(store, token, notification_id, action, contest_id)
