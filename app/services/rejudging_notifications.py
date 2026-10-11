"""Durable, recipient-only inbox. Creation participates in the apply transaction."""
from fastapi.encoders import jsonable_encoder
from sqlalchemy import func, select, update

from app.models import now_utc
from app.orm_models import (
    ContestRow, GeneralSessionRow, RejudgingNotificationRow as Notice,
    TeamMemberRow as Member, TeamSessionRow,
)
from app.services.errors import authentication_required, not_found
from app.services.security import decode_session_token, token_hash
from app.services.store import _aware


def create_for_change(db, batch, item, submission, context):
    if submission.submission_kind != "participant" or item.original_result["status"] == submission.status:
        return
    members = select(Member).where(
        Member.contest_id == batch.contest_id,
        Member.participant_team_id == submission.participant_team_id,
    )
    if submission.team_member_id:
        members = members.where(Member.team_member_id == submission.team_member_id)
    elif submission.submitted_by_email:
        members = members.where(func.lower(Member.email) == submission.submitted_by_email.lower())
    else:
        # Legacy shared-team submissions have no identifiable individual author.
        if not submission.participant_team_id:
            return
    contest = db.get(ContestRow, batch.contest_id)
    for member in db.scalars(members):
        db.add(Notice(
            rejudging_item_id=item.rejudging_item_id, team_member_id=member.team_member_id,
            contest_id=batch.contest_id, submission_id=submission.submission_id,
            problem_id=batch.problem_id, contest_title=contest.title if contest else "대회",
            problem_code=context["problem"]["problem_code"], old_status=item.original_result["status"],
            new_status=submission.status, reason=batch.reason, created_at=submission.status_updated_at,
        ))


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


def _fields(row):
    return jsonable_encoder({key: getattr(row, key) for key in (
        "notification_id", "contest_id", "contest_title", "problem_id", "problem_code", "submission_id",
        "old_status", "new_status", "reason", "created_at", "read_at",
    )})


def list_notifications(store, token, contest_id=None, offset=0, limit=50):
    offset, limit = max(0, offset), max(1, min(limit, 100))
    with store._session() as db:
        owned = _owned_member_ids(db, token, contest_id)
        rows = list(db.scalars(select(Notice).where(
            Notice.team_member_id.in_(owned), Notice.deleted_at.is_(None),
        ).order_by(Notice.created_at.desc(), Notice.notification_id.desc()).offset(offset).limit(limit + 1)))
        return {"items": [_fields(row) for row in rows[:limit]], "next_offset": offset + limit if len(rows) > limit else None}


def update_notification(store, token, notification_id, action, contest_id=None):
    with store._session() as db:
        owned = _owned_member_ids(db, token, contest_id)
        row = db.scalar(select(Notice).where(
            Notice.notification_id == notification_id, Notice.team_member_id.in_(owned),
        ).with_for_update())
        if row is None:
            raise not_found()
        if action == "read":
            # SQL coalesce preserves the first read timestamp across duplicate requests.
            db.execute(update(Notice).where(Notice.notification_id == row.notification_id)
                .values(read_at=func.coalesce(Notice.read_at, now_utc())))
        else:
            db.execute(update(Notice).where(Notice.notification_id == row.notification_id)
                .values(deleted_at=func.coalesce(Notice.deleted_at, now_utc())))
        db.commit()
        return {"notification_id": notification_id}
