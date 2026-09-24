"""Scoped delivery logs and plain-text previews, excluding authentication mail."""
import base64
import binascii
import json
from datetime import datetime, timezone
from typing import Literal

from fastapi import Query
from sqlalchemy import and_, case, func, or_, select

from app.database import SessionLocal
from app.orm_models import ContestRow, MailQueueItemRow
from app.services.errors import AppError

PREVIEW_MAIL_TYPES = frozenset({
    "participant_invited", "contest_operator_assigned", "contest_question_created",
    "contest_question_answered", "contest_reminder_24h", "contest_reminder_1h",
    "contest_reminder_10m", "contact_inquiry_created", "contact_inquiry_answered",
    "contest_settings_updated", "contest_notice_created",
})


def mail_log_preview(mail_id: str, *, contest_id: str | None = None) -> dict:
    mail = MailQueueItemRow
    filters = [mail.mail_queue_id == mail_id]
    if contest_id is not None:
        filters.append(mail.contest_id == contest_id)
    with SessionLocal() as db:
        row = db.execute(select(mail.mail_type,
            case((mail.mail_type.in_(PREVIEW_MAIL_TYPES), func.substr(mail.body_text, 1, 40001)), else_=None).label("body")
        ).where(*filters)).mappings().first()
    if not row:
        raise AppError(404, "not_found", "이메일 발송 기록을 찾을 수 없습니다.")
    allowed = row["mail_type"] in PREVIEW_MAIL_TYPES
    body = row["body"] or ""
    return {"body_text": body[:40000] if allowed else None,
            "restricted": not allowed, "truncated": len(body) > 40000}


def mail_log_filters(
    q: str = Query(default="", max_length=200),
    status: Literal["pending", "sending", "sent", "failed", "canceled"] | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    cursor: str | None = Query(default=None, max_length=400),
    limit: int = Query(default=50, ge=1, le=100),
) -> dict:
    since = _utc(since) if since else None
    until = _utc(until) if until else None
    if since and until and since >= until:
        raise AppError(422, "validation_error", "종료일은 시작일 이후여야 합니다.")
    return dict(q=q, status=status, since=since, until=until, cursor=cursor, limit=limit)


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def list_mail_logs(*, contest_id: str | None = None, q: str = "", status: str | None = None,
                   since: datetime | None = None, until: datetime | None = None,
                   cursor: str | None = None, limit: int = 50) -> tuple[list[dict], str | None, int]:
    mail = MailQueueItemRow
    filters = []
    if contest_id is not None:
        filters.append(mail.contest_id == contest_id)
    if q.strip():
        filters.append(or_(*(func.lower(column).contains(q.strip().lower(), autoescape=True)
                             for column in (mail.recipient_email, mail.subject, ContestRow.title))))
    if status:
        filters.append(mail.status == status)
    if since:
        filters.append(mail.created_at >= since)
    if until:
        filters.append(mail.created_at < until)
    # Authentication bodies never leave the database, including in list previews.
    statement = select(mail.mail_queue_id, mail.contest_id, ContestRow.title.label("contest_title"),
                       mail.mail_type, mail.recipient_email, mail.subject, mail.status,
                       mail.created_at, mail.last_attempt_at, mail.sent_at,
                       case((mail.mail_type.in_(PREVIEW_MAIL_TYPES), func.substr(mail.body_text, 1, 180)), else_=None).label("body_preview"),
                       (~mail.mail_type.in_(PREVIEW_MAIL_TYPES)).label("preview_restricted")).outerjoin(
                           ContestRow, mail.contest_id == ContestRow.contest_id).where(*filters)
    count = select(func.count()).select_from(mail).outerjoin(
        ContestRow, mail.contest_id == ContestRow.contest_id).where(*filters)
    if cursor:
        try:
            value = json.loads(base64.urlsafe_b64decode(cursor.encode("ascii")))
            created_at, mail_id = _utc(datetime.fromisoformat(value["at"])), value["id"]
            if not isinstance(mail_id, str) or not 1 <= len(mail_id) <= 36:
                raise ValueError()
        except (ValueError, TypeError, KeyError, UnicodeError, binascii.Error):
            raise AppError(422, "validation_error", "이메일 로그의 페이지 정보가 올바르지 않습니다.") from None
        statement = statement.where(or_(mail.created_at < created_at,
                                        and_(mail.created_at == created_at, mail.mail_queue_id < mail_id)))
    with SessionLocal() as db:
        total = int(db.scalar(count) or 0)
        rows = db.execute(statement.order_by(mail.created_at.desc(), mail.mail_queue_id.desc()).limit(limit + 1)).mappings().all()
        items = [dict(row) for row in rows[:limit]]
    for item in items:
        for key in ("created_at", "last_attempt_at", "sent_at"):
            if item[key] is not None:
                item[key] = _utc(item[key])
    next_cursor = None
    if len(rows) > limit:
        last = items[-1]
        next_cursor = base64.urlsafe_b64encode(json.dumps({"at": last["created_at"].isoformat(), "id": last["mail_queue_id"]}).encode()).decode("ascii")
    return items, next_cursor, total
