"""Watch persisted sessions, including revocations made by another API worker."""
import asyncio
from dataclasses import dataclass

from fastapi import Request
from sqlalchemy import select
from starlette.concurrency import run_in_threadpool

from app.database import SessionLocal
from app.models import now_utc
from app.orm_models import GeneralSessionRow, StaffSessionRow, TeamSessionRow
from app.services.security import decode_session_token, token_hash
from app.services.store import _aware

SESSION_CHECK_INTERVAL_SECONDS = 1
SESSION_TYPES = {
    "general_access": (GeneralSessionRow, "general_session_id", "access_expires_at", "refresh_expires_at"),
    "participant_access": (TeamSessionRow, "team_session_id", "expires_at", "expires_at"),
    "staff_access": (StaffSessionRow, "staff_session_id", "access_expires_at", "refresh_expires_at"),
}


@dataclass(frozen=True)
class SessionWatch:
    kind: str
    session_id: str


def resolve_session_watch(token: str | None) -> SessionWatch | None:
    if not token:
        return None
    for kind, (model, id_field, access_expiry, _) in SESSION_TYPES.items():
        if not decode_session_token(token, kind):
            continue
        with SessionLocal() as db:
            row = db.scalar(select(model).where(model.access_token_hash == token_hash(token)))
            if not row or row.revoked_at or _aware(getattr(row, access_expiry)) <= now_utc():
                return None
            return SessionWatch(kind, getattr(row, id_field))
    return None


def session_watch_active(watch: SessionWatch) -> bool:
    model, _, _, expiry = SESSION_TYPES[watch.kind]
    with SessionLocal() as db:
        row = db.get(model, watch.session_id)
        return bool(row and not row.revoked_at and _aware(getattr(row, expiry)) > now_utc())


async def session_events(request: Request, watch: SessionWatch):
    yield "event: ready\ndata: {}\n\n"
    ticks = 0
    while not await request.is_disconnected():
        if not await run_in_threadpool(session_watch_active, watch):
            yield "event: session_revoked\ndata: {}\n\n"
            return
        ticks += 1
        if ticks % 15 == 0:
            yield ": keepalive\n\n"
        await asyncio.sleep(SESSION_CHECK_INTERVAL_SECONDS)
