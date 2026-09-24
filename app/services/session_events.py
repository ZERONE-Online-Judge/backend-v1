"""Watch persisted sessions, including revocations made by another API worker."""
import asyncio
import logging
from dataclasses import dataclass
from weakref import WeakKeyDictionary

from fastapi import Request
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from starlette.concurrency import run_in_threadpool

from app.database import SessionLocal
from app.models import now_utc
from app.orm_models import GeneralSessionRow, StaffSessionRow, TeamSessionRow
from app.services.security import decode_session_token, token_hash
from app.services.store import _aware

SESSION_CHECK_INTERVAL_SECONDS = 1
SESSION_CHECK_BATCH_SIZE = 500
logger = logging.getLogger(__name__)
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


def active_session_watches(watches: set[SessionWatch]) -> set[SessionWatch]:
    """One indexed query per batch/type, regardless of how many tabs watch it."""
    active = set()
    now = now_utc()
    with SessionLocal() as db:
        for kind, (model, id_field, _, expiry) in SESSION_TYPES.items():
            ids = [watch.session_id for watch in watches if watch.kind == kind]
            for offset in range(0, len(ids), SESSION_CHECK_BATCH_SIZE):
                found = db.scalars(select(getattr(model, id_field)).where(
                    getattr(model, id_field).in_(ids[offset:offset + SESSION_CHECK_BATCH_SIZE]),
                    model.revoked_at.is_(None), getattr(model, expiry) > now,
                ))
                active.update(SessionWatch(kind, session_id) for session_id in found)
    return active


class SessionMonitor:
    def __init__(self):
        self.watchers: dict[SessionWatch, set[asyncio.Event]] = {}
        self.task: asyncio.Task | None = None

    def subscribe(self, watch):
        event = asyncio.Event()
        self.watchers.setdefault(watch, set()).add(event)
        if self.task is None:
            self.task = asyncio.create_task(self._run())
        return event

    async def unsubscribe(self, watch, event):
        listeners = self.watchers.get(watch)
        if listeners is not None:
            listeners.discard(event)
            if not listeners:
                self.watchers.pop(watch, None)
        if not self.watchers and self.task is not None:
            task, self.task = self.task, None
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def _run(self):
        while self.watchers:
            snapshot = set(self.watchers)
            try:
                active = await run_in_threadpool(active_session_watches, snapshot)
            except SQLAlchemyError:
                # An unavailable database is not evidence that a user logged out.
                # Normal API authorization still consults the database separately.
                logger.warning("Session status check unavailable; retrying")
            else:
                for watch in snapshot - active:
                    for event in self.watchers.get(watch, ()):
                        event.set()
            await asyncio.sleep(SESSION_CHECK_INTERVAL_SECONDS)


_monitors = WeakKeyDictionary()


def session_monitor():
    loop = asyncio.get_running_loop()
    if loop not in _monitors:
        _monitors[loop] = SessionMonitor()
    return _monitors[loop]


async def session_events(request: Request, watch: SessionWatch):
    monitor = session_monitor()
    revoked = monitor.subscribe(watch)
    try:
        yield "event: ready\ndata: {}\n\n"
        while not await request.is_disconnected():
            try:
                await asyncio.wait_for(revoked.wait(), timeout=15)
            except TimeoutError:
                yield ": keepalive\n\n"
            else:
                yield "event: session_revoked\ndata: {}\n\n"
                return
    finally:
        await monitor.unsubscribe(watch, revoked)
