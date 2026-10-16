"""Coalesce result waits into bounded, cross-worker-safe database batches."""
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from threading import BoundedSemaphore
from weakref import WeakKeyDictionary

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from starlette.concurrency import run_in_threadpool

from app.database import SessionLocal
from app.orm_models import SubmissionRow
from app.services.errors import AppError

PENDING = {"waiting", "preparing", "judging"}
BATCH_SIZE = 500
CHECK_INTERVAL = 0.5
logger = logging.getLogger(__name__)
_snapshot_workers = ThreadPoolExecutor(max_workers=2, thread_name_prefix="result-auth")
_snapshot_slots = BoundedSemaphore(16)


async def read_status_snapshot(function):
    if not _snapshot_slots.acquire(blocking=False):
        raise AppError(503, "api_busy", "요청이 많습니다. 잠시 후 다시 시도해 주세요.",
            {"retry_after_seconds":1})
    try:
        future = _snapshot_workers.submit(function)
    except BaseException:
        _snapshot_slots.release()
        raise
    # A disconnected caller must not free a slot while its SQL is still running.
    future.add_done_callback(lambda _: _snapshot_slots.release())
    return await asyncio.wrap_future(future)


def pending_submissions(ids):
    pending = set()
    with SessionLocal() as db:
        for offset in range(0, len(ids), BATCH_SIZE):
            pending.update(db.scalars(select(SubmissionRow.submission_id).where(
                SubmissionRow.submission_id.in_(ids[offset:offset+BATCH_SIZE]),
                SubmissionRow.status.in_(PENDING))))
    return pending


class SubmissionMonitor:
    def __init__(self):
        self.watchers = {}
        self.task = None

    async def wait(self, submission_id, seconds):
        if seconds <= 0:
            return
        event = asyncio.Event()
        self.watchers.setdefault(submission_id, set()).add(event)
        if self.task is None:
            self.task = asyncio.create_task(self._run())
        try:
            try:
                await asyncio.wait_for(event.wait(), timeout=min(seconds, 10))
            except TimeoutError:
                pass
        finally:
            listeners = self.watchers.get(submission_id)
            if listeners is not None:
                listeners.discard(event)
                if not listeners:
                    self.watchers.pop(submission_id, None)
            if not self.watchers and self.task:
                task, self.task = self.task, None
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    async def _run(self):
        while self.watchers:
            snapshot = set(self.watchers)
            try:
                pending = await run_in_threadpool(pending_submissions, list(snapshot))
            except SQLAlchemyError:
                # A failed read cannot be reported as a terminal judge result.
                logger.warning("Submission status check unavailable; retrying")
            else:
                for sid in snapshot-pending:
                    for event in self.watchers.get(sid, ()):
                        event.set()
            await asyncio.sleep(CHECK_INTERVAL)


_monitors = WeakKeyDictionary()


async def wait_for_submission(submission_id, seconds):
    loop = asyncio.get_running_loop()
    monitor = _monitors.setdefault(loop, SubmissionMonitor())
    await monitor.wait(submission_id, max(0, seconds))
