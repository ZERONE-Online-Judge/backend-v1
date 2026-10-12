"""Private, signed Editor execution requests; no contest permissions or writes."""
import hashlib
import tempfile
import threading

from fastapi import APIRouter, Query, Request
from starlette.concurrency import run_in_threadpool

from app.database import SessionLocal
from app.services import editor_judging as judging
from app.services.editor_bridge import identity
from app.services.errors import AppError
from app.services.responses import ok

router = APIRouter(prefix="/editor/judge", tags=["editor judge integration"])
_upload_slots = threading.BoundedSemaphore(2)


def authenticated(request, empty=False):
    email, digest = identity(request)
    if empty and digest != hashlib.sha256(b"").hexdigest():
        raise AppError(401, "editor_content_mismatch", "요청 내용이 인증 정보와 다릅니다.")
    with SessionLocal() as db:
        judging.owner(db, email)
    return email, digest


@router.get("/capabilities")
def capabilities(request: Request):
    authenticated(request, empty=True)
    return ok(request, judging.capabilities())


@router.post("/runs", status_code=202)
async def create_run(request: Request):
    email, expected = authenticated(request)
    if request.headers.get("content-type", "").split(";")[0] not in {"application/zip", "application/octet-stream"}:
        raise AppError(415, "editor_judge_snapshot_invalid", "ZIP 채점 자료를 전송하세요.")
    if not _upload_slots.acquire(blocking=False):
        raise AppError(429, "editor_judge_busy", "채점 자료를 처리 중입니다. 잠시 후 다시 요청하세요.")
    try:
        with tempfile.TemporaryFile("w+b") as source:
            size, digest = 0, hashlib.sha256()
            async for chunk in request.stream():
                size += len(chunk)
                if size > judging.MAX_ARCHIVE:
                    raise AppError(413, "editor_judge_archive_large", "채점 자료 ZIP은 128MiB 이하여야 합니다.")
                digest.update(chunk)
                await run_in_threadpool(source.write, chunk)
            if digest.hexdigest() != expected:
                raise AppError(401, "editor_content_mismatch", "채점 자료가 서명한 내용과 다릅니다.")
            source.seek(0)
            return ok(request, await run_in_threadpool(judging.create, email, source, expected))
    finally:
        _upload_slots.release()


@router.get("/runs/{run_id}")
def run_status(run_id: str, request: Request):
    email, _ = authenticated(request, empty=True)
    return ok(request, judging.get_run(email, run_id))


@router.post("/runs/{run_id}/remaining", status_code=202)
def remaining(run_id: str, request: Request, operation_id: str = Query(pattern=r"^[a-f0-9]{32}$")):
    email, _ = authenticated(request, empty=True)
    return ok(request, judging.resume(email, run_id, operation_id))


@router.delete("/runs/{run_id}")
def cancel(run_id: str, request: Request):
    email, _ = authenticated(request, empty=True)
    return ok(request, judging.get_run(email, run_id, cancel=True))
