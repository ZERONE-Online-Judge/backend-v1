"""Problem archive transport. Authorize before accepting the upload body."""
import tempfile
import threading
from urllib.parse import quote

from fastapi import APIRouter, Query, Request
from fastapi.responses import StreamingResponse
from starlette.background import BackgroundTask
from starlette.concurrency import run_in_threadpool

from app.routers.operator import _require_contest_mutation_open
from app.services.authz import require_contest_staff
from app.services.errors import AppError
from app.services.problem_archive import CHUNK, MAX_UPLOAD, export_archive, import_archive, inspect_archive
from app.services.responses import ok

router = APIRouter(tags=['operator'])
# Bound temporary disk/CPU usage independently of the general request thread pool.
_archive_slots = threading.BoundedSemaphore(2)


def _reserve():
    if not _archive_slots.acquire(blocking=False):
        raise AppError(429, 'problem_archive_busy', '다른 문제 ZIP을 처리 중입니다. 잠시 후 다시 시도해 주세요.')


async def _receive(request: Request, handler):
    if request.headers.get('content-type', '').split(';')[0] not in {'application/zip', 'application/octet-stream'}:
        raise AppError(415, 'problem_archive_invalid', 'ZIP 파일을 선택해 주세요.')
    try:
        length = int(request.headers.get('content-length', '0'))
    except ValueError:
        raise AppError(422, 'problem_archive_invalid', '잘못된 업로드 크기입니다.')
    if length > MAX_UPLOAD:
        raise AppError(413, 'problem_archive_too_large', '문제 ZIP은 512 MiB 이하만 지원합니다.')
    _reserve()
    try:
        with tempfile.TemporaryFile('w+b') as temporary:
            size = 0
            async for chunk in request.stream():
                size += len(chunk)
                if size > MAX_UPLOAD:
                    raise AppError(413, 'problem_archive_too_large', '문제 ZIP은 512 MiB 이하만 지원합니다.')
                await run_in_threadpool(temporary.write, chunk)
            temporary.seek(0)
            return await run_in_threadpool(handler, temporary)
    finally:
        _archive_slots.release()


@router.get('/operator/contests/{contest_id}/problems/{problem_id}/archive')
def download_problem_archive(contest_id: str, problem_id: str, request: Request):
    require_contest_staff(request, contest_id, 'contest.problem.resource.view')
    _reserve()
    try:
        content, filename = export_archive(contest_id, problem_id)
    finally:
        _archive_slots.release()
    content.seek(0, 2)
    length = content.tell()
    content.seek(0)
    def chunks():
        try:
            while chunk := content.read(CHUNK):
                yield chunk
        finally:
            content.close()
    return StreamingResponse(chunks(), media_type='application/zip',
        headers={'Content-Disposition': "attachment; filename=problem.zoj.zip; filename*=UTF-8''" + quote(filename, safe=''),
                 'Content-Length': str(length), 'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff'},
        background=BackgroundTask(content.close))


@router.post('/operator/contests/{contest_id}/problem-archives:inspect')
async def inspect_problem_archive(contest_id: str, request: Request):
    require_contest_staff(request, contest_id, 'contest.problem.manage')
    _require_contest_mutation_open(contest_id)
    result = await _receive(request, inspect_archive)
    return ok(request, result)


@router.post('/operator/contests/{contest_id}/problem-archives:import')
async def import_problem_archive(contest_id: str, request: Request,
                                 division_id: str = Query(min_length=1, max_length=36),
                                 problem_code: str = Query(min_length=1, max_length=16),
                                 display_order: int | None = Query(default=None, ge=1, le=1_000_000)):
    require_contest_staff(request, contest_id, 'contest.problem.manage')
    _require_contest_mutation_open(contest_id)
    result = await _receive(request, lambda source: import_archive(source, contest_id, division_id, problem_code, display_order))
    return ok(request, result)
