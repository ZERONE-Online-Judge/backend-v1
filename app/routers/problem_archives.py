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


@router.get('/operator/contests/{contest_id}/problems/{problem_id}/package-backups/{operation_key}')
def download_package_backup(contest_id: str, problem_id: str, operation_key: str, request: Request):
    import re
    from app.database import SessionLocal
    from app.orm_models import EditorImportRow, ProblemRow
    from app.services.storage import object_storage
    require_contest_staff(request, contest_id, 'contest.problem.resource.view')
    if not re.fullmatch(r'[a-f0-9]{64}', operation_key):
        raise AppError(404, 'not_found', '백업을 찾을 수 없습니다.')
    with SessionLocal() as db:
        problem, receipt = db.get(ProblemRow, problem_id), db.get(EditorImportRow, operation_key)
        if not problem or problem.contest_id != contest_id or not receipt or receipt.problem_id != problem_id:
            raise AppError(404, 'not_found', '백업을 찾을 수 없습니다.')
    key = f'contests/{contest_id}/problems/{problem_id}/package-backups/{operation_key}.zoj.zip'
    def chunks():
        with object_storage.open_reader(key) as source:
            while chunk := source.read(CHUNK):
                yield chunk
    return StreamingResponse(chunks(), media_type='application/zip', headers={
        'Content-Disposition': 'attachment; filename="before-update.zoj.zip"', 'Cache-Control': 'no-store'})


@router.get('/operator/contests/{contest_id}/problems/{problem_id}/package-updates')
def package_update_history(contest_id: str, problem_id: str, request: Request):
    import json
    from sqlalchemy import select
    from app.database import SessionLocal
    from app.orm_models import OperationalAuditLogRow, ProblemRow
    require_contest_staff(request, contest_id, 'contest.problem.resource.view')
    with SessionLocal() as db:
        problem = db.get(ProblemRow, problem_id)
        if not problem or problem.contest_id != contest_id:
            raise AppError(404, 'not_found', '문제를 찾을 수 없습니다.')
        rows = db.scalars(select(OperationalAuditLogRow).where(
            OperationalAuditLogRow.contest_id == contest_id,
            OperationalAuditLogRow.action == 'Editor 패키지 업데이트',
            OperationalAuditLogRow.details.contains(problem_id)
        ).order_by(OperationalAuditLogRow.created_at.desc()).limit(50))
        result = []
        for row in rows:
            detail = json.loads(row.details)
            if detail.get('problem_id') != problem_id or not detail.get('backup_key'):
                continue
            operation = detail['backup_key'].rsplit('/', 1)[-1].removesuffix('.zoj.zip')
            result.append({'id': row.operational_audit_log_id, 'created_at': row.created_at,
                'actor_name': row.actor_name, 'reason': detail.get('reason', ''),
                'backup_url': f'/api/operator/contests/{contest_id}/problems/{problem_id}/package-backups/{operation}'})
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
