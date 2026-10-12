import hashlib
import json
from datetime import datetime, timezone
from fastapi import APIRouter, Query, Request
from sqlalchemy import select
from app.database import SessionLocal
from app.orm_models import ContestDivisionRow, ContestRow
from app.services.authz import has_contest_permission
from app.services.editor_bridge import identity, staff
from app.services.errors import AppError
from app.services.problem_archive import CHUNK, import_archive
from app.services.responses import ok
from app.routers.problem_archives import _receive

router = APIRouter(prefix="/editor", tags=["editor integration"])


@router.get("/contests")
def eligible_contests(request: Request):
    email, content_hash = identity(request)
    if content_hash != hashlib.sha256(b"").hexdigest():
        raise AppError(401, "editor_content_mismatch", "요청 내용이 인증 정보와 다릅니다.")
    with SessionLocal() as db:
        account = staff(db, email)
        contests = []
        for row in db.scalars(select(ContestRow).order_by(ContestRow.created_at.desc())):
            if not has_contest_permission(account, row.contest_id, "contest.problem.manage"):
                continue
            start = row.start_at.replace(tzinfo=timezone.utc) if row.start_at.tzinfo is None else row.start_at
            end = row.end_at.replace(tzinfo=timezone.utc) if row.end_at.tzinfo is None else row.end_at
            now = datetime.now(timezone.utc)
            locked = row.status == "running" or (row.status not in {"draft", "schedule_tbd", "ended", "finalized", "archived"} and start <= now < end)
            divisions = [{"id": d.division_id, "name": d.name, "code": d.code} for d in db.scalars(select(ContestDivisionRow).where(ContestDivisionRow.contest_id == row.contest_id).order_by(ContestDivisionRow.display_order))]
            contests.append({"id": row.contest_id, "title": row.title, "status": row.status, "locked": locked, "divisions": divisions})
        return ok(request, {"email": email, "contests": contests})


@router.post("/contests/{contest_id}/imports")
async def receive_problem(contest_id: str, request: Request,
        division_id: str = Query(min_length=1, max_length=36),
        problem_code: str = Query(min_length=1, max_length=16),
        operation_id: str = Query(pattern=r"^[a-f0-9]{32}$")):
    email, content_hash = identity(request)
    with SessionLocal() as db:
        staff(db, email, contest_id)
    operation_key = hashlib.sha256((email + "\n" + operation_id).encode()).hexdigest()
    fingerprint = hashlib.sha256(json.dumps([contest_id, division_id, problem_code.strip(), content_hash], ensure_ascii=False).encode()).hexdigest()
    context = {"email": email, "operation_key": operation_key, "request_digest": fingerprint, "request_id": request.state.request_id}
    def handle(source):
        digest = hashlib.sha256()
        while chunk := source.read(CHUNK):
            digest.update(chunk)
        if digest.hexdigest() != content_hash:
            raise AppError(401, "editor_content_mismatch", "업로드 내용이 인증 정보와 다릅니다.")
        source.seek(0)
        return import_archive(source, contest_id, division_id, problem_code, editor_context=context)
    return ok(request, await _receive(request, handle))
