"""Narrow server trust: verified Editor emails may only list/create problems.

No staff access tokens are issued and permission scopes come from ZOJ's DB.
"""
import base64
import hashlib
import hmac
import re
import time
from sqlalchemy import delete, func, select
from sqlalchemy.exc import IntegrityError
from app.database import SessionLocal
from app.orm_models import EditorNonceRow, StaffAccountRow
from app.settings import settings
from app.services.authz import has_contest_permission
from app.services.errors import AppError
from app.services.store import _staff


def identity(request):
    secret = settings.editor_bridge_secret.get_secret_value() if settings.editor_bridge_secret else ""
    if len(secret) < 32:
        raise AppError(503, "editor_bridge_disabled", "Editor 연동이 아직 설정되지 않았습니다.")
    encoded = request.headers.get("x-editor-identity", "")
    stamp = request.headers.get("x-editor-timestamp", "")
    nonce = request.headers.get("x-editor-nonce", "")
    content_hash = request.headers.get("x-editor-content-sha256", "")
    signature = request.headers.get("x-editor-signature", "")
    if not re.fullmatch(r"[0-9]{10}", stamp) or abs(int(stamp) - int(time.time())) > 90 or not re.fullmatch(r"[a-f0-9]{32}", nonce) or not re.fullmatch(r"[a-f0-9]{64}", content_hash) or not re.fullmatch(r"[a-f0-9]{64}", signature) or len(encoded) > 512:
        raise AppError(401, "editor_signature_invalid", "Editor 인증 요청이 유효하지 않습니다.")
    path = request.url.path + ("?" + request.url.query if request.url.query else "")
    canonical = "\n".join(["zoj-editor-v1", request.method, path, stamp, nonce, encoded, content_hash])
    expected = hmac.new(secret.encode(), canonical.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, signature):
        raise AppError(401, "editor_signature_invalid", "Editor 인증 요청이 유효하지 않습니다.")
    try:
        email = base64.b64decode(encoded + "=" * (-len(encoded) % 4), altchars=b'-_', validate=True).decode().lower()
        from pydantic import TypeAdapter, EmailStr
        email = str(TypeAdapter(EmailStr).validate_python(email)).lower()
    except (ValueError, UnicodeError):
        raise AppError(401, "editor_signature_invalid", "Editor 계정 정보가 유효하지 않습니다.")
    with SessionLocal() as db:
        db.execute(delete(EditorNonceRow).where(EditorNonceRow.expires_at < int(time.time())))
        db.add(EditorNonceRow(nonce=nonce, expires_at=int(time.time()) + 180))
        try:
            db.commit()
        except IntegrityError:
            db.rollback()
            raise AppError(401, "editor_signature_replayed", "이미 처리한 Editor 인증 요청입니다.")
    return email, content_hash


def staff(db, email, contest_id=None, lock=False):
    query = select(StaffAccountRow).where(func.lower(StaffAccountRow.email) == email.lower())
    if lock:
        query = query.with_for_update()
    row = db.scalar(query)
    if not row:
        raise AppError(403, "editor_account_not_operator", "같은 이메일로 등록된 ZOJ 운영자 계정이 없습니다.")
    account = _staff(row)
    if contest_id and not has_contest_permission(account, contest_id, "contest.problem.manage"):
        raise AppError(403, "scope_denied", "이 대회에 문제를 등록할 권한이 없습니다.")
    return account
