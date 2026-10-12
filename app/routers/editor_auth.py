"""Server-only authentication adapter; ZOJ owns all credentials and sessions."""

import hashlib
import json
from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse
from pydantic import (
    BaseModel,
    ConfigDict,
    EmailStr,
    Field,
    ValidationError,
    field_validator,
)
from starlette.concurrency import run_in_threadpool
from app.database import SessionLocal
from app.orm_models import IdentityAccountRow
from app.services.editor_bridge import identity
from app.services.auth_rate_limit import consume
from app.services.errors import AppError
from app.services import password_auth, shared_login
from app.services.responses import ok
from app.services.access_logging import write_access_log
from app.services.session_events import SessionWatch, session_events
from app.services.store import SessionConflictError, store
from app.settings import settings

router = APIRouter(prefix="/editor/auth", tags=["shared authentication"])


class Call(BaseModel):
    model_config = ConfigDict(extra="forbid")
    email: EmailStr | None = None
    session_token: str | None = Field(default=None, max_length=4096)
    password: str | None = Field(default=None, min_length=1, max_length=128)
    new_password: str | None = Field(default=None, min_length=8, max_length=128)
    code: str | None = Field(default=None, pattern=r"^[0-9]{6}$")
    client_ip: str = Field(default="127.0.0.1", max_length=64)
    force_new_session: bool = False
    display_name: str | None = Field(default=None, min_length=1, max_length=80)

    @field_validator("new_password")
    @classmethod
    def password_policy(cls, value):
        from app.services.account_passwords import validate_password

        return validate_password(value) if value is not None else value


async def verified(request, readonly=False):
    key = (
        settings.editor_auth_bridge_secret.get_secret_value()
        if settings.editor_auth_bridge_secret
        else ""
    )
    _, expected = identity(
        request,
        secret_override=key,
        purpose="zoj-shared-auth-v1",
        consume_nonce=not readonly,
    )
    body = bytearray()
    async for chunk in request.stream():
        body.extend(chunk)
        if len(body) > 8192:
            raise AppError(413, "auth_request_too_large", "요청 크기가 너무 큽니다.")
    if hashlib.sha256(body).hexdigest() != expected:
        raise AppError(401, "auth_request_changed", "인증 요청 내용이 변경되었습니다.")
    try:
        return Call.model_validate(json.loads(body))
    except (ValueError, ValidationError):
        raise AppError(422, "invalid_auth_request", "인증 입력 값을 확인하세요.")


def require(values, *names):
    if any(getattr(values, name) is None for name in names):
        raise AppError(422, "invalid_auth_request", "필수 인증 입력을 확인하세요.")


def execute(action, values):
    email = str(values.email).lower() if values.email else ""
    if action == "login-options":
        require(values, "email")
        return password_auth.login_options(email, values.client_ip)
    if action == "code-request":
        require(values, "email")
        consume("otp-mail-ip", values.client_ip, 1000, 3600)
        consume("otp-mail-email", email, 10, 3600)
        retry = store.general_otp_retry_after_seconds(email)
        if retry:
            raise AppError(
                429,
                "otp_request_rate_limited",
                "잠시 후 인증번호를 다시 요청하세요.",
                {"retry_after_seconds": retry},
            )
        if not store.create_general_otp(email, allow_new=True):
            raise AppError(422, "invalid_email", "사용할 이메일을 확인하세요.")
        return {
            "sent": True,
            "cooldown_seconds": settings.otp_request_cooldown_seconds,
            "valid_seconds": settings.otp_ttl_seconds,
        }
    if action == "code-verify":
        require(values, "email", "code")
        from app.services.auth_rate_limit import check_otp

        check_otp(email, values.client_ip)
        session = store.verify_general_otp(email, values.code, values.force_new_session)
        if not session:
            raise AppError(
                401, "invalid_credentials", "이메일 또는 인증번호가 올바르지 않습니다."
            )
        return shared_login.editor_result(session, values.session_token)
    if action == "password-login":
        require(values, "email", "password")
        session = password_auth.login(
            email, values.password, values.force_new_session, values.client_ip
        )
        if not session:
            raise AppError(
                401, "invalid_credentials", "이메일 또는 비밀번호가 올바르지 않습니다."
            )
        return shared_login.editor_result(session, values.session_token)
    if action == "password-reset-request":
        require(values, "email")
        return password_auth.request_change(email, "reset", values.client_ip)
    if action == "password-reset-confirm":
        require(values, "email", "code", "new_password")
        return password_auth.confirm_change(
            email, "reset", values.code, values.new_password, values.client_ip
        )
    user = shared_login.resolve(values.session_token)
    if action == "me":
        user.pop("session_id")
        return user
    if action == "logout":
        store.revoke_general_session(None, values.session_token)
        return {"revoked": True}
    if action == "password-status":
        return {"has_password": password_auth.has_password(user["email"])}
    if action == "password-request":
        return password_auth.request_change(user["email"], "set", values.client_ip)
    if action == "password-confirm":
        require(values, "code", "new_password")
        return password_auth.confirm_change(
            user["email"], "set", values.code, values.new_password, values.client_ip
        )
    if action == "profile":
        require(values, "display_name")
        name = values.display_name.strip()
        if not name:
            raise AppError(422, "invalid_profile", "표시 이름을 입력하세요.")
        with SessionLocal() as db:
            account = db.get(IdentityAccountRow, user["account_id"])
            account.display_name = name
            db.commit()
        return {"display_name": name}
    raise AppError(404, "not_found", "인증 작업을 찾을 수 없습니다.")


@router.post("/events")
async def watch(request: Request):
    values = await verified(request, readonly=True)
    user = await run_in_threadpool(shared_login.resolve, values.session_token)
    return StreamingResponse(
        session_events(request, SessionWatch("general_access", user["session_id"])),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"},
    )


@router.post("/{action}")
async def call(action: str, request: Request):
    allowed = {
        "login-options",
        "code-request",
        "code-verify",
        "password-login",
        "password-reset-request",
        "password-reset-confirm",
        "me",
        "logout",
        "password-status",
        "password-request",
        "password-confirm",
        "profile",
    }
    if action not in allowed:
        raise AppError(404, "not_found", "인증 작업을 찾을 수 없습니다.")
    values = await verified(request, readonly=action in {"me", "password-status"})
    email = str(values.email).lower() if values.email else None
    if action in {"password-confirm", "logout"}:
        email = (await run_in_threadpool(shared_login.resolve, values.session_token))[
            "email"
        ]
    try:
        result = await run_in_threadpool(execute, action, values)
    except SessionConflictError as error:
        raise AppError(
            409,
            "session_conflict",
            "이미 다른 브라우저 또는 기기에서 로그인 중입니다.",
            error.details,
        )
    except AppError as error:
        if action in {"code-verify", "password-login"} and error.status_code == 401:
            write_access_log(
                request,
                event_type="login_failed",
                account_scope="general",
                email=email,
                details={
                    "source": "editor",
                    "method": action,
                    "client_ip": values.client_ip,
                },
            )
        raise
    event = {
        "code-verify": "general_login",
        "password-login": "general_login",
        "password-confirm": "password_changed",
        "password-reset-confirm": "password_reset",
        "logout": "logout",
    }.get(action)
    if event:
        user = result.get("user", {})
        write_access_log(
            request,
            event_type=event,
            account_scope="general",
            email=user.get("email", email),
            display_name=user.get("display_name"),
            details={
                "source": "editor",
                "method": action,
                "client_ip": values.client_ip,
            },
        )
    return ok(request, result)
