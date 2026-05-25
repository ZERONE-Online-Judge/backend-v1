import re
from uuid import uuid4

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.routers import admin, auth, internal_judge, operator, participant, public, storage
from app.services.errors import AppError
from app.services.authz import bearer_token
from app.services.store import store
from app.settings import settings


app = FastAPI(title="Zerone Online Judge API", version="0.1.0")

AUDITED_METHODS = {"POST", "PATCH", "PUT", "DELETE"}
ID_SEGMENT_PATTERN = re.compile(
    r"/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
    re.IGNORECASE,
)

cors_origins = [origin.strip() for origin in settings.cors_allow_origins.split(",") if origin.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_origin_regex=settings.cors_allow_origin_regex,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(AppError)
async def app_error_handler(request: Request, exc: AppError) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": {
                "code": exc.code,
                "message": exc.message,
                "request_id": request.state.request_id,
                "details": exc.details,
            }
        },
    )


@app.middleware("http")
async def request_id_middleware(request: Request, call_next):
    request.state.request_id = request.headers.get("x-request-id", f"req_{uuid4().hex}")
    response = await call_next(request)
    response.headers["x-request-id"] = request.state.request_id
    return response


def _audit_scope(path: str) -> str | None:
    if path.startswith("/api/admin/"):
        return "admin"
    if path.startswith("/api/operator/"):
        return "operator"
    return None


def _audit_contest_id(path: str) -> str | None:
    match = re.match(r"^/api/(?:operator|admin)/contests/([^/]+)", path)
    return match.group(1) if match else None


def _audit_action(method: str, path: str) -> str:
    normalized_path = ID_SEGMENT_PATTERN.sub("/{id}", path)
    return f"{method.upper()} {normalized_path.removeprefix('/api')}"


def _client_ip(request: Request) -> str | None:
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",", 1)[0].strip() or None
    return request.client.host if request.client else None


@app.middleware("http")
async def operational_audit_middleware(request: Request, call_next):
    response = await call_next(request)
    path = request.url.path
    scope = _audit_scope(path)
    if request.method.upper() not in AUDITED_METHODS or not scope:
        return response

    try:
        token = bearer_token(request)
        account = store.get_staff_by_access_token(token) if token else None
        if not account and token:
            account = store.get_staff_by_general_access_token(token)
        actor_role = None
        if account:
            actor_role = "service_master" if account.is_service_master else "operator"
        store.append_operational_audit_log(
            scope=scope,
            action=_audit_action(request.method, path),
            method=request.method.upper(),
            path=path,
            status_code=response.status_code,
            actor_email=str(account.email) if account else None,
            actor_name=account.display_name if account else None,
            actor_role=actor_role,
            contest_id=_audit_contest_id(path),
            client_ip=_client_ip(request),
            user_agent=request.headers.get("user-agent"),
            request_id=getattr(request.state, "request_id", None),
            details={"query": request.url.query} if request.url.query else {},
        )
    except Exception:
        # Audit logging must never break the operator/admin action itself.
        pass
    return response


@app.get("/api/health")
async def health(request: Request):
    return {
        "data": {
            "status": "ok",
            "env": settings.app_env,
            "release_color": settings.release_color,
            "release_version": settings.release_version,
        },
        "request_id": request.state.request_id,
    }


app.include_router(public.router, prefix="/api")
app.include_router(auth.router, prefix="/api")
app.include_router(participant.router, prefix="/api")
app.include_router(admin.router, prefix="/api")
app.include_router(operator.router, prefix="/api")
app.include_router(internal_judge.router, prefix="/api")
app.include_router(storage.router, prefix="/api")
