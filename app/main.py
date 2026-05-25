import json
import re
from collections.abc import Mapping
from typing import Any
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
AUDIT_BODY_MAX_BYTES = 256 * 1024
AUDIT_REDACTED_KEYS = {
    "access_token",
    "node_secret",
    "password",
    "refresh_token",
    "source_code",
    "token",
}
AUDIT_TRUNCATE_STRING_LENGTH = 4000
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


def _audit_contest_title(contest_id: str | None) -> str | None:
    if not contest_id:
        return None
    contest = store.contests.get(contest_id)
    return contest.title if contest else None


def _audit_value(value: Any) -> Any:
    if isinstance(value, str):
        if len(value) > AUDIT_TRUNCATE_STRING_LENGTH:
            omitted = len(value) - AUDIT_TRUNCATE_STRING_LENGTH
            return f"{value[:AUDIT_TRUNCATE_STRING_LENGTH]}...(생략 {omitted}자)"
        return value
    if isinstance(value, list):
        if len(value) > 12:
            return [_audit_value(item) for item in value[:12]] + [
                f"...외 {len(value) - 12}개",
            ]
        return [_audit_value(item) for item in value]
    if isinstance(value, Mapping):
        return _audit_mapping(value)
    return value


def _audit_mapping(data: Mapping[str, Any]) -> dict[str, Any]:
    sanitized: dict[str, Any] = {}
    for key, value in data.items():
        normalized_key = str(key)
        if normalized_key.lower() in AUDIT_REDACTED_KEYS:
            sanitized[normalized_key] = "기록 제외"
        else:
            sanitized[normalized_key] = _audit_value(value)
    return sanitized


def _audit_changes(data: Any, before: Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
    if not isinstance(data, Mapping):
        return []
    mapped = _audit_mapping(data)
    before_mapped = _audit_mapping(before) if before else {}
    changes: list[dict[str, Any]] = []
    for key, value in mapped.items():
        item = {"field": str(key), "new": value}
        if key in before_mapped:
            item["old"] = before_mapped[key]
        changes.append(item)
    return changes


def _audit_model_dump(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if hasattr(value, "model_dump"):
        dumped = value.model_dump(mode="json")
        return dumped if isinstance(dumped, dict) else None
    return None


def _audit_existing_values(path: str) -> dict[str, Any] | None:
    contest_match = re.match(r"^/api/operator/contests/([^/]+)/settings$", path)
    if contest_match:
        return _audit_model_dump(store.contests.get(contest_match.group(1)))

    division_match = re.match(r"^/api/operator/contests/[^/]+/divisions/([^/]+)$", path)
    if division_match:
        return _audit_model_dump(store.divisions.get(division_match.group(1)))

    notice_match = re.match(r"^/api/operator/contests/[^/]+/notices/([^/]+)$", path)
    if notice_match:
        return _audit_model_dump(store.contest_notices.get(notice_match.group(1)))

    team_match = re.match(r"^/api/operator/contests/[^/]+/participants/([^/]+)$", path)
    if team_match:
        return _audit_model_dump(store.teams.get(team_match.group(1)))

    problem_match = re.match(r"^/api/operator/contests/[^/]+/problems/([^/]+)$", path)
    if problem_match:
        return _audit_model_dump(store.problems.get(problem_match.group(1)))

    service_notice_match = re.match(r"^/api/admin/service-notices/([^/]+)$", path)
    if service_notice_match:
        return _audit_model_dump(store.service_notices.get(service_notice_match.group(1)))

    return None


def _audit_path_entities(path: str) -> dict[str, str]:
    patterns = [
        (r"^/api/operator/contests/([^/]+)/settings$", ["contest_id"]),
        (r"^/api/operator/contests/([^/]+)/divisions/([^/]+)$", ["contest_id", "division_id"]),
        (r"^/api/operator/contests/([^/]+)/operators/([^/]+)$", ["contest_id", "operator_email"]),
        (r"^/api/operator/contests/([^/]+)/notices/([^/]+)$", ["contest_id", "notice_id"]),
        (r"^/api/operator/contests/([^/]+)/boards/([^/]+)$", ["contest_id", "question_id"]),
        (
            r"^/api/operator/contests/([^/]+)/boards/([^/]+)/answers/([^/]+)$",
            ["contest_id", "question_id", "answer_id"],
        ),
        (r"^/api/operator/contests/([^/]+)/participants/([^/]+)$", ["contest_id", "participant_team_id"]),
        (
            r"^/api/operator/contests/([^/]+)/participants/([^/]+)/members/([^/]+)(?:/sessions:revoke)?$",
            ["contest_id", "participant_team_id", "team_member_id"],
        ),
        (r"^/api/operator/contests/([^/]+)/problems/([^/]+)$", ["contest_id", "problem_id"]),
        (
            r"^/api/operator/contests/([^/]+)/problems/([^/]+)/test-submissions$",
            ["contest_id", "problem_id"],
        ),
        (
            r"^/api/operator/contests/([^/]+)/problems/([^/]+)/assets/([^/]+)$",
            ["contest_id", "problem_id", "asset_id"],
        ),
        (
            r"^/api/operator/contests/([^/]+)/problems/([^/]+)/testcase-sets/([^/]+)$",
            ["contest_id", "problem_id", "testcase_set_id"],
        ),
        (
            r"^/api/operator/contests/([^/]+)/problems/([^/]+)/testcase-sets/([^/]+)/testcases/([^/]+)$",
            ["contest_id", "problem_id", "testcase_set_id", "testcase_id"],
        ),
        (r"^/api/admin/contests/([^/]+)/operators$", ["contest_id"]),
        (r"^/api/admin/contests/([^/]+)/divisions$", ["contest_id"]),
        (r"^/api/admin/service-notices/([^/]+)$", ["service_notice_id"]),
        (r"^/api/admin/contact-inquiries/([^/]+)/answer$", ["contact_inquiry_id"]),
    ]
    for pattern, keys in patterns:
        match = re.match(pattern, path)
        if match:
            return dict(zip(keys, match.groups(), strict=False))
    return {}


async def _audit_request_payload(request: Request) -> tuple[Request, dict[str, Any]]:
    content_type = request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
    content_length = int(request.headers.get("content-length") or "0")
    if content_type != "application/json" or content_length > AUDIT_BODY_MAX_BYTES:
        return request, {}

    body = await request.body()

    async def receive():
        return {"type": "http.request", "body": body, "more_body": False}

    replay_request = Request(request.scope, receive)
    if not body:
        return replay_request, {}

    try:
        parsed = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return replay_request, {}

    return replay_request, {
        "body": _audit_value(parsed),
    }


def _client_ip(request: Request) -> str | None:
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",", 1)[0].strip() or None
    return request.client.host if request.client else None


@app.middleware("http")
async def operational_audit_middleware(request: Request, call_next):
    path = request.url.path
    scope = _audit_scope(path)
    payload_details: dict[str, Any] = {}
    if request.method.upper() in AUDITED_METHODS and scope:
        request, payload_details = await _audit_request_payload(request)
        payload_details["entities"] = _audit_path_entities(path)
        payload_details["changes"] = _audit_changes(
            payload_details.get("body"),
            _audit_existing_values(path),
        )

    response = await call_next(request)
    if request.method.upper() not in AUDITED_METHODS or not scope:
        return response

    try:
        contest_id = _audit_contest_id(path)
        details: dict[str, Any] = {
            **payload_details,
            "contest_title": _audit_contest_title(contest_id),
        }
        if request.url.query:
            details["query"] = request.url.query
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
            contest_id=contest_id,
            client_ip=_client_ip(request),
            user_agent=request.headers.get("user-agent"),
            request_id=getattr(request.state, "request_id", None),
            details={key: value for key, value in details.items() if value not in (None, "", [], {})},
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
