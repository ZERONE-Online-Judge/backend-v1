from datetime import date
import logging
import re
from typing import Literal
from uuid import UUID

from fastapi import APIRouter, Request, Response
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.exc import SQLAlchemyError

from app.services.authz import bearer_token, require_service_master
from app.services.responses import ok
from app.services.usage_ingest import record_usage
from app.services.usage_reports import usage_report
from app.settings import settings

router = APIRouter(tags=["analytics"])
logger = logging.getLogger(__name__)


class UsageEventRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    event_id: UUID
    visitor_id: UUID
    visit_id: UUID
    path: str = Field(min_length=1, max_length=256)
    referrer: str | None = Field(default=None, max_length=2048)
    active_seconds: int = Field(default=0, ge=0, le=86400)


@router.post("/public/usage", status_code=202)
def collect_usage(payload: UsageEventRequest, request: Request):
    if request.headers.get("dnt") == "1" or request.headers.get("sec-gpc") == "1":
        return ok(request, {"accepted": False})
    origin = request.headers.get("origin")
    if origin and origin.rstrip("/") not in {settings.public_base_url.rstrip("/"), *[value.strip().rstrip("/") for value in settings.cors_allow_origins.split(",")]}:
        if not settings.cors_allow_origin_regex or not re.fullmatch(settings.cors_allow_origin_regex, origin):
            return ok(request, {"accepted": False})
    try:
        network = request.headers.get("x-real-ip") or (request.client.host if request.client else None)
        accepted = record_usage(payload, request.headers.get("user-agent", "")[:1024], bearer_token(request), network)
    except SQLAlchemyError:
        logger.exception("Usage collection failed")
        accepted = False
    return ok(request, {"accepted": accepted})


@router.get("/admin/analytics")
def analytics(request: Request, response: Response,
    start: date | None = None, end: date | None = None,
    contest_id: UUID | None = None,
    service: Literal["public", "contest", "operator", "admin"] | None = None,
    audience: Literal["all", "visitors", "staff", "anonymous", "signed_in"] = "all",
):
    require_service_master(request)
    response.headers["Cache-Control"] = "private, no-store"
    return ok(request, usage_report(start, end, str(contest_id) if contest_id else None, service, audience))
