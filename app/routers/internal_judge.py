import asyncio

from fastapi import APIRouter, Request
from pydantic import BaseModel, Field, field_validator

from app.models import JUDGE_FINAL_STATUSES, JUDGE_PROGRESS_STATUSES, SubmissionStatus
from app.settings import settings
from app.services.errors import AppError, not_found
from app.services.responses import ok
from app.services.store import store

router = APIRouter(tags=["internal-judge"])


class NodeCredentialRequest(BaseModel):
    node_secret: str = Field(min_length=1, max_length=1024)


class RegisterNodeRequest(NodeCredentialRequest):
    node_name: str = Field(min_length=1, max_length=120)
    total_slots: int = Field(default=10, ge=1, le=1024)
    agent_version: str = Field(default="0.1.0", max_length=64)


class HeartbeatRequest(NodeCredentialRequest):
    total_slots: int = Field(ge=1, le=1024)
    free_slots: int = Field(ge=0, le=1024)
    running_job_count: int = Field(ge=0, le=1024)
    agent_version: str | None = Field(default=None, max_length=64)


class ClaimRequest(NodeCredentialRequest):
    max_count: int = Field(default=1, ge=1, le=100)
    wait_seconds: float = Field(default=0.0, ge=0, le=60)


class ResultRequest(NodeCredentialRequest):
    lease_token: str = Field(min_length=1, max_length=128)
    final_status: SubmissionStatus
    compile_message: str | None = None
    judge_message: str | None = None
    failed_testcase_order: int | None = None
    runtime_ms: int | None = None
    memory_kb: int | None = None

    @field_validator("final_status")
    @classmethod
    def terminal_status_only(cls, value):
        if value not in JUDGE_FINAL_STATUSES:
            raise ValueError("Result must be a terminal judge status")
        return value


class ProgressRequest(NodeCredentialRequest):
    lease_token: str = Field(min_length=1, max_length=128)
    status: SubmissionStatus
    progress_current: int | None = None
    progress_total: int | None = None

    @field_validator("status")
    @classmethod
    def progress_status_only(cls, value):
        if value not in JUDGE_PROGRESS_STATUSES:
            raise ValueError("Progress must be preparing or judging")
        return value


class LeaseRenewRequest(NodeCredentialRequest):
    lease_token: str = Field(min_length=1, max_length=128)


class AgentLogItem(BaseModel):
    level: str = Field(default="info", max_length=16)
    message: str = Field(max_length=8000)


class AgentLogsRequest(NodeCredentialRequest):
    logs: list[AgentLogItem] = Field(max_length=300)


@router.post("/internal/judge/nodes/register")
async def register_node(payload: RegisterNodeRequest, request: Request):
    try:
        node = await asyncio.to_thread(
            store.register_node,
            payload.node_name,
            payload.node_secret,
            payload.total_slots,
            payload.agent_version,
        )
    except ValueError:
        raise AppError(403, "node_secret_invalid", "Judge node secret is invalid.")
    return ok(request, {"judge_node_id": node.judge_node_id, "heartbeat_interval_seconds": 2})


@router.post("/internal/judge/nodes/{node_id}/heartbeat")
async def heartbeat(node_id: str, payload: HeartbeatRequest, request: Request):
    try:
        node = await asyncio.to_thread(
            store.update_node_heartbeat,
            node_id,
            payload.node_secret,
            payload.total_slots,
            payload.free_slots,
            payload.running_job_count,
            payload.agent_version,
        )
    except ValueError:
        raise AppError(403, "node_secret_invalid", "Judge node secret is invalid.")
    if not node:
        raise not_found()
    return ok(request, node.model_dump(mode="json"))


@router.post("/internal/judge/nodes/{node_id}/logs")
async def append_node_logs(node_id: str, payload: AgentLogsRequest, request: Request):
    try:
        accepted = await asyncio.to_thread(
            store.append_judge_agent_logs,
            node_id,
            payload.node_secret,
            [item.model_dump() for item in payload.logs],
        )
    except ValueError:
        raise AppError(403, "node_secret_invalid", "Judge node secret is invalid.")
    if accepted is None:
        raise not_found()
    return ok(request, {"accepted": accepted})


@router.post("/internal/judge/nodes/{node_id}/assignments:claim")
async def claim(node_id: str, payload: ClaimRequest, request: Request):
    deadline = min(max(payload.wait_seconds, 0.0), settings.judge_claim_max_wait_seconds)
    started = asyncio.get_running_loop().time()
    while True:
        try:
            jobs = await asyncio.to_thread(store.claim_jobs, node_id, payload.node_secret, payload.max_count)
        except ValueError:
            raise AppError(403, "node_secret_invalid", "Judge node secret is invalid.")
        if jobs is None:
            raise not_found()
        if jobs or deadline <= 0:
            break
        elapsed = asyncio.get_running_loop().time() - started
        if elapsed >= deadline:
            break
        await asyncio.sleep(min(settings.judge_claim_poll_interval_seconds, deadline - elapsed))
    if jobs is None:
        raise not_found()
    return ok(request, {"jobs": jobs})


@router.post("/internal/judge/jobs/{job_id}/result")
async def report_result(job_id: str, payload: ResultRequest, request: Request):
    try:
        result = await asyncio.to_thread(
            store.report_judge_result,
            job_id,
            payload.node_secret,
            payload.lease_token,
            payload.final_status,
            payload.compile_message,
            payload.judge_message,
            payload.failed_testcase_order,
            payload.runtime_ms,
            payload.memory_kb,
        )
    except ValueError as error:
        if "lease mismatch" in str(error):
            raise AppError(409, "lease_conflict", "Lease token mismatch.")
        raise AppError(403, "node_secret_invalid", "Judge node secret is invalid.")
    if not result:
        raise not_found()
    submission, job = result
    return ok(request, {"accepted": True, "submission": submission.model_dump(mode="json"), "job": job.model_dump(mode="json")})


@router.post("/internal/judge/jobs/{job_id}/progress")
async def report_progress(job_id: str, payload: ProgressRequest, request: Request):
    try:
        result = await asyncio.to_thread(
            store.update_judge_progress,
            job_id,
            payload.node_secret,
            payload.lease_token,
            payload.status,
            payload.progress_current,
            payload.progress_total,
        )
    except ValueError as error:
        if "lease mismatch" in str(error):
            raise AppError(409, "lease_conflict", "Lease token mismatch.")
        raise AppError(403, "node_secret_invalid", "Judge node secret is invalid.")
    if not result:
        raise not_found()
    submission, job = result
    return ok(request, {"accepted": True, "submission": submission.model_dump(mode="json"), "job": job.model_dump(mode="json")})


@router.post("/internal/judge/jobs/{job_id}/lease:renew")
async def renew_lease(job_id: str, payload: LeaseRenewRequest, request: Request):
    try:
        job = await asyncio.to_thread(
            store.renew_judge_lease,
            job_id,
            payload.node_secret,
            payload.lease_token,
        )
    except ValueError as error:
        if "lease mismatch" in str(error):
            raise AppError(409, "lease_conflict", "Lease token mismatch.")
        raise AppError(403, "node_secret_invalid", "Judge node secret is invalid.")
    if not job:
        raise not_found()
    return ok(request, {"accepted": True, "job": job.model_dump(mode="json")})
