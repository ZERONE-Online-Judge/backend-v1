import asyncio

from fastapi import APIRouter, Request
from pydantic import BaseModel

from app.models import SubmissionStatus
from app.settings import settings
from app.services.errors import AppError, not_found
from app.services.responses import ok
from app.services.store import store

router = APIRouter(tags=["internal-judge"])


class RegisterNodeRequest(BaseModel):
    node_name: str
    node_secret: str
    total_slots: int = 10
    agent_version: str = "0.1.0"


class HeartbeatRequest(BaseModel):
    node_secret: str
    total_slots: int
    free_slots: int
    running_job_count: int


class ClaimRequest(BaseModel):
    node_secret: str
    max_count: int = 1
    wait_seconds: float = 0.0


class ResultRequest(BaseModel):
    node_secret: str
    lease_token: str
    final_status: SubmissionStatus
    awarded_score: int | None = None
    compile_message: str | None = None
    judge_message: str | None = None
    failed_testcase_order: int | None = None


class ProgressRequest(BaseModel):
    node_secret: str
    lease_token: str
    status: SubmissionStatus
    progress_current: int | None = None
    progress_total: int | None = None


@router.post("/internal/judge/nodes/register")
async def register_node(payload: RegisterNodeRequest, request: Request):
    try:
        node = await asyncio.to_thread(store.register_node, payload.node_name, payload.node_secret, payload.total_slots)
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
        )
    except ValueError:
        raise AppError(403, "node_secret_invalid", "Judge node secret is invalid.")
    if not node:
        raise not_found()
    return ok(request, node.model_dump(mode="json"))


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
            payload.awarded_score,
            payload.compile_message,
            payload.judge_message,
            payload.failed_testcase_order,
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
