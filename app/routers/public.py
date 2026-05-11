from fastapi import APIRouter, Request

from app.services.errors import not_found
from app.services.responses import ok, page
from app.services.store import store

router = APIRouter(tags=["public"])


@router.get("/public/home")
async def home(request: Request):
    contests = store.visible_public_contests()
    emergency = next((notice for notice in store.service_notices.values() if notice.emergency), None)
    return ok(
        request,
        {
            "hero": {
                "title": "Zerone Online Judge",
                "subtitle": "대회 운영, 제출, 채점 큐, 스코어보드를 한 흐름으로 관리합니다.",
            },
            "active_contest_count": len(contests),
            "emergency_notice": emergency.model_dump(mode="json") if emergency else None,
        },
    )


@router.get("/public/contests")
async def contests(request: Request):
    return page(request, [contest.model_dump(mode="json") for contest in store.visible_public_contests()])


@router.get("/public/contests/{contest_id}")
async def contest_detail(contest_id: str, request: Request):
    contest = store.get_public_contest(contest_id)
    if not contest:
        raise not_found()
    return ok(
        request,
        {
            "contest": contest.model_dump(mode="json"),
            "divisions": [division.model_dump(mode="json") for division in store.contest_divisions(contest_id)],
        },
    )


@router.get("/public/service-notices")
async def service_notices(request: Request):
    notices = [notice.model_dump(mode="json") for notice in store.service_notices.values()]
    notices.sort(key=lambda item: item.get("published_at", ""), reverse=True)
    return page(request, notices)


@router.get("/public/service-notices/{notice_id}")
async def service_notice_detail(notice_id: str, request: Request):
    notice = store.service_notices.get(notice_id)
    if not notice:
        raise not_found()
    return ok(request, notice.model_dump(mode="json"))


@router.get("/public/judge-status")
async def judge_status(request: Request):
    running_jobs = sum(node.running_job_count for node in store.judge_nodes.values())
    return ok(
        request,
        {
            "active_node_count": len(store.judge_nodes),
            "total_running_jobs": running_jobs,
            "total_queue_depth": len([job for job in store.judge_jobs.values() if job.status == "pending"]),
            "allocation_policy": "internal claim FIFO",
        },
    )


@router.get("/public/rules")
async def rules(request: Request):
    return ok(
        request,
        {
            "sections": [
                {"anchor": "login", "title": "참가자 이메일 OTP 로그인"},
                {"anchor": "submit", "title": "대회 종료 전까지 제출 가능"},
                {"anchor": "freeze", "title": "종료 1시간 전 프리즈"},
            ]
        },
    )
