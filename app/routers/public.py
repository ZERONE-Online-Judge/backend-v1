from datetime import timedelta

from fastapi import APIRouter, Request
from pydantic import BaseModel, EmailStr, Field

from app.models import now_utc
from app.settings import settings
from app.services.errors import not_found
from app.services.mail_templates import render_branded_email
from app.services.responses import ok, page
from app.services.store import store

router = APIRouter(tags=["public"])


class ContactInquiryCreateRequest(BaseModel):
    title: str = Field(min_length=1, max_length=255)
    sender_name: str = Field(min_length=1, max_length=120)
    sender_email: EmailStr
    body: str = Field(min_length=1, max_length=10000)


def _service_master_emails() -> list[str]:
    emails = []
    seen = set()
    for account in store.staff_accounts.values():
        if not account.is_service_master:
            continue
        email = str(account.email).strip().lower()
        if email and email not in seen:
            seen.add(email)
            emails.append(email)
    return emails


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


@router.post("/public/contact-inquiries")
async def create_contact_inquiry(payload: ContactInquiryCreateRequest, request: Request):
    inquiry = store.create_contact_inquiry(
        payload.title.strip(),
        payload.sender_name.strip(),
        str(payload.sender_email).strip(),
        payload.body.strip(),
    )
    subject = f"[ZOJ] 서비스 문의 접수: {inquiry.title}"
    body_text = "\n".join(
        [
            "서비스 문의가 접수되었습니다.",
            "",
            f"문의 ID: {inquiry.contact_inquiry_id}",
            f"제목: {inquiry.title}",
            f"이름: {inquiry.sender_name}",
            f"이메일: {inquiry.sender_email}",
            f"접수 시각: {inquiry.created_at.isoformat()}",
            "",
            "문의 본문:",
            inquiry.body,
            "",
            "서비스 관리자 페이지에서 답변을 등록하면 문의자에게 이메일이 발송됩니다.",
        ]
    )
    for email in _service_master_emails():
        store.enqueue_mail(
            "contact_inquiry_created",
            email,
            subject,
            body_text,
            render_branded_email(
                title="서비스 문의가 접수되었습니다",
                preheader=inquiry.title,
                body=[
                    "서비스 문의가 접수되었습니다.",
                    "서비스 관리자 페이지에서 답변을 등록하면 문의자에게 이메일이 발송됩니다.",
                ],
                meta=[
                    ("문의 ID", inquiry.contact_inquiry_id),
                    ("제목", inquiry.title),
                    ("이름", inquiry.sender_name),
                    ("이메일", str(inquiry.sender_email)),
                    ("접수 시각", inquiry.created_at.isoformat()),
                ],
                sections=[("문의 본문", inquiry.body)],
            ),
        )
    return ok(request, inquiry.model_dump(mode="json"))


@router.get("/public/judge-status")
async def judge_status(request: Request):
    nodes = list(store.judge_nodes.values())
    active_since = now_utc() - timedelta(seconds=max(5, settings.judge_node_active_window_seconds))
    active_nodes = [node for node in nodes if node.last_heartbeat_at >= active_since]
    running_jobs = sum(node.running_job_count for node in active_nodes)
    return ok(
        request,
        {
            "active_node_count": len(active_nodes),
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
