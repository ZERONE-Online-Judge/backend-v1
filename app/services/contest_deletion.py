"""Remove a contest from the service without destroying historical data."""
from sqlalchemy import delete, select, update

from app.database import SessionLocal
from app.models import now_utc
from app.orm_models import (ContestRow, JudgeJobRow, MailQueueItemRow, NotificationRow,
    ParticipantPreviewSessionRow, TeamMemberRow, TeamSessionRow)
from app.services.errors import AppError, not_found


def delete_contest(contest_id: str, confirm_title: str):
    from app.services.store import _schedule_status
    with SessionLocal() as db:
        contest = db.scalar(select(ContestRow).where(ContestRow.contest_id == contest_id).with_for_update())
        if not contest:
            raise not_found("대회를 찾을 수 없습니다.")
        if confirm_title.strip() != contest.title.strip():
            raise AppError(422, "contest_title_mismatch", "삭제할 대회명을 정확하게 입력하세요.")
        now = now_utc()
        if _schedule_status(contest.status, contest.start_at, contest.end_at, now) == "running":
            raise AppError(409, "contest_running", "진행 중인 대회는 삭제할 수 없습니다. 대회를 종료한 뒤 삭제하세요.")
        busy = db.scalar(select(JudgeJobRow.judge_job_id).where(JudgeJobRow.contest_id == contest_id,
            JudgeJobRow.status.in_(["pending", "assigned", "running"])).limit(1))
        if busy:
            raise AppError(409, "contest_judging", "진행 중이거나 대기 중인 채점이 있습니다. 채점이 끝난 뒤 삭제하세요.")
        contest.deleted_at = now
        db.execute(update(TeamSessionRow).where(TeamSessionRow.contest_id == contest_id, TeamSessionRow.revoked_at.is_(None)).values(revoked_at=now))
        db.execute(update(TeamMemberRow).where(TeamMemberRow.contest_id == contest_id).values(active_sessions=0, session_revoked_at=now))
        db.execute(delete(ParticipantPreviewSessionRow).where(ParticipantPreviewSessionRow.contest_id == contest_id))
        db.execute(update(NotificationRow).where(NotificationRow.contest_id == contest_id, NotificationRow.deleted_at.is_(None)).values(deleted_at=now))
        db.execute(update(MailQueueItemRow).where(MailQueueItemRow.contest_id == contest_id, MailQueueItemRow.status == "pending").values(status="canceled"))
        db.commit()
        return {"deleted": True, "contest_id": contest_id, "title": contest.title}
