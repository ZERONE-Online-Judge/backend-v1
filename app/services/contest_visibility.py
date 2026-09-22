from app.models import ContestResourceAccess, ContestStatus, now_utc


def contest_has_ended(contest) -> bool:
    if contest.status == ContestStatus.SCHEDULE_TBD:
        return False
    return contest.status in {ContestStatus.ENDED, ContestStatus.FINALIZED, ContestStatus.ARCHIVED} or now_utc() >= contest.end_at


def contest_payload_for_view(contest, participant: dict | None = None) -> dict:
    payload = contest.model_dump(mode="json")
    # The overview and public problem workspace must not bypass notice access.
    if (
        contest_has_ended(contest)
        and contest.notice_access_after_end != ContestResourceAccess.PUBLIC
        and not participant
    ):
        payload["emergency_notice"] = None
    return payload
