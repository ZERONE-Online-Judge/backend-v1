"""SQL-aggregated usage and operating metrics; only administrators can read them."""
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import Integer, cast, func, select
from sqlalchemy.orm import aliased

from app.database import SessionLocal
from app.models import now_utc
from app.orm_models import AccessLogRow, ContestQuestionRow, ContestRow, OperationalAuditLogRow, SubmissionRow, UsageEventRow
from app.services.errors import AppError
from app.services.usage_ingest import PAGE_LABELS, RETENTION_DAYS, SERVICES

KST = ZoneInfo("Asia/Seoul")
STAFF_AUDIENCES = ["operator", "service_master", "preview"]
AUDIENCES = {"anonymous": "비로그인", "member": "일반 회원", "participant": "참가자", "operator": "운영진", "service_master": "서비스 마스터", "preview": "참가자 미리보기"}


def _aware(value):
    return value.replace(tzinfo=timezone.utc) if value and value.tzinfo is None else value


def report_period(start: date | None, end: date | None):
    today = now_utc().astimezone(KST).date()
    end = end or today
    start = start or end - timedelta(days=6)
    if start > end or (end - start).days >= 366 or end > today or start < today - timedelta(days=RETENTION_DAYS):
        raise AppError(422, "invalid_analytics_period", "최근 395일 중 최대 366일을 선택해 주세요. 시작일은 종료일보다 늦을 수 없습니다.")
    return datetime.combine(start, time.min, KST).astimezone(timezone.utc), datetime.combine(end + timedelta(days=1), time.min, KST).astimezone(timezone.utc)


def usage_report(start: date | None, end: date | None, contest_id: str | None, service: str | None, audience: str):
    since, until = report_period(start, end)
    now = now_utc()
    common = []
    if contest_id:
        common.append(UsageEventRow.contest_id == contest_id)
    if service:
        common.append(UsageEventRow.service == service)
    if audience == "visitors":
        common.append(UsageEventRow.audience.not_in(STAFF_AUDIENCES))
    elif audience == "staff":
        common.append(UsageEventRow.audience.in_(STAFF_AUDIENCES))
    elif audience == "anonymous":
        common.append(UsageEventRow.audience == "anonymous")
    elif audience == "signed_in":
        common.append(UsageEventRow.account_key.is_not(None))
    filters = [*common, UsageEventRow.created_at >= since, UsageEventRow.created_at < until]
    with SessionLocal() as db:
        if contest_id and not db.get(ContestRow, contest_id):
            raise AppError(404, "not_found", "대회를 찾을 수 없습니다.")
        def summary(where):
            row = db.execute(select(func.count(), func.count(func.distinct(UsageEventRow.visitor_key)), func.count(func.distinct(UsageEventRow.visit_key)), func.count(func.distinct(UsageEventRow.account_key)), func.coalesce(func.sum(UsageEventRow.active_seconds), 0)).select_from(UsageEventRow).where(*where)).one()
            return dict(zip(["views", "visitors", "visits", "signed_in_users", "active_seconds"], [int(value) for value in row]))
        current = summary(filters)
        previous_since = since - (until - since)
        previous = summary([*common, UsageEventRow.created_at >= previous_since, UsageEventRow.created_at < min(until, now) - (until - since)])
        first_event = _aware(db.scalar(select(func.min(UsageEventRow.created_at))))
        recent_filters = [*common, UsageEventRow.last_seen_at >= now - timedelta(minutes=5)]
        active_visitors = int(db.scalar(select(func.count(func.distinct(UsageEventRow.visitor_key))).where(*recent_filters)))
        historic = aliased(UsageEventRow)
        seen_before = select(historic.event_id).where(historic.visitor_key == UsageEventRow.visitor_key, historic.created_at < since).exists()
        returning = int(db.scalar(select(func.count(func.distinct(UsageEventRow.visitor_key))).where(*filters, seen_before)))
        current.update(active_visitors=active_visitors, returning_visitors=returning, new_visitors=current["visitors"] - returning, average_active_seconds=round(current["active_seconds"] / current["views"], 1) if current["views"] else 0)

        sqlite = db.bind.dialect.name == "sqlite"
        local = UsageEventRow.created_at
        if sqlite:
            day = func.strftime("%Y-%m-%d", local, "+9 hours")
            hour = cast(func.strftime("%H", local, "+9 hours"), Integer)
            weekday = (cast(func.strftime("%w", local, "+9 hours"), Integer) + 6) % 7
            bucket = func.strftime("%Y-%m-%dT%H:00:00+09:00", local, "+9 hours") if until - since == timedelta(days=1) else day
        else:
            local = func.timezone("Asia/Seoul", local)
            day = func.to_char(local, "YYYY-MM-DD")
            hour = cast(func.extract("hour", local), Integer)
            weekday = cast(func.extract("isodow", local), Integer) - 1
            bucket = func.to_char(local, 'YYYY-MM-DD"T"HH24:00:00+09:00') if until - since == timedelta(days=1) else day
        timeline_rows = db.execute(select(bucket, func.count(), func.count(func.distinct(UsageEventRow.visitor_key)), func.count(func.distinct(UsageEventRow.visit_key))).where(*filters).group_by(bucket).order_by(bucket)).all()
        values = {key: {"views": views, "visitors": visitors, "visits": visits} for key, views, visitors, visits in timeline_rows}
        timeline = []
        hourly = until - since == timedelta(days=1)
        cursor = since.astimezone(KST)
        while cursor < until:
            key = cursor.isoformat() if hourly else cursor.date().isoformat()
            timeline.append({"period": key, "future": cursor > now, **values.get(key, {"views": 0, "visitors": 0, "visits": 0})})
            cursor += timedelta(hours=1) if hourly else timedelta(days=1)
        hours = {int(h): int(n) for h, n in db.execute(select(hour, func.count()).where(*filters).group_by(hour))}
        heat = {(int(d), int(h)): int(n) for d, h, n in db.execute(select(weekday, hour, func.count()).where(*filters).group_by(weekday, hour))}

        def breakdown(column, labels=None, limit=30):
            rows = db.execute(select(column, func.count(), func.count(func.distinct(UsageEventRow.visitor_key)), func.coalesce(func.sum(UsageEventRow.active_seconds), 0)).where(*filters).group_by(column).order_by(func.count().desc(), column).limit(limit)).all()
            return [{"key": key, "label": (labels or {}).get(key, key), "views": count, "visitors": visitors, "active_seconds": seconds} for key, count, visitors, seconds in rows]
        contests = db.execute(select(UsageEventRow.contest_id, func.count(), func.count(func.distinct(UsageEventRow.visitor_key)), func.count(func.distinct(UsageEventRow.visit_key))).where(*filters, UsageEventRow.contest_id.is_not(None)).group_by(UsageEventRow.contest_id).order_by(func.count().desc(), UsageEventRow.contest_id).limit(30)).all()
        names = dict(db.execute(select(ContestRow.contest_id, ContestRow.title).where(ContestRow.contest_id.in_([row[0] for row in contests]))).all()) if contests else {}
        contest_rows = [{"contest_id": cid, "title": names.get(cid, "삭제된 대회"), "views": count, "visitors": visitors, "visits": visits} for cid, count, visitors, visits in contests]
        operations = operational_metrics(db, since, until, contest_id)
        return {
            "generated_at": now.isoformat(), "timezone": "Asia/Seoul", "retention_days": RETENTION_DAYS,
            "first_event_at": first_event.isoformat() if first_event else None,
            "period": {"start": since.astimezone(KST).date().isoformat(), "end": (until.astimezone(KST) - timedelta(days=1)).date().isoformat(), "interval": "hour" if hourly else "day"},
            "filters": {"contest_id": contest_id, "service": service, "audience": audience},
            "summary": current, "previous": previous, "comparison_available": bool(first_event and first_event <= previous_since),
            "timeline": timeline, "hours": [{"hour": h, "views": hours.get(h, 0)} for h in range(24)],
            "heatmap": [{"weekday": d, "hour": h, "views": heat.get((d, h), 0)} for d in range(7) for h in range(24)],
            "services": breakdown(UsageEventRow.service, SERVICES), "pages": breakdown(UsageEventRow.page_key, PAGE_LABELS),
            "audiences": breakdown(UsageEventRow.audience, AUDIENCES), "devices": breakdown(UsageEventRow.device, {"desktop": "데스크톱", "mobile": "모바일", "tablet": "태블릿"}),
            "browsers": breakdown(UsageEventRow.browser), "referrers": breakdown(UsageEventRow.referrer_host, {"direct": "직접 방문 / 출처 없음", "internal": "ZOJ 내부 이동", "external": "기타 외부 사이트"}),
            "contests": contest_rows, "operations": operations,
        }


def operational_metrics(db, since, until, contest_id):
    def period(model, timestamp):
        return [timestamp >= since, timestamp < until] + ([model.contest_id == contest_id] if contest_id else [])
    submissions = period(SubmissionRow, SubmissionRow.submitted_at)
    def grouped(column):
        return [{"key": key, "count": count} for key, count in db.execute(select(column, func.count()).where(*submissions).group_by(column).order_by(func.count().desc(), column))]
    outcomes = grouped(SubmissionRow.status)
    kinds = grouped(SubmissionRow.submission_kind)
    login_counts = dict(db.execute(select(AccessLogRow.event_type, func.count()).where(*period(AccessLogRow, AccessLogRow.created_at), AccessLogRow.event_type.in_(["general_login", "participant_login", "login_failed", "session_conflict"])).group_by(AccessLogRow.event_type)).all())
    failures = int(db.scalar(select(func.count()).select_from(OperationalAuditLogRow).where(*period(OperationalAuditLogRow, OperationalAuditLogRow.created_at), OperationalAuditLogRow.status_code >= 400)))
    questions = int(db.scalar(select(func.count()).select_from(ContestQuestionRow).where(*period(ContestQuestionRow, ContestQuestionRow.created_at))))
    if db.bind.dialect.name == "sqlite":
        elapsed = (func.julianday(SubmissionRow.status_updated_at) - func.julianday(SubmissionRow.submitted_at)) * 86400
    else:
        elapsed = func.extract("epoch", SubmissionRow.status_updated_at - SubmissionRow.submitted_at)
    delay = db.scalar(select(func.avg(elapsed)).where(*submissions, SubmissionRow.status.not_in(["waiting", "preparing", "judging"]), elapsed >= 0))
    return {"submissions": sum(row["count"] for row in outcomes), "outcomes": outcomes, "languages": grouped(SubmissionRow.language), "submission_kinds": kinds,
        "login_successes": login_counts.get("general_login", 0) + login_counts.get("participant_login", 0), "login_failures": login_counts.get("login_failed", 0), "session_conflicts": login_counts.get("session_conflict", 0),
        "operation_failures": failures, "questions": questions, "average_judge_seconds": round(float(delay), 2) if delay is not None else None}
