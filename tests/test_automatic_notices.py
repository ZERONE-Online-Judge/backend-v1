import os
from datetime import datetime, timedelta, timezone
from importlib import import_module
from uuid import uuid4

import pytest

os.environ.setdefault("ENABLE_DEMO_SEED", "true")
os.environ.setdefault("ALLOW_EMPTY_OTP", "true")

from app.models import ContestStatus
from app.orm_models import ContestNoticeRow
from app.services.automatic_notices import (
    KST,
    LEGACY_SCHEDULE_FOOTER,
    compact_automatic_notice,
    legacy_scheduled_notice,
    scheduled_notice_copy,
    time_update_notice_body,
)
from app.services.store import store
from app.settings import settings


def at(day=5, hour=21, minute=4, year=2026):
    return datetime(year, 9, day, hour, minute, tzinfo=KST)


def test_time_change_mentions_only_changed_minutes():
    body = time_update_notice_body("2026 ZOAC", [
        ("start_at", at(hour=18, minute=4).replace(second=23), at(hour=18, minute=4)),
        ("freeze_at", at(), at(minute=30)),
        ("end_at", at(hour=22), at(hour=22)),
    ])
    assert body == "2026 ZOAC의 프리즈 시간이 21시 04분에서 21시 30분으로 변경되었습니다."
    assert time_update_notice_body("대회", [("start_at", at(), at().astimezone(timezone.utc))]) == ""


def test_dates_are_only_included_when_the_date_changes():
    assert time_update_notice_body("대회", [("start_at", at(), at(day=6))]) == "대회의 시작 시간이 09월 05일 21시 04분에서 09월 06일 21시 04분으로 변경되었습니다."
    assert time_update_notice_body("대회", [("end_at", at(), at(year=2027))]) == "대회의 종료 시간이 2026년 09월 05일 21시 04분에서 2027년 09월 05일 21시 04분으로 변경되었습니다."


def test_multiple_changes_use_short_separate_sentences():
    assert time_update_notice_body("대회", [
        ("start_at", at(hour=18), at(hour=19)),
        ("end_at", at(hour=22), at(hour=23)),
    ]) == "대회의 시작 시간이 18시 04분에서 19시 04분으로 변경되었습니다.\n종료 시간이 22시 04분에서 23시 04분으로 변경되었습니다."


LEGACY_TIME = "\n".join([
    "2026 ZOAC 운영시간이 변경되었습니다.", "",
    "- 오픈: 26년 09월 05일 18시 04분 KST -> 26년 09월 05일 18시 04분 KST",
    "- 프리즈: 26년 09월 05일 21시 04분 KST -> 26년 09월 05일 21시 30분 KST",
    "- 마감: 26년 09월 05일 22시 04분 KST -> 26년 09월 05일 22시 04분 KST",
    "", LEGACY_SCHEDULE_FOOTER,
])


def test_existing_time_notice_is_compact_and_keeps_manual_additions():
    expected = "2026 ZOAC의 프리즈 시간이 21시 04분에서 21시 30분으로 변경되었습니다."
    assert compact_automatic_notice(LEGACY_TIME) == expected
    assert compact_automatic_notice(LEGACY_TIME + "\n\n문의는 운영진에게 해 주세요.") == expected + "\n\n문의는 운영진에게 해 주세요."
    assert compact_automatic_notice(expected) == expected


@pytest.mark.parametrize("body", [None, "", "직접 작성한 긴급 안내입니다.\n내용을 유지하세요.", LEGACY_TIME.replace("26년 09월", "잘못된 날짜"), LEGACY_TIME.replace(LEGACY_SCHEDULE_FOOTER, "운영진이 직접 덧붙인 내용")])
def test_manual_or_unrecognized_notices_are_unchanged(body):
    assert compact_automatic_notice(body) == body


def legacy_event(target, target_at, remaining=None):
    time = target_at.astimezone(KST)
    formatted = f"{time.year}. {time.month}. {time.day}. {time:%H:%M} KST"
    label = "프리즈" if target == "freeze" else "종료"
    if remaining:
        first = f"{'스코어보드 프리즈' if target == 'freeze' else '대회 종료'}까지 {remaining} 남았습니다."
        last = "- 프리즈 이후 제출 결과는 대회 종료 전까지 스코어보드에 반영되지 않을 수 있습니다." if target == "freeze" else "- 종료 이후에는 제출이 제한될 수 있으니 남은 시간을 확인해 주세요."
    else:
        first = "스코어보드가 프리즈되었습니다." if target == "freeze" else "대회가 종료되었습니다."
        last = "- 프리즈 이후 제출 결과는 대회 종료 전까지 공개 스코어보드에 반영되지 않을 수 있습니다." if target == "freeze" else "- 종료 이후에는 제출이 제한됩니다."
    return f"{first}\n\n- {label} 시각: {formatted}\n{last}"


@pytest.mark.parametrize("target", ["freeze", "end"])
@pytest.mark.parametrize("remaining", ["30분", "10분", "5분", "1분", None])
def test_all_scheduled_templates_and_existing_notices_are_concise(target, remaining):
    title, body = scheduled_notice_copy(target, remaining)
    assert "\n" not in body and len(body) < 70
    assert "KST" not in body and "시각:" not in body
    legacy = legacy_event(target, at(), remaining)
    assert legacy_scheduled_notice(legacy) == (title, at(), body)
    assert compact_automatic_notice(legacy) == body


@pytest.mark.parametrize("access", ["전체 공개", "참가자 공개"])
def test_legacy_scoreboard_notice_does_not_claim_final_rankings_are_released(access):
    body = f"스코어보드가 공개되었습니다.\n\n- 공개 범위: {access}\n- 공개 시각: 2026. 9. 5. 22:04 KST"
    compact = compact_automatic_notice(body)
    assert "확인할 수 있습니다." in compact
    assert "공개되었습니다" not in compact


@pytest.fixture
def contest(monkeypatch):
    now = datetime(2035, 9, 5, 12, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(import_module("app.services.store"), "now_utc", lambda: now)
    monkeypatch.setattr(settings, "feature_emergency_notice_auto", True)
    contest = store.create_contest(
        "자동 공지 테스트 " + uuid4().hex[:8], "테스트", "",
        start_at=now - timedelta(hours=1), freeze_at=now + timedelta(minutes=9),
        end_at=now + timedelta(hours=1), status=ContestStatus.RUNNING,
    )
    yield contest, now
    store.update_contest_settings(contest.contest_id, status=ContestStatus.ARCHIVED)


def notices(contest):
    return store.contest_notices_for_view(contest.contest_id, operator=True)


def test_worker_deduplicates_short_copy_but_allows_rescheduled_reminders(contest):
    contest, now = contest
    store.enqueue_due_contest_emergency_notices()
    original = notices(contest)
    assert len(original) == 1
    assert original[0].body == "스코어보드 프리즈까지 10분 남았습니다."
    store.enqueue_due_contest_emergency_notices()
    assert len(notices(contest)) == 1
    store.update_contest_settings(contest.contest_id, freeze_at=now + timedelta(minutes=8))
    store.enqueue_due_contest_emergency_notices()
    assert len(notices(contest)) == 2
    assert notices(contest)[0].body == notices(contest)[1].body


def test_legacy_reminder_is_not_reposted_after_deployment(contest):
    contest, _ = contest
    body = legacy_event("freeze", contest.freeze_at, "10분")
    notice = store.create_contest_notice(contest.contest_id, "스코어보드 프리즈 10분 전", body, emergency=True)
    store.enqueue_due_contest_emergency_notices()
    assert len(notices(contest)) == 1
    assert store.contests[contest.contest_id].emergency_notice == "스코어보드 프리즈까지 10분 남았습니다."
    with store._session() as db:
        assert db.get(ContestNoticeRow, notice.contest_notice_id).body == body


def test_edited_automatic_notice_is_not_reposted(contest):
    contest, _ = contest
    store.enqueue_due_contest_emergency_notices()
    notice = notices(contest)[0]
    store.update_contest_notice(contest.contest_id, notice.contest_notice_id, body="운영진 수정 안내")
    store.enqueue_due_contest_emergency_notices()
    assert len(notices(contest)) == 1
    assert notices(contest)[0].body == "운영진 수정 안내"


def test_live_scoreboard_does_not_announce_a_freeze(contest):
    contest, now = contest
    store.update_contest_settings(contest.contest_id, scoreboard_freeze_mode="live")
    store.enqueue_due_contest_emergency_notices()
    assert notices(contest) == []
    store.update_contest_settings(contest.contest_id, freeze_at=now - timedelta(minutes=1))
    store.enqueue_due_contest_emergency_notices()
    assert notices(contest) == []


@pytest.mark.parametrize("release_mode", ["manual", "immediate", "resolver"])
def test_end_notice_is_short_and_does_not_promise_unreleased_rankings(contest, release_mode):
    contest, now = contest
    store.update_contest_settings(contest.contest_id, freeze_at=now - timedelta(hours=1), end_at=now - timedelta(minutes=1), scoreboard_access_after_end="public", scoreboard_release_mode=release_mode)
    store.enqueue_due_contest_emergency_notices()
    store.enqueue_due_contest_emergency_notices()
    assert [(notice.title, notice.body) for notice in notices(contest)] == [("대회 종료", "대회가 종료되었습니다. 수고하셨습니다.")]
    assert store.contests[contest.contest_id].emergency_notice == "대회가 종료되었습니다. 수고하셨습니다."


def test_existing_schedule_notice_displays_compact_copy_without_changing_storage(contest):
    contest, _ = contest
    notice = store.create_contest_notice(contest.contest_id, "대회 운영 시간이 변경되었습니다", LEGACY_TIME, emergency=True)
    expected = "2026 ZOAC의 프리즈 시간이 21시 04분에서 21시 30분으로 변경되었습니다."
    assert notice.body == store.contests[contest.contest_id].emergency_notice == expected
    assert notices(contest)[0].title == "대회 일정 변경"
    with store._session() as db:
        assert db.get(ContestNoticeRow, notice.contest_notice_id).body == LEGACY_TIME
