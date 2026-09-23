from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from app.services.notice_countdown import countdown_template, notice_template, render_notice

NOW = datetime(2035, 9, 5, 12, tzinfo=timezone.utc)


def context(**changes):
    return SimpleNamespace(**dict({
        "status": "running", "scoreboard_freeze_mode": "auto",
        "start_at": NOW + timedelta(minutes=30),
        "freeze_at": NOW + timedelta(hours=1), "end_at": NOW + timedelta(hours=2),
    }, **changes))


@pytest.mark.parametrize("elapsed,expected", [(0, "30분"), (1, "29분 59초"), (60, "29분"), (1799, "1초")])
def test_countdown_uses_exact_remaining_time(elapsed, expected):
    assert render_notice("{{countdown:start}}", context(), NOW + timedelta(seconds=elapsed)) == f"대회 시작까지 {expected} 남았습니다."


@pytest.mark.parametrize("target,expected", [("start", "대회가 시작되었습니다."), ("freeze", "스코어보드가 프리즈되었습니다. 이후 제출 결과는 스코어보드에서 잠시 숨겨집니다."), ("end", "대회가 종료되었습니다. 수고하셨습니다.")])
def test_same_template_reaches_completion_without_negative_time(target, expected):
    template = countdown_template(target, NOW)
    for seconds in [0, 1, 86400]:
        assert render_notice(template, context(), NOW + timedelta(seconds=seconds)) == expected


def test_custom_surrounding_text_and_multiple_commands_are_preserved():
    assert render_notice("안내\n{{countdown:start}}\n{{countdown:end}}", context(), NOW) == "안내\n대회 시작까지 30분 남았습니다.\n대회 종료까지 2시간 남았습니다."
    assert render_notice("{{countdown:start}}", context(start_at=NOW + timedelta(days=1, hours=1, minutes=1, seconds=1)), NOW) == "대회 시작까지 1일 1시간 1분 1초 남았습니다."


def test_manual_commands_follow_the_schedule_but_automatic_history_keeps_its_deadline():
    contest = context(start_at=NOW + timedelta(minutes=29))
    snapshot = countdown_template("start", NOW + timedelta(minutes=30))
    assert render_notice(snapshot, contest, NOW) == "대회 시작까지 30분 남았습니다."
    assert render_notice("{{countdown:start}}", contest, NOW) == "대회 시작까지 29분 남았습니다."
    assert notice_template(snapshot) == snapshot
    assert notice_template("일반 공지") is None


def test_utc_storage_timezone_and_operator_overrides():
    assert render_notice("{{countdown:start}}", context(start_at=(NOW + timedelta(minutes=30)).replace(tzinfo=None)), NOW) == "대회 시작까지 30분 남았습니다."
    assert render_notice("{{countdown:start@2035-09-05T21:30:00+09:00}}", context(), NOW) == "대회 시작까지 30분 남았습니다."
    assert render_notice("{{countdown:freeze}}", context(scoreboard_freeze_mode="live"), NOW) == "스코어보드가 실시간으로 갱신됩니다."
    assert "프리즈되었습니다" in render_notice("{{countdown:freeze}}", context(scoreboard_freeze_mode="frozen"), NOW)
    assert "종료되었습니다" in render_notice("{{countdown:end}}", context(status="ended"), NOW)
    assert "확정되지 않았습니다" in render_notice("{{countdown:start}}", context(status="draft"), NOW)
    assert "일정을 확인" in render_notice("{{countdown:start}}", None, NOW)


@pytest.mark.parametrize("body", [None, "", "일반 공지", "{{countdown:unknown}}", "{{countdown:start@bad}}", "{{countdown:start@2035-09-05T12:00:00}}", "{{__import__('os')}}", "<script>alert(1)</script>"])
def test_unrecognized_commands_are_literal_text(body):
    assert render_notice(body, context(), NOW) == body
