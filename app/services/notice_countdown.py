"""A small, allowlisted notice template language; never execute user text."""
import math
import re
from datetime import datetime, timezone

from app.models import now_utc
from app.services.automatic_notices import scheduled_notice_copy


COUNTDOWN = re.compile(r"\{\{countdown:(start|freeze|end)(?:@([^\s{}]+))?\}\}")
LABELS = {"start": "대회 시작", "freeze": "스코어보드 프리즈", "end": "대회 종료"}


def countdown_template(target: str, target_at: datetime) -> str:
    # Automatic notices retain their original scheduled instant in history.
    instant = target_at.astimezone(timezone.utc).isoformat(timespec="seconds")
    return "{{countdown:" + target + "@" + instant + "}}"


def notice_template(body: str | None) -> str | None:
    return body if body and COUNTDOWN.search(body) else None


def render_notice(body: str | None, contest=None, now: datetime | None = None) -> str | None:
    if not body:
        return body
    now = now or now_utc()

    def replace(match):
        target, fixed = match.groups()
        if contest and contest.status in {"draft", "schedule_tbd"}:
            return f"{LABELS[target]} 일정이 확정되지 않았습니다."
        if target == "freeze" and contest:
            if contest.scoreboard_freeze_mode == "live":
                return "스코어보드가 실시간으로 갱신됩니다."
            if contest.scoreboard_freeze_mode == "frozen":
                return scheduled_notice_copy(target)[1]
        if target == "end" and contest and contest.status in {"ended", "finalized", "archived"}:
            return scheduled_notice_copy(target)[1]
        try:
            deadline = datetime.fromisoformat(fixed.replace("Z", "+00:00")) if fixed else getattr(contest, target + "_at", None)
            if deadline is None:
                return f"{LABELS[target]} 일정을 확인해 주세요."
            if deadline.tzinfo is None:
                if fixed:
                    return match[0]
                # SQLite stores the same UTC schedule without its timezone.
                deadline = deadline.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            return match[0]
        seconds = max(0, math.ceil((deadline - now).total_seconds()))
        if not seconds:
            return scheduled_notice_copy(target)[1]
        values = [(seconds // 86400, "일"), (seconds // 3600 % 24, "시간"), (seconds // 60 % 60, "분"), (seconds % 60, "초")]
        remaining = " ".join(f"{value}{unit}" for value, unit in values if value)
        return f"{LABELS[target]}까지 {remaining} 남았습니다."

    return COUNTDOWN.sub(replace, body)
