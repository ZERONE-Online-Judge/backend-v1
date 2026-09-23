"""Short public copy, with compatibility for previously generated notices."""
import re
from datetime import datetime, timezone
from uuid import NAMESPACE_URL, uuid5
from zoneinfo import ZoneInfo


KST = ZoneInfo("Asia/Seoul")
TIME_LABELS = {"start_at": "시작", "freeze_at": "프리즈", "end_at": "종료"}
LEGACY_LABELS = {"오픈": "start_at", "프리즈": "freeze_at", "마감": "end_at"}
LEGACY_SCHEDULE_FOOTER = "변경된 시간 기준으로 대회 접근, 제출 가능 여부, 스코어보드 프리즈가 자동 적용됩니다."


def time_update_notice_body(contest_title: str, changed_fields: list[tuple[str, datetime, datetime]]) -> str:
    changes = []
    for field, before, after in changed_fields:
        before, after = (value.astimezone(KST).replace(second=0, microsecond=0) for value in (before, after))
        if before == after:
            continue
        if before.year != after.year:
            pattern = "%Y년 %m월 %d일 %H시 %M분"
        elif before.date() != after.date():
            pattern = "%m월 %d일 %H시 %M분"
        else:
            pattern = "%H시 %M분"
        prefix = f"{contest_title}의 " if not changes else ""
        changes.append(f"{prefix}{TIME_LABELS[field]} 시간이 {before.strftime(pattern)}에서 {after.strftime(pattern)}으로 변경되었습니다.")
    return "\n".join(changes)


def scheduled_notice_copy(target: str, remaining: str | None = None) -> tuple[str, str]:
    label = {"start": "대회 시작", "freeze": "스코어보드 프리즈", "end": "대회 종료"}[target]
    if remaining:
        return f"{label} {remaining} 전", f"{label}까지 {remaining} 남았습니다."
    if target == "start":
        return "대회 시작", "대회가 시작되었습니다."
    if target == "freeze":
        return "스코어보드 프리즈 시작", "스코어보드가 프리즈되었습니다. 이후 제출 결과는 스코어보드에서 잠시 숨겨집니다."
    return "대회 종료", "대회가 종료되었습니다. 수고하셨습니다."


def scheduled_notice_id(contest_id: str, title: str, target_at: datetime) -> str:
    # Copy can change without re-announcing an event. A rescheduled event can
    # still be announced even when its short public wording stays the same.
    instant = target_at.astimezone(timezone.utc).replace(second=0, microsecond=0)
    return str(uuid5(NAMESPACE_URL, f"zoj:contest-notice:{contest_id}:{title}:{instant.isoformat()}"))


def legacy_scheduled_notice(body: str) -> tuple[str, datetime, str] | None:
    lines = body.split("\n")
    if len(lines) != 4 or lines[1] != "":
        return None
    match = re.fullmatch(r"- (프리즈|종료|공개) 시각: (\d{4})\. (\d{1,2})\. (\d{1,2})\. (\d{2}):(\d{2}) KST", lines[2])
    # The old scoreboard access notice put the access line before its time.
    if lines[0] == "스코어보드가 공개되었습니다.":
        match = re.fullmatch(r"- (공개) 시각: (\d{4})\. (\d{1,2})\. (\d{1,2})\. (\d{2}):(\d{2}) KST", lines[3])
        if match and lines[2] in {"- 공개 범위: 전체 공개", "- 공개 범위: 참가자 공개"}:
            copy = "스코어보드는 누구나 확인할 수 있습니다." if lines[2].endswith("전체 공개") else "스코어보드는 대회 참가자만 확인할 수 있습니다."
            try:
                return "스코어보드 공개됨", datetime(*map(int, match.groups()[1:]), tzinfo=KST), copy
            except ValueError:
                return None
    if not match:
        return None
    target = "freeze" if match[1] == "프리즈" else "end"
    reminder = re.fullmatch(r"(스코어보드 프리즈|대회 종료)까지 (30분|10분|5분|1분) 남았습니다\.", lines[0])
    remaining = reminder[2] if reminder else None
    if remaining:
        expected_first = f"{'스코어보드 프리즈' if target == 'freeze' else '대회 종료'}까지 {remaining} 남았습니다."
        expected_last = (
            "- 프리즈 이후 제출 결과는 대회 종료 전까지 스코어보드에 반영되지 않을 수 있습니다."
            if target == "freeze" else "- 종료 이후에는 제출이 제한될 수 있으니 남은 시간을 확인해 주세요."
        )
    else:
        expected_first = "스코어보드가 프리즈되었습니다." if target == "freeze" else "대회가 종료되었습니다."
        expected_last = (
            "- 프리즈 이후 제출 결과는 대회 종료 전까지 공개 스코어보드에 반영되지 않을 수 있습니다."
            if target == "freeze" else "- 종료 이후에는 제출이 제한됩니다."
        )
    if lines[0] != expected_first or lines[3] != expected_last:
        return None
    try:
        target_at = datetime(*map(int, match.groups()[1:]), tzinfo=KST)
    except ValueError:
        return None
    title, copy = scheduled_notice_copy(target, remaining)
    return title, target_at, copy


def compact_automatic_notice(body: str | None) -> str | None:
    if not body:
        return body
    scheduled = legacy_scheduled_notice(body)
    if scheduled:
        return scheduled[2]
    header, separator, rest = body.partition(" 운영시간이 변경되었습니다.\n\n")
    if not separator:
        return body
    rows, footer, suffix = rest.partition("\n\n" + LEGACY_SCHEDULE_FOOTER)
    if not footer or (suffix and not suffix.startswith("\n\n")):
        return body
    changes = []
    for line in rows.split("\n"):
        match = re.fullmatch(r"- (오픈|프리즈|마감): (.+) -> (.+)", line)
        if not match:
            return body
        try:
            before, after = (datetime.strptime(value, "%y년 %m월 %d일 %H시 %M분 KST").replace(tzinfo=KST) for value in match.groups()[1:])
        except ValueError:
            return body
        changes.append((LEGACY_LABELS[match[1]], before, after))
    copy = time_update_notice_body(header, changes)
    # Leave unrecognizable or all-unchanged historical records intact.
    return copy + suffix if copy else body
