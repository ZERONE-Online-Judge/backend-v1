"""The single server-side contract for contest roles and their permissions."""
from pydantic import BaseModel, EmailStr, Field, field_validator

ROLE_PERMISSIONS: dict[str, set[str]] = {
    "master": {"contest.*"},
    "settings_manager": {"contest.settings.manage", "contest.update_organization", "contest.update_overview", "contest.update_rule", "contest.update_schedule"},
    "participants_manager": {"contest.participant.view", "contest.participant.manage", "contest.participant.create", "contest.participant.update", "contest.participant.remove", "contest.participant.bulk_create", "contest.access_log.view"},
    "posts_manager": {"contest.notice.view", "contest.notice.manage", "contest.notice.create", "contest.notice.update", "contest.notice.delete", "contest.notice.emergency_publish", "contest.board.question.view", "contest.board.question.manage", "contest.board.answer.create"},
    "staff_manager": {"contest.staff.view", "contest.staff.manage"},
    "submissions_viewer": {"contest.submission.view", "contest.submission.source.view"},
    "scoreboard_viewer": {"contest.scoreboard.view"},
    "scoreboard_manager": {"contest.scoreboard.view", "contest.scoreboard.manage", "contest.scoreboard.freeze", "contest.scoreboard.unfreeze", "contest.scoreboard.setting"},
    "problem_author": {"contest.problem.view", "contest.problem.manage", "contest.problem.review", "contest.problem.test", "contest.problem.create", "contest.problem.update", "contest.problem.delete", "contest.problem.reorder", "contest.problem.resource.view", "contest.problem.resource.manage", "contest.testcase.view", "contest.testcase.manage", "contest.generator.view", "contest.generator.manage"},
    "problem_reviewer": {"contest.problem.review", "contest.problem.test"},
}


def validate_roles(roles: list[str]) -> list[str]:
    if not roles or any(role not in ROLE_PERMISSIONS for role in roles):
        raise ValueError("하나 이상의 올바른 권한을 선택해야 합니다.")
    roles = list(dict.fromkeys(roles))
    if "master" in roles and len(roles) != 1:
        raise ValueError("대회 마스터는 다른 권한과 함께 선택할 수 없습니다.")
    return roles


def permissions_for_roles(roles: list[str]) -> list[str]:
    roles = validate_roles(roles)
    if roles == ["master"]:
        return ["contest.*"]
    return sorted({"contest.view"}.union(*(ROLE_PERMISSIONS[role] for role in roles)))


def roles_for_scopes(scopes: list[str]) -> list[str]:
    """Compatibility for accounts created before roles were persisted."""
    if "contest.*" in scopes:
        return ["master"]
    return [role for role, permissions in ROLE_PERMISSIONS.items() if permissions <= set(scopes)]


class ContestOperatorUpdateRequest(BaseModel):
    display_name: str = Field(min_length=1, max_length=120)
    roles: list[str] = Field(min_length=1, max_length=10)

    @field_validator("display_name")
    @classmethod
    def nonblank_name(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("이름을 입력해야 합니다.")
        return value.strip()

    @field_validator("roles")
    @classmethod
    def valid_roles(cls, value: list[str]) -> list[str]:
        return validate_roles(value)


class ContestOperatorCreateRequest(ContestOperatorUpdateRequest):
    email: EmailStr
