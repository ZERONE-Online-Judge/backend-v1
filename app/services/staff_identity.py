"""Atomic login-email changes for a contest staff account's global identity."""
import json

from sqlalchemy import delete, func, select, update

from app.models import now_utc
from app.orm_models import (
    ContestNoticeRow, ContestQuestionAnswerRow, GeneralSessionRow, MailQueueItemRow,
    OtpCodeRow, ParticipantPreviewQuestionRow, ParticipantPreviewSessionRow,
    StaffAccountRow, StaffSessionRow, SubmissionRow, TeamMemberRow,
)
from app.services.errors import AppError


def _permission(scopes: dict, contest_id: str, permission: str) -> bool:
    values = scopes.get(contest_id, [])
    return "contest.*" in values or permission in values


def _email_conflict() -> AppError:
    return AppError(409, "email_already_in_use", "이미 다른 계정에서 사용한 이메일입니다. 다른 이메일을 입력해 주세요.")


def rename_staff_identity(db, account: StaffAccountRow, actor_id: str | None, new_email: str) -> None:
    """Validate and stage all changes in the caller's transaction; never commit."""
    old_email = account.email.strip().lower()
    new_email = new_email.strip().lower()
    if old_email == new_email:
        return
    actor = db.get(StaffAccountRow, actor_id) if actor_id else None
    if not actor:
        raise AppError(403, "email_change_scope_denied", "이 계정의 이메일을 변경할 권한이 없습니다.")
    target_scopes = json.loads(account.contest_scopes or "{}")
    target_roles = json.loads(account.contest_roles or "{}")
    protected = json.loads(account.protected_master_contests or "[]")
    if not actor.is_service_master:
        if protected:
            raise AppError(409, "assigned_master_email_immutable", "서비스 관리자가 할당한 마스터의 이메일은 서비스 관리자만 변경할 수 있습니다.")
        actor_scopes = json.loads(actor.contest_scopes or "{}")
        affected = {cid for cid, scopes in target_scopes.items() if scopes} | {cid for cid, roles in target_roles.items() if roles}
        for contest_id in affected:
            target_master = "master" in target_roles.get(contest_id, []) or "contest.*" in target_scopes.get(contest_id, [])
            if not _permission(actor_scopes, contest_id, "contest.staff.manage") or (
                target_master and "contest.*" not in actor_scopes.get(contest_id, [])
            ):
                raise AppError(403, "email_change_scope_denied", "이메일 변경에는 이 계정이 배정된 모든 대회의 운영자 관리 권한이 필요합니다. 마스터 계정은 각 대회의 마스터만 변경할 수 있습니다.")
    # Staff and participant identities may share an old email across contests.
    # A staff editor must not split or move someone else's participant identity.
    if db.scalar(select(TeamMemberRow.team_member_id).where(func.lower(TeamMemberRow.email) == old_email).limit(1)):
        raise AppError(409, "email_change_participant_identity", "이 이메일은 참가자 계정에도 연결되어 있어 운영자 화면에서 변경할 수 없습니다.")
    if db.scalar(select(StaffAccountRow.staff_account_id).where(func.lower(StaffAccountRow.email) == new_email, StaffAccountRow.staff_account_id != account.staff_account_id).limit(1)):
        raise _email_conflict()
    if db.scalar(select(TeamMemberRow.team_member_id).where(func.lower(TeamMemberRow.email) == new_email).limit(1)):
        raise _email_conflict()
    if db.scalar(select(GeneralSessionRow.general_session_id).where(func.lower(GeneralSessionRow.email) == new_email).limit(1)):
        raise _email_conflict()
    # Do not attach an existing orphaned author's records to this staff account.
    for column in (SubmissionRow.submitted_by_email, ContestQuestionAnswerRow.created_by_email, ContestNoticeRow.created_by_email):
        if db.scalar(select(column).where(func.lower(column) == new_email).limit(1)):
            raise _email_conflict()

    revoked_at = now_utc()
    db.execute(update(StaffSessionRow).where(StaffSessionRow.staff_account_id == account.staff_account_id).values(revoked_at=revoked_at))
    # Retain historical session emails; the new address must authenticate again.
    db.execute(update(GeneralSessionRow).where(func.lower(GeneralSessionRow.email) == old_email).values(revoked_at=revoked_at))
    db.execute(delete(OtpCodeRow).where(func.lower(OtpCodeRow.email).in_([old_email, new_email])))
    db.execute(delete(ParticipantPreviewSessionRow).where(ParticipantPreviewSessionRow.staff_account_id == account.staff_account_id))
    db.execute(update(MailQueueItemRow).where(
        func.lower(MailQueueItemRow.recipient_email).in_([old_email, new_email]),
        MailQueueItemRow.mail_type.in_(["general_otp", "staff_otp", "participant_otp"]),
        MailQueueItemRow.status == "pending",
    ).values(status="cancelled"))

    db.execute(update(SubmissionRow).where(
        func.lower(SubmissionRow.submitted_by_email) == old_email,
        SubmissionRow.submission_kind.in_(["operator_test", "participant_preview"]),
    ).values(submitted_by_email=new_email))
    db.execute(update(ContestQuestionAnswerRow).where(func.lower(ContestQuestionAnswerRow.created_by_email) == old_email).values(created_by_email=new_email))
    db.execute(update(ContestNoticeRow).where(func.lower(ContestNoticeRow.created_by_email) == old_email).values(created_by_email=new_email))
    # Preview board snapshots retain stable staff/team ids. Replace only identity
    # email fields, never body text or historical display names.
    for row in db.scalars(select(ParticipantPreviewQuestionRow)).all():
        payload = json.loads(row.payload)
        changed = False
        if str(payload.get("author_email") or "").lower() == old_email:
            payload["author_email"] = new_email
            changed = True
        for answer in payload.get("answers", []):
            if str(answer.get("created_by_email") or "").lower() == old_email:
                answer["created_by_email"] = new_email
                changed = True
        if changed:
            row.payload = json.dumps(payload)
    account.email = new_email
