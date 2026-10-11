from datetime import timedelta

import pytest
from sqlalchemy import func, select

from app.models import now_utc, SubmissionStatus
from app.orm_models import (ContestRow, ContestDivisionRow, ParticipantTeamRow, TeamMemberRow,
    GeneralSessionRow, TeamSessionRow, SubmissionRow, RejudgingNotificationRow as Notice)
from app.services import rejudging_notifications as inbox
from app.services.errors import AppError
from app.services.security import new_session_token, token_hash
from app.services.store import store
from tests.test_staged_rejudging import database, finished, leased, create, claim, report, decide, client


@pytest.fixture
def participant(database, finished):
    now = now_utc()
    with database() as db:
        db.add(ContestRow(contest_id='contest', title='Test contest', organization_name='Test', overview='',
            status='running', start_at=now, end_at=now + timedelta(hours=2), freeze_at=now + timedelta(hours=1)))
        db.flush()
        db.add(ContestDivisionRow(division_id='division', contest_id='contest', code='all', name='All'))
        db.flush()
        for team in ('team', 'other-team'):
            db.add(ParticipantTeamRow(participant_team_id=team, contest_id='contest', division_id='division', team_name=team))
        db.flush()
        for member, team in (('author', 'team'), ('teammate', 'team'), ('stranger', 'other-team')):
            db.add(TeamMemberRow(team_member_id=member, contest_id='contest', participant_team_id=team,
                name=member, email=f'{member}@example.com', role='member'))
        row = db.get(SubmissionRow, 'submission')
        row.participant_team_id = 'team'
        row.team_member_id = 'author'
        db.commit()
    return finished


def login(database, member='author', kind='general_access', **changes):
    token = new_session_token(kind, member, 3600)
    now = now_utc()
    values = dict(access_token_hash=token_hash(token), issued_at=now, revoked_at=None)
    if kind == 'general_access':
        values.update(email=f'{member}@example.com', refresh_token_hash=token_hash(token + 'refresh'),
            access_expires_at=now + timedelta(hours=1), refresh_expires_at=now + timedelta(days=1))
        model = GeneralSessionRow
    else:
        values.update(contest_id='contest', division_id='division', participant_team_id='team', team_member_id=member,
            expires_at=now + timedelta(hours=1))
        model = TeamSessionRow
    values.update(changes)
    with database() as db:
        db.add(model(**values))
        db.commit()
    return token


def headers(token):
    return {'Authorization': f'Bearer {token}'}


def count(database):
    with database() as db:
        return db.scalar(select(func.count()).select_from(Notice))


def apply(node, status=SubmissionStatus.ACCEPTED):
    batch = create()
    report(claim(node)[0], status)
    decide(batch, 'apply')
    return batch


def test_only_applied_changed_participant_verdict_notifies_author_once(database, participant):
    token = login(database)
    batch = create()
    report(claim(participant)[0])
    assert count(database) == 0
    decide(batch, 'apply')
    assert count(database) == 1
    notice = inbox.list_notifications(store, token)['items'][0]
    assert (notice['old_status'], notice['new_status'], notice['reason']) == ('wrong_answer', 'accepted', 'Fix testcase')
    assert notice['submission_id'] == 'submission' and notice['problem_id'] == 'problem'
    assert notice['contest_title'] == 'Test contest' and notice['problem_code'] == 'A'
    assert 'team_member_id' not in notice and 'email' not in notice and 'source_code' not in notice
    assert inbox.list_notifications(store, login(database, 'teammate'))['items'] == []
    with pytest.raises(AppError):
        decide(batch, 'apply')
    assert count(database) == 1
    apply(participant, SubmissionStatus.ACCEPTED)  # Runtime/message changes do not notify.
    assert count(database) == 1
    cancelled = create()
    report(claim(participant)[0], SubmissionStatus.WRONG_ANSWER)
    decide(cancelled, 'cancel')
    assert count(database) == 1


@pytest.mark.parametrize('kind', ['operator_test', 'participant_preview'])
def test_nonparticipant_verdict_changes_do_not_notify(database, participant, kind):
    with database() as db:
        db.get(SubmissionRow, 'submission').submission_kind = kind
        db.commit()
    apply(participant)
    assert count(database) == 0


def test_read_hide_persist_and_cannot_touch_other_participants_notifications(database, participant):
    apply(participant)
    token, stranger = login(database), login(database, 'stranger')
    base = '/api/notifications/rejudging'
    notice = client.get(base, headers=headers(token)).json()['data']['items'][0]
    nid = notice['notification_id']
    assert client.get(base).status_code == 401
    assert client.get(base, headers=headers(stranger)).json()['data']['items'] == []
    for other in (stranger, login(database, 'teammate')):
        assert client.post(f'{base}/{nid}/read', headers=headers(other)).status_code == 404
        assert client.delete(f'{base}/{nid}', headers=headers(other)).status_code == 404
    assert client.post(f'{base}/{nid}/read', headers=headers(token)).status_code == 200
    read = inbox.list_notifications(store, login(database))['items'][0]['read_at']
    assert read
    assert client.post(f'{base}/{nid}/read', headers=headers(token)).status_code == 200
    assert inbox.list_notifications(store, token)['items'][0]['read_at'] == read
    assert client.delete(f'{base}/{nid}', headers=headers(token)).status_code == 200
    assert inbox.list_notifications(store, login(database))['items'] == []
    assert client.delete(f'{base}/{nid}', headers=headers(token)).status_code == 200
    assert count(database) == 1


@pytest.mark.parametrize('kind', ['general_access', 'participant_access'])
def test_expired_revoked_and_cross_contest_sessions_are_rejected(database, participant, kind):
    apply(participant)
    cid = 'contest' if kind == 'participant_access' else None
    token = login(database, kind=kind)
    assert len(inbox.list_notifications(store, token, cid)['items']) == 1
    if kind == 'participant_access':
        with pytest.raises(AppError):
            inbox.list_notifications(store, token, 'other-contest')
        with pytest.raises(AppError):
            inbox.list_notifications(store, token)
        with database() as db:
            db.get(TeamMemberRow, 'author').session_revoked_at = now_utc()
            db.commit()
        with pytest.raises(AppError):
            inbox.list_notifications(store, token, cid)
    with database() as db:
        model = GeneralSessionRow if kind == 'general_access' else TeamSessionRow
        db.scalar(select(model).where(model.access_token_hash == token_hash(token))).revoked_at = now_utc()
        db.commit()
    with pytest.raises(AppError):
        inbox.list_notifications(store, token, cid)
    expiry = 'access_expires_at' if kind == 'general_access' else 'expires_at'
    with pytest.raises(AppError):
        inbox.list_notifications(store, login(database, kind=kind, **{expiry: now_utc() - timedelta(seconds=1)}), cid)


def test_repeated_verdict_changes_and_pagination_keep_distinct_events(database, participant):
    token = login(database)
    for status in (SubmissionStatus.ACCEPTED, SubmissionStatus.WRONG_ANSWER, SubmissionStatus.ACCEPTED):
        apply(participant, status)
    first = inbox.list_notifications(store, token, limit=2)
    second = inbox.list_notifications(store, token, offset=first['next_offset'], limit=2)
    assert len(first['items']) == 2 and len(second['items']) == 1 and second['next_offset'] is None
    assert len({row['notification_id'] for row in first['items'] + second['items']}) == 3


def test_failed_atomic_apply_does_not_leave_a_notification_or_changed_verdict(database, participant):
    with database() as db:
        db.add(SubmissionRow(submission_id='a-first', contest_id='contest', division_id='division', problem_id='problem',
            participant_team_id='team', team_member_id='author', language='cpp17', source_code='int main(){}', status='wrong_answer'))
        db.commit()
    batch = create(problem_id='problem')
    for job in claim(participant):
        report(job)
    with database() as db:
        db.get(SubmissionRow, 'submission').source_code = 'changed'
        db.commit()
    with pytest.raises(AppError):
        decide(batch, 'apply')
    assert count(database) == 0
    assert store.get_submission('a-first').status == 'wrong_answer'


def test_legacy_authorless_team_submission_notifies_only_its_current_members(database, participant):
    with database() as db:
        db.get(SubmissionRow, 'submission').team_member_id = None
        db.commit()
    apply(participant)
    assert count(database) == 2
    assert len(inbox.list_notifications(store, login(database, 'author'))['items']) == 1
    assert len(inbox.list_notifications(store, login(database, 'teammate'))['items']) == 1
    assert inbox.list_notifications(store, login(database, 'stranger'))['items'] == []


def test_recipient_membership_matches_email_case_insensitively(database, participant):
    apply(participant)
    with database() as db:
        db.get(TeamMemberRow, 'author').email = 'Author@Example.Com'
        db.commit()
    assert len(inbox.list_notifications(store, login(database))['items']) == 1
