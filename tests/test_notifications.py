"""Verify offline delivery, recipient privacy and independent inbox states."""
import pytest
from sqlalchemy import func, select

from app.models import SubmissionStatus
from app.orm_models import (NotificationRow as Notice, ContestNoticeRow, ContestQuestionRow,
    GeneralSessionRow, JudgeJobRow, SubmissionRow, TeamMemberRow, ParticipantTeamRow)
from app.services import notifications as inbox
from app.services.store import store
from tests.test_rejudging_notifications import database, finished, leased, participant, login, headers, apply, client
from tests.test_judge_security import SECRET


def feed(token, **params):
    response = client.get('/api/notifications', headers=headers(token), params=params)
    assert response.status_code == 200
    return response.json()['data']


def test_notices_are_delivered_offline_and_not_recreated_after_deletion(database, participant):
    notice = store.create_contest_notice('contest', 'Emergency', 'Content', visibility='participants')
    with database() as db:
        assert db.scalar(select(func.count()).select_from(GeneralSessionRow)) == 0
        assert db.scalar(select(func.count()).select_from(Notice)) == 3
    token = login(database)
    item = feed(token)['items'][0]
    assert item['notification_type'] == 'notice' and item['body'] == 'Emergency'
    assert 'noticeId=' + notice.contest_notice_id in item['href']
    assert feed(login(database, 'stranger'))['unread_count'] == 1
    nid = item['notification_id']
    assert client.delete(f'/api/notifications/{nid}', headers=headers(token)).status_code == 200
    with database() as db:
        inbox.create_notice(db, db.get(ContestNoticeRow, notice.contest_notice_id))
        db.commit()
    assert feed(login(database))['items'] == []
    store.update_contest_notice('contest', notice.contest_notice_id, body='Edited')
    assert feed(token)['items'] == []


@pytest.mark.parametrize('visibility', ['public', 'private'])
def test_answers_notify_only_the_question_team_not_other_participants(database, participant, visibility):
    with database() as db:
        question = ContestQuestionRow(contest_question_id='question', contest_id='contest',
            participant_team_id='team', team_member_id='author', title='Private question', body='body', visibility=visibility)
        db.add(question)
        db.commit()
    store.create_answer('contest', 'question', 'Private answer body', visibility)
    assert feed(login(database))['items'][0]['notification_type'] == 'answer'
    assert feed(login(database, 'teammate'))['unread_count'] == 1
    assert feed(login(database, 'stranger'))['items'] == []
    assert 'Private answer body' not in str(feed(login(database)))  # Only the question title is previewed.


def ordinary_job(database):
    with database() as db:
        row = db.get(SubmissionRow, 'submission')
        row.status = 'judging'
        job = db.get(JudgeJobRow, 'job')
        job.status, job.lease_token = 'running', 'ordinary-lease'
        db.commit()


def test_judge_final_result_is_saved_once_for_its_author_and_candidate_is_separate(database, participant):
    ordinary_job(database)
    result = store.report_judge_result('job', SECRET, 'ordinary-lease', SubmissionStatus.WRONG_ANSWER,
        None, None, 1, 23, 45)
    assert result
    token = login(database)
    assert feed(token)['items'][0]['notification_type'] == 'submission'
    assert feed(login(database, 'teammate'))['items'] == []
    with pytest.raises(ValueError, match='lease mismatch'):
        store.report_judge_result('job', SECRET, 'ordinary-lease', SubmissionStatus.ACCEPTED, None, None, None)
    apply(participant)
    items = feed(token)['items']
    assert {item['notification_type'] for item in items} == {'rejudging', 'submission'}
    assert len(items) == 2  # The candidate report itself does not produce an ordinary result event.


@pytest.mark.parametrize('kind', ['operator_test', 'verification', 'preview'])
def test_nonparticipant_results_do_not_create_personal_alerts(database, participant, kind):
    ordinary_job(database)
    with database() as db:
        db.get(SubmissionRow, 'submission').submission_kind = kind
        db.commit()
    store.report_judge_result('job', SECRET, 'ordinary-lease', SubmissionStatus.ACCEPTED, None, None, None)
    assert feed(login(database))['items'] == []


def test_read_dismiss_hide_and_read_all_are_independent_and_account_scoped(database, participant):
    store.create_contest_notice('contest', 'First', 'body')
    store.create_contest_notice('contest', 'Second', 'body')
    token, stranger = login(database), login(database, 'stranger')
    nid = feed(token)['items'][0]['notification_id']
    for action in ['read', 'unread', 'dismiss']:
        assert client.post(f'/api/notifications/{nid}/{action}', headers=headers(stranger)).status_code == 404
    assert client.post(f'/api/notifications/{nid}/dismiss', headers=headers(token)).status_code == 200
    dismissed = feed(login(database))
    item = next(item for item in dismissed['items'] if item['notification_id'] == nid)
    assert item['dismissed_at'] and item['read_at'] is None and dismissed['unread_count'] == 2
    assert client.post(f'/api/notifications/{nid}/read', headers=headers(token)).status_code == 200
    assert feed(token)['unread_count'] == 1
    assert client.post(f'/api/notifications/{nid}/unread', headers=headers(token)).status_code == 200
    assert feed(token)['unread_count'] == 2
    result = client.post('/api/notifications/read-all', headers=headers(token))
    assert result.status_code == 200 and result.json()['data']['updated_count'] == 2
    assert feed(login(database))['unread_count'] == 0
    assert feed(stranger)['unread_count'] == 2
    assert client.delete(f'/api/notifications/{nid}', headers=headers(token)).status_code == 200
    assert feed(token)['total_count'] == 1
    assert client.post(f'/api/notifications/{nid}/unread', headers=headers(token)).status_code == 404
    assert client.post(f'/api/notifications/{nid}/unknown', headers=headers(token)).status_code == 404
    assert client.get('/api/notifications').status_code == 401


def test_server_count_includes_unloaded_pages_and_preview_teams_are_excluded(database, participant):
    with database() as db:
        db.add(ParticipantTeamRow(participant_team_id='preview', contest_id='contest', division_id='division', team_name='__operator_test__test'))
        db.flush()
        db.add(TeamMemberRow(team_member_id='preview-member', contest_id='contest', participant_team_id='preview',
            name='Preview', role='member', email='preview-member@example.com'))
        db.commit()
    for i in range(3):
        store.create_contest_notice('contest', str(i), 'body')
    token = login(database)
    first = feed(token, limit=1)
    second = feed(token, limit=1, offset=first['next_offset'])
    assert first['unread_count'] == first['total_count'] == 3
    assert first['items'][0]['notification_id'] != second['items'][0]['notification_id']
    assert feed(login(database, 'preview-member'))['items'] == []


def test_changing_member_email_does_not_transfer_private_history(database, participant):
    apply(participant)
    with database() as db:
        db.get(TeamMemberRow, 'author').email = 'new@example.com'
        db.commit()
    assert feed(login(database, email='new@example.com'))['items'] == []


def test_notification_failure_rolls_back_its_official_result(database, participant, monkeypatch):
    ordinary_job(database)
    def fail(*args, **kwargs):
        raise RuntimeError('inbox write failed')
    monkeypatch.setattr(inbox, '_emit', fail)
    with pytest.raises(RuntimeError):
        store.report_judge_result('job', SECRET, 'ordinary-lease', SubmissionStatus.ACCEPTED, None, None, None)
    assert store.get_submission('submission').status == 'judging'
    with database() as db:
        assert db.get(JudgeJobRow, 'job').status == 'running'
        assert db.scalar(select(func.count()).select_from(Notice)) == 0
