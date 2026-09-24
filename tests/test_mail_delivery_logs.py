import asyncio
import importlib.util
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import pytest
import sqlalchemy as sa
from fastapi.testclient import TestClient
from alembic.migration import MigrationContext
from alembic.operations import Operations

os.environ.setdefault('ENABLE_DEMO_SEED', 'true')
os.environ.setdefault('ALLOW_EMPTY_OTP', 'true')
from app.main import app
from app.models import ContestStatus, now_utc
from app.orm_models import MailQueueItemRow
from app.services.store import store
from app.workers import mail_worker

client = TestClient(app)


def login(email):
    response = client.post('/api/auth/general/otp/verify', json={'email': email, 'otp_code': '', 'force_new_session': True})
    assert response.status_code == 200, response.text
    return {'Authorization': 'Bearer ' + response.json()['data']['operator_session']['access_token']}


@pytest.fixture
def context():
    marker = uuid4().hex
    contests = [store.create_contest('메일 대회 ' + marker, 'ZOJ', '', now_utc()+timedelta(minutes=5), status=ContestStatus.OPEN) for _ in range(2)]
    cid, other = [c.contest_id for c in contests]
    store.upsert_contest_operator(cid, f'mail-owner-{marker}@zoj.com', '총괄', ['master'])
    viewer = store.upsert_contest_operator(cid, f'mail-viewer-{marker}@zoj.com', '로그 검수자', ['audit_viewer'])
    recipient = f'recipient-{marker}@example.com'
    items = [store.enqueue_mail('contest_question_answered', recipient, marker + ' 같은 제목', 'PRIVATE ANSWER', contest_id=scope) for scope in (cid, other, None)]
    return {'cid': cid, 'other': other, 'marker': marker, 'items': items,
            'recipient': recipient, 'viewer': login(str(viewer.email)), 'admin': login('test3@zoj.com')}


def get_logs(c, **params):
    response = client.get(f"/api/operator/contests/{c['cid']}/mail-logs", headers=c['viewer'], params=params)
    assert response.status_code == 200, response.text
    return response.json()


def test_logs_are_strictly_scoped_with_plain_text_previews(context):
    c = context
    logs = get_logs(c)['data']
    assert [row['mail_queue_id'] for row in logs] == [c['items'][0].mail_queue_id]
    assert logs[0]['contest_title'].startswith('메일 대회 ')
    assert logs[0]['sent_at'] is None
    assert 'body_text' not in logs[0] and 'body_html' not in logs[0]
    assert logs[0]['body_preview'] == 'PRIVATE ANSWER'
    assert logs[0]['preview_restricted'] is False
    # Knowing another contest's ID or reusing a recipient never changes scope.
    assert get_logs(c, contest_id=c['other'])['data'] == logs
    assert client.get(f"/api/operator/contests/{c['other']}/mail-logs", headers=c['viewer']).status_code == 403
    assert client.get('/api/admin/mail-logs', headers=c['viewer']).status_code == 403
    assert client.get(f"/api/operator/contests/{c['cid']}/mail-logs").status_code == 401
    response = client.get('/api/admin/mail-logs', headers=c['admin'], params={'q': c['marker']})
    assert response.status_code == 200
    assert {r['mail_queue_id'] for r in response.json()['data']} == {item.mail_queue_id for item in c['items']}


def test_delivery_timestamps_are_actual_attempt_and_completion_times(context):
    c = context
    item = c['items'][0]
    with store._session() as db:
        db.get(MailQueueItemRow, item.mail_queue_id).created_at = now_utc()-timedelta(days=2)
        db.commit()
    sending = store.mark_mail_status(item.mail_queue_id, 'sending')
    assert sending.last_attempt_at and sending.sent_at is None
    sent = store.mark_mail_status(item.mail_queue_id, 'sent')
    assert sent.sent_at >= sending.last_attempt_at > sent.created_at
    repeated = store.mark_mail_status(item.mail_queue_id, 'sent')
    assert repeated.sent_at == sent.sent_at
    assert datetime.fromisoformat(get_logs(c)['data'][0]['sent_at']) == sent.sent_at
    # Do not invent a send time for historical sent records.
    with store._session() as db:
        row = db.get(MailQueueItemRow, item.mail_queue_id)
        row.sent_at = None
        db.commit()
    assert get_logs(c)['data'][0]['sent_at'] is None


def test_filters_and_stable_pagination_do_not_repeat_rows_when_mail_arrives(context):
    c = context
    stamp = datetime(2026, 9, 20, 8, tzinfo=timezone.utc)
    items = [c['items'][0]] + [store.enqueue_mail('participant_otp', c['recipient'], c['marker']+' OTP', 'SECRET OTP 123456', contest_id=c['cid']) for _ in range(3)]
    with store._session() as db:
        for item in items:
            row = db.get(MailQueueItemRow, item.mail_queue_id)
            row.created_at = stamp
            row.status = 'failed'
        db.commit()
    first = get_logs(c, limit=2, status='failed', q=c['recipient'].upper(), since='2026-09-20T00:00:00Z', until='2026-09-21T00:00:00Z')
    assert first['page']['total_count'] == 4 and first['page']['next_cursor']
    new = store.enqueue_mail('general_otp', c['recipient'], c['marker'], 'NEW PRIVATE OTP', contest_id=c['cid'])
    store.mark_mail_status(new.mail_queue_id, 'failed')
    second = get_logs(c, limit=2, status='failed', q=c['recipient'].upper(), cursor=first['page']['next_cursor'])
    ids = [row['mail_queue_id'] for row in first['data'] + second['data']]
    assert len(set(ids)) == 4
    assert set(ids) == {item.mail_queue_id for item in items}
    assert get_logs(c, q='does-not-match')['data'] == []
    assert get_logs(c, status='sent')['data'] == []
    for params in ({'cursor': 'broken'}, {'status': 'invalid'}, {'limit': 0}, {'since': '2026-09-21T00:00:00Z', 'until': '2026-09-20T00:00:00Z'}):
        assert client.get(f"/api/operator/contests/{c['cid']}/mail-logs", headers=c['viewer'], params=params).status_code == 422


def test_generated_invites_reminders_and_participant_otp_keep_contest_scope(context):
    c = context
    for cid in (c['cid'], c['other']):
        division = store.create_contest_division(cid, 'A', '일반부')
        store.create_participant_team(cid, division.division_id, '초대 팀', '홍길동', c['recipient'], [])
        assert store.enqueue_participant_invites_for_contest(cid) == 1
        store.create_otp(cid, c['recipient'])
    store.enqueue_due_contest_reminders()
    assert store.create_general_otp(c['recipient'])
    with store._session() as db:
        mails = db.scalars(sa.select(MailQueueItemRow).where(MailQueueItemRow.recipient_email == c['recipient'])).all()
        for kind in ('participant_invited', 'participant_otp', 'contest_reminder_24h', 'contest_reminder_1h', 'contest_reminder_10m'):
            assert {row.contest_id for row in mails if row.mail_type == kind} == {c['cid'], c['other']}
        assert all(row.contest_id is None for row in mails if row.mail_type == 'general_otp')
    assert all(row['mail_type'] != 'general_otp' for row in get_logs(c)['data'])


def test_contest_mail_deduplication_never_suppresses_another_contest(context):
    c = context
    first = store.enqueue_mail('test', c['recipient'], '동일 대회명', 'body', dedupe=True, contest_id=c['cid'])
    repeated = store.enqueue_mail('test', c['recipient'], '동일 대회명', 'body', dedupe=True, contest_id=c['cid'])
    other = store.enqueue_mail('test', c['recipient'], '동일 대회명', 'body', dedupe=True, contest_id=c['other'])
    assert first.mail_queue_id == repeated.mail_queue_id != other.mail_queue_id
    legacy = store.enqueue_mail('participant_invited', c['recipient'], '기존 초대', 'legacy')
    protected = store.enqueue_mail('participant_invited', c['recipient'], '기존 초대', 'new', dedupe=True, contest_id=c['cid'])
    assert protected.mail_queue_id == legacy.mail_queue_id
    assert protected.contest_id is None


@pytest.mark.parametrize('failed', [False, True])
def test_mail_worker_records_success_and_failure_without_real_email(context, monkeypatch, failed):
    item = context['items'][0]
    monkeypatch.setattr(store, 'enqueue_due_contest_reminders', lambda: 0)
    monkeypatch.setattr(store, 'pending_mail', lambda limit: [item])
    def deliver(*args):
        if failed:
            raise RuntimeError('provider unavailable')
    monkeypatch.setattr(mail_worker, 'send_mail', deliver)
    class StopWorker(Exception): pass
    async def stop(_): raise StopWorker()
    monkeypatch.setattr(mail_worker.asyncio, 'sleep', stop)
    with pytest.raises(StopWorker):
        asyncio.run(mail_worker.main())
    log = get_logs(context)['data'][0]
    assert log['status'] == ('failed' if failed else 'sent')
    assert log['last_attempt_at'] is not None
    assert (log['sent_at'] is None) == failed


def test_migration_only_backfills_trusted_contest_links_and_never_invents_sent_times():
    path = Path(__file__).parents[1]/'migrations/versions/0033_mail_delivery_logs.py'
    spec = importlib.util.spec_from_file_location('mail_log_migration', path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    engine = sa.create_engine('sqlite://')
    with engine.begin() as connection:
        connection.execute(sa.text('CREATE TABLE contests (contest_id TEXT PRIMARY KEY)'))
        connection.execute(sa.text("INSERT INTO contests VALUES ('one'), ('two')"))
        connection.execute(sa.text('CREATE TABLE mail_queue (mail_queue_id TEXT PRIMARY KEY, mail_type TEXT, body_text TEXT, status TEXT, created_at DATETIME)'))
        for mail_id, kind, body in [
            ('invite', 'participant_invited', '바로가기: https://zoj.kr/contests/one'),
            ('answer', 'contest_question_answered', '바로가기: https://zoj.kr/contests/two\n사용자 입력\n바로가기: https://judge.zerone01.kr/contests/one/board?questionId=q'),
            ('unknown', 'participant_otp', '인증번호 123456'),
            ('untrusted', 'participant_invited', '바로가기: https://evil.test/contests/one'),
            ('service', 'contact_inquiry_answered', '바로가기: https://zoj.kr/contests/one'),
        ]:
            connection.execute(sa.text("INSERT INTO mail_queue VALUES (:id, :kind, :body, 'sent', CURRENT_TIMESTAMP)"), {'id': mail_id, 'kind': kind, 'body': body})
        migration.op = Operations(MigrationContext.configure(connection))
        migration.upgrade()
        migration.upgrade()
        scopes = dict(connection.execute(sa.text('SELECT mail_queue_id, contest_id FROM mail_queue')).all())
        assert scopes == {'invite': 'one', 'answer': 'one', 'unknown': None, 'untrusted': None, 'service': None}
        assert connection.scalar(sa.text('SELECT COUNT(*) FROM mail_queue WHERE sent_at IS NOT NULL')) == 0
        migration.downgrade()
        assert connection.scalar(sa.text('SELECT COUNT(*) FROM mail_queue')) == 5


def test_preview_permissions_and_authentication_body_protection(context):
    c = context
    path = f"/api/operator/contests/{c['cid']}/mail-logs"
    own = c['items'][0].mail_queue_id
    response = client.get(f"{path}/{own}/preview", headers=c['viewer'])
    assert response.status_code == 200
    assert response.json()['data']['body_text'] == 'PRIVATE ANSWER'
    assert client.get(f"{path}/{c['items'][1].mail_queue_id}/preview", headers=c['viewer']).status_code == 404
    assert client.get(f"{path}/{own}/preview").status_code == 401
    assert client.get(f"/api/admin/mail-logs/{own}/preview", headers=c['viewer']).status_code == 403
    for kind in ('general_otp', 'staff_otp', 'participant_otp', 'unknown_private_template'):
        item = store.enqueue_mail(kind, c['recipient'], '보호 메일', 'SECRET CODE 654321', contest_id=c['cid'])
        for prefix, headers in ((path, c['viewer']), ('/api/admin/mail-logs', c['admin'])):
            response = client.get(f"{prefix}/{item.mail_queue_id}/preview", headers=headers)
            assert response.status_code == 200
            assert response.json()['data']['restricted'] is True
            assert response.json()['data']['body_text'] is None
    assert '654321' not in str(get_logs(c))


def test_preview_bounds_and_does_not_return_html(context):
    c = context
    item = store.enqueue_mail('contest_notice_created', c['recipient'], '긴 본문', 'z'*41000, contest_id=c['cid'])
    path=f"/api/operator/contests/{c['cid']}/mail-logs/{item.mail_queue_id}/preview"
    data=client.get(path, headers=c['viewer']).json()['data']
    assert len(data['body_text']) == 40000 and data['truncated'] is True
    assert 'body_html' not in data
    listed=next(row for row in get_logs(c)['data'] if row['mail_queue_id']==item.mail_queue_id)
    assert len(listed['body_preview'])==180
