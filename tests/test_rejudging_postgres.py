"""Optional real PostgreSQL locking/migration checks against an isolated database.

Run with REJUDGING_TEST_POSTGRES_URL pointing to a disposable test database.
"""
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, select, func, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import make_url

from app.database import Base
from app.models import StaffAccount, SubmissionStatus, now_utc
from app.orm_models import (ContestRow, ContestDivisionRow, ProblemRow, TestcaseSetRow as CaseSet,
    TestcaseRow as Case, SubmissionRow, RejudgingRow, RejudgingItemRow, JudgeJobRow,
    RejudgingNotificationRow as Notice, ParticipantTeamRow, TeamMemberRow)
from app.services import rejudging
from app.services.errors import AppError
from app.services.store import store
from app.settings import settings

URL = os.environ.get('REJUDGING_TEST_POSTGRES_URL')
if URL:
    target = make_url(URL)
    if target.host not in {'localhost', '127.0.0.1', '::1'} or not (target.database or '').endswith('_rejudging_test'):
        raise ValueError('Use a local disposable database ending in _rejudging_test')
pytestmark = pytest.mark.skipif(not URL, reason='requires a disposable PostgreSQL database')
ACTOR = StaffAccount(staff_account_id='staff', email='operator@test.com', display_name='Operator')
SECRET = 'postgres-test-approved-agent-secret-32'


@pytest.fixture
def postgres(monkeypatch):
    engine = create_engine(URL)
    Base.metadata.drop_all(engine)
    # Build the historical schema rather than mixing current ORM tables with
    # an old version stamp (newer Editor/auth tables would already exist).
    with engine.begin() as db:
        db.execute(text('DROP TABLE IF EXISTS alembic_version'))
    monkeypatch.setattr(settings, 'database_url', URL)
    cfg = Config('alembic.ini')
    command.upgrade(cfg, '0039_restore_verification_runs')
    with engine.begin() as db:
        db.execute(text('DROP INDEX IF EXISTS ix_team_members_email_lower'))
    command.upgrade(cfg, 'head')
    assert {'rejudgings', 'rejudging_items', 'notifications'} <= set(inspect(engine).get_table_names())
    assert 'ix_team_members_email_lower' in {index['name'] for index in inspect(engine).get_indexes('team_members')}
    sessions = sessionmaker(bind=engine, expire_on_commit=False, autoflush=False)
    monkeypatch.setattr(store, '_session', sessions)
    with sessions() as db:
        now = now_utc()
        db.add(ContestRow(contest_id='contest', title='Test', organization_name='Test', overview='', status='running',
            start_at=now, end_at=now + timedelta(hours=2), freeze_at=now + timedelta(hours=1)))
        db.flush()
        db.add(ContestDivisionRow(division_id='division', contest_id='contest', code='all', name='All'))
        db.flush()
        db.add(ProblemRow(problem_id='problem', contest_id='contest', division_id='division', problem_code='A', title='Test',
            statement='Test', time_limit_ms=1000, memory_limit_mb=128, display_order=1))
        db.flush()
        db.add(CaseSet(testcase_set_id='set', problem_id='problem', version=1, is_active=True))
        db.flush()
        db.add(Case(testcase_id='case', testcase_set_id='set', display_order=1, input_storage_key='in', output_storage_key='out',
            input_sha256='a'*64, output_sha256='b'*64))
        for index in range(4):
            db.add(SubmissionRow(submission_id=str(index), contest_id='contest', division_id='division', problem_id='problem',
                language='cpp17', source_code='int main(){}', status='wrong_answer'))
        db.commit()
    yield sessions
    # Verify rollback removes only the two new tables and retains old submissions.
    command.downgrade(cfg, '0039_restore_verification_runs')
    assert not {'rejudgings', 'rejudging_items'} & set(inspect(engine).get_table_names())
    with sessions() as db:
        assert db.scalar(select(func.count()).select_from(SubmissionRow)) == 4
    engine.dispose()


def create():
    return rejudging.create(store, 'contest', problem_id='problem', reason='Test', actor=ACTOR)


def finish(batch):
    node = store.provision_node('node', SECRET, 4)
    jobs = store.claim_jobs(node.judge_node_id, SECRET, 4)
    assert len(jobs) == 4
    for job in jobs:
        store.report_judge_result(job['judge_job_id'], SECRET, job['lease_token'], SubmissionStatus.ACCEPTED, None, None, None, 10, 100)


def test_postgres_applied_notification_and_foreign_key_cleanup(postgres):
    with postgres() as db:
        db.add(ParticipantTeamRow(participant_team_id='team', contest_id='contest', division_id='division', team_name='Team'))
        db.flush()
        db.add(TeamMemberRow(team_member_id='member', participant_team_id='team', contest_id='contest',
            role='member', name='Member', email='member@test.com'))
        db.flush()
        row = db.get(SubmissionRow, '0')
        row.participant_team_id, row.team_member_id = 'team', 'member'
        db.commit()
    batch = create()
    finish(batch)
    rejudging.decide(store, 'contest', batch['rejudging_id'], 'apply', ACTOR)
    with postgres() as db:
        notice = db.scalar(select(Notice))
        assert notice and notice.old_status == 'wrong_answer' and notice.new_status == 'accepted'
        assert notice.team_member_id == 'member' and notice.reason == 'Test'
        # Deleting an operator history item cascades its notification, not its submission.
        db.delete(db.get(RejudgingItemRow, notice.rejudging_item_id))
        db.commit()
        assert db.scalar(select(func.count()).select_from(Notice)) == 0
        assert db.scalar(select(func.count()).select_from(SubmissionRow)) == 4


def test_postgres_duplicate_create_and_apply_cancel_race(postgres):
    def request(_):
        try: return create()
        except AppError as error: return error.status_code
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(request, range(2)))
    assert results.count(409) == 1
    batch = next(result for result in results if isinstance(result, dict))
    finish(batch)
    def decision(action):
        try: return rejudging.decide(store, 'contest', batch['rejudging_id'], action, ACTOR)['status']
        except AppError as error: return error.status_code
    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(decision, ['apply', 'cancel']))
    assert outcomes.count(409) == 1
    expected = 'accepted' if 'applied' in outcomes else 'wrong_answer'
    with postgres() as db:
        assert {row.status for row in db.scalars(select(SubmissionRow))} == {expected}
        assert db.scalar(select(func.count()).select_from(RejudgingRow)) == 1
        assert db.scalar(select(func.count()).select_from(RejudgingItemRow)) == 4


def test_postgres_stale_item_rolls_back_entire_batch(postgres):
    batch = create()
    finish(batch)
    with postgres() as db:
        db.get(SubmissionRow, '3').status = 'runtime_error'
        db.commit()
    with pytest.raises(AppError):
        rejudging.decide(store, 'contest', batch['rejudging_id'], 'apply', ACTOR)
    with postgres() as db:
        assert [row.status for row in db.scalars(select(SubmissionRow).order_by(SubmissionRow.submission_id))] == ['wrong_answer']*3 + ['runtime_error']
        assert db.scalar(select(RejudgingRow)).status == 'pending'
        assert db.scalar(select(func.count()).select_from(RejudgingItemRow).where(RejudgingItemRow.active_submission_id.is_not(None))) == 4


def test_claim_skips_a_package_update_lock_and_uses_one_complete_version(postgres):
    with postgres() as db:
        db.add(JudgeJobRow(judge_job_id='package-job',submission_id='0',contest_id='contest',
            division_id='division',status='pending',queue_position=1))
        db.commit()
    node = store.provision_node('package-node', SECRET, 1)
    with ThreadPoolExecutor(max_workers=1) as pool, postgres() as db:
        problem = db.scalar(select(ProblemRow).where(ProblemRow.problem_id=='problem').with_for_update())
        assert pool.submit(store.claim_jobs,node.judge_node_id,SECRET,1).result(timeout=3)==[]
        problem.time_limit_ms=2000
        db.get(CaseSet,'set').is_active=False
        db.add(CaseSet(testcase_set_id='new-package-set',problem_id='problem',version=2,is_active=True))
        db.flush()
        db.add(Case(testcase_set_id='new-package-set',display_order=1,input_storage_key='new-in',output_storage_key='new-out',
            input_sha256='c'*64,output_sha256='d'*64))
        db.commit()
    jobs=store.claim_jobs(node.judge_node_id,SECRET,1)
    assert len(jobs)==1 and jobs[0]['problem']['time_limit_ms']==2000
    assert jobs[0]['testcases'][0]['input_sha256']=='c'*64


def test_postgres_metadata_changes_wait_for_atomic_apply(postgres, monkeypatch):
    from concurrent.futures import TimeoutError
    from threading import Event
    batch = create()
    finish(batch)
    checked, allow_apply, modifying = Event(), Event(), Event()
    original_digest = rejudging.context_digest
    def pause_at_validation(context):
        value = original_digest(context)
        checked.set()
        assert allow_apply.wait(5)
        return value
    monkeypatch.setattr(rejudging, 'context_digest', pause_at_validation)
    def modify_case():
        with postgres() as db:
            case = db.get(Case, 'case')
            case.time_limit_ms_override = 2000
            modifying.set()
            db.commit()
    with ThreadPoolExecutor(max_workers=2) as pool:
        applying = pool.submit(rejudging.decide, store, 'contest', batch['rejudging_id'], 'apply', ACTOR)
        assert checked.wait(5)
        changing = pool.submit(modify_case)
        try:
            assert modifying.wait(5)
            with pytest.raises(TimeoutError):
                changing.result(timeout=0.1)
        finally:
            allow_apply.set()
        assert applying.result(timeout=5)['status'] == 'applied'
        changing.result(timeout=5)
    assert store.get_submission('0').status == 'accepted'


def test_postgres_inbox_upgrade_preserves_existing_read_and_hidden_history(postgres):
    with postgres() as db:
        db.add(ParticipantTeamRow(participant_team_id='team', contest_id='contest', division_id='division', team_name='Team'))
        db.flush()
        db.add(TeamMemberRow(team_member_id='member', participant_team_id='team', contest_id='contest',
            role='member', name='Member', email='Member@Test.Com'))
        db.flush()
        row = db.get(SubmissionRow, '0')
        row.participant_team_id, row.team_member_id = 'team', 'member'
        db.commit()
    batch = create()
    finish(batch)
    rejudging.decide(store, 'contest', batch['rejudging_id'], 'apply', ACTOR)
    with postgres() as db:
        notice = db.scalar(select(Notice))
        notice.read_at = notice.deleted_at = now_utc()
        db.commit()
        notice_id, item_id, created, read = notice.notification_id, notice.rejudging_item_id, notice.created_at, notice.read_at
    cfg = Config('alembic.ini')
    command.downgrade(cfg, '0041_rejudging_notifications')
    assert 'rejudging_notifications' in inspect(postgres.kw['bind']).get_table_names()
    command.upgrade(cfg, 'head')
    with postgres() as db:
        notice = db.get(Notice, notice_id)
        assert notice.created_at == created and notice.read_at == notice.deleted_at == read
        assert notice.recipient_email == 'member@test.com'
        assert notice.event_key == f'rejudging:{item_id}' and notice.notification_type == 'rejudging'
        from app.services.notifications import _fields
        fields = _fields(notice)
        assert fields['body'] == '틀렸습니다 → 맞았습니다' and fields['href'].endswith('submissionId=0')


def test_postgres_general_inbox_read_all_and_independent_dismissal(postgres):
    from app.services import notifications
    from tests.test_rejudging_notifications import login
    with postgres() as db:
        db.add(ParticipantTeamRow(participant_team_id='team', contest_id='contest', division_id='division', team_name='Team'))
        db.flush()
        db.add(TeamMemberRow(team_member_id='member', participant_team_id='team', contest_id='contest',
            role='member', name='Member', email='member@test.com'))
        db.commit()
    store.create_contest_notice('contest', 'First', 'body')
    store.create_contest_notice('contest', 'Second', 'body')
    token = login(postgres, 'member', email='member@test.com')
    feed = notifications.list_notifications(store, token, limit=1)
    assert feed['unread_count'] == 2 and feed['next_offset'] == 1
    nid = feed['items'][0]['notification_id']
    notifications.update_notification(store, token, nid, 'dismiss')
    assert notifications.list_notifications(store, token)['unread_count'] == 1
    assert notifications.read_all(store, token)['updated_count'] == 1
    assert notifications.list_notifications(store, token)['unread_count'] == 0
    notifications.update_notification(store, token, nid, 'unread')
    feed = notifications.list_notifications(store, token)
    item = next(item for item in feed['items'] if item['notification_id'] == nid)
    assert item['read_at'] is None and item['dismissed_at']
    notifications.update_notification(store, token, nid, 'delete')
    notifications.update_notification(store, token, nid, 'delete')
    assert notifications.list_notifications(store, token)['unread_count'] == 0


def test_postgres_expiry_cleanup_skips_locked_contests_and_preserves_judging_records(postgres, monkeypatch):
    from app.services import notifications
    with postgres() as db:
        db.add(ParticipantTeamRow(participant_team_id='team', contest_id='contest', division_id='division', team_name='Team'))
        db.flush()
        db.add(TeamMemberRow(team_member_id='member', participant_team_id='team', contest_id='contest',
            role='member', name='Member', email='member@test.com'))
        db.commit()
    for i in range(3):
        store.create_contest_notice('contest', str(i), 'body')
    with postgres() as db:
        rows = list(db.scalars(select(Notice)))
        rows[0].read_at = now_utc()
        rows[1].deleted_at = now_utc()
        db.get(ContestRow, 'contest').end_at = now_utc() - timedelta(hours=13)
        db.commit()
    monkeypatch.setattr(settings, 'notification_cleanup_batch_size', 2)
    with postgres() as locked:
        locked.scalar(select(ContestRow).where(ContestRow.contest_id == 'contest').with_for_update())
        with ThreadPoolExecutor(max_workers=1) as pool:
            assert pool.submit(notifications.cleanup_expired_notifications, store).result(timeout=5) == 0
        locked.rollback()
    assert notifications.cleanup_expired_notifications(store) == 2
    assert notifications.cleanup_expired_notifications(store) == 1
    with postgres() as db:
        assert db.scalar(select(func.count()).select_from(Notice)) == 0
        assert db.scalar(select(func.count()).select_from(SubmissionRow)) == 4
        assert db.get(ContestRow, 'contest')
