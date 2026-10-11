from datetime import timedelta

from sqlalchemy import func, select

from app.models import now_utc
from app.orm_models import ContestRow, ContestNoticeRow, NotificationRow as Notice, SubmissionRow
from app.services import notifications as inbox
from app.services.store import store
from app.settings import settings
from tests.test_rejudging_notifications import database, finished, leased, participant, login, headers, client


def end_at(database, when, status='ended'):
    with database() as db:
        contest = db.get(ContestRow, 'contest')
        contest.end_at, contest.status = when, status
        db.commit()


def count(database):
    with database() as db:
        return db.scalar(select(func.count()).select_from(Notice))


def test_notifications_live_until_end_plus_twelve_hours_then_hide_before_cleanup(database, participant, monkeypatch):
    now = now_utc()
    store.create_contest_notice('contest', 'Notice', 'body')
    token = login(database)
    end_at(database, now - timedelta(hours=12))
    monkeypatch.setattr(inbox, 'now_utc', lambda: now - timedelta(microseconds=1))
    assert inbox.list_notifications(store, token)['unread_count'] == 1
    monkeypatch.setattr(inbox, 'now_utc', lambda: now)
    assert inbox.list_notifications(store, token)['items'] == []
    assert inbox.list_notifications(store, token)['unread_count'] == 0
    assert count(database) == 3  # API filtering does not depend on a successful worker cycle.
    with database() as db:
        nid = db.scalar(select(Notice.notification_id).where(Notice.team_member_id == 'author'))
    assert client.post(f'/api/notifications/{nid}/read', headers=headers(token)).status_code == 404
    assert inbox.read_all(store, token)['updated_count'] == 0
    assert inbox.cleanup_expired_notifications(store) == 3
    assert count(database) == 0
    with database() as db:
        assert db.scalar(select(func.count()).select_from(ContestNoticeRow)) == 1
        assert db.scalar(select(func.count()).select_from(SubmissionRow)) == 1
        assert db.get(ContestRow, 'contest')


def test_extending_end_time_preserves_existing_notifications_and_delays_expiry(database, participant, monkeypatch):
    now = now_utc()
    monkeypatch.setattr(inbox, 'now_utc', lambda: now)
    store.create_contest_notice('contest', 'Notice', 'body')
    token = login(database)
    end_at(database, now - timedelta(hours=13))
    assert inbox.list_notifications(store, token)['items'] == []
    end_at(database, now - timedelta(hours=1))
    assert inbox.list_notifications(store, token)['unread_count'] == 1
    assert inbox.cleanup_expired_notifications(store) == 0
    assert count(database) == 3


def test_after_expiry_new_events_are_not_created_and_do_not_reappear(database, participant, monkeypatch):
    now = now_utc()
    monkeypatch.setattr(inbox, 'now_utc', lambda: now)
    end_at(database, now - timedelta(hours=13))
    notice = store.create_contest_notice('contest', 'After expiry', 'body')
    assert count(database) == 0
    with database() as db:
        inbox.create_notice(db, db.get(ContestNoticeRow, notice.contest_notice_id))
        db.commit()
    assert count(database) == 0


def test_cleanup_is_batched_and_removes_read_unread_and_hidden_entries(database, participant, monkeypatch):
    now = now_utc()
    monkeypatch.setattr(inbox, 'now_utc', lambda: now)
    monkeypatch.setattr(settings, 'notification_cleanup_batch_size', 2)
    store.create_contest_notice('contest', 'Notice', 'body')
    with database() as db:
        rows = list(db.scalars(select(Notice).order_by(Notice.team_member_id)))
        rows[0].read_at = now
        rows[1].deleted_at = now
        db.commit()
    end_at(database, now - timedelta(hours=13))
    assert inbox.cleanup_expired_notifications(store) == 2
    assert count(database) == 1
    assert inbox.cleanup_expired_notifications(store) == 1
    assert inbox.cleanup_expired_notifications(store) == 0


def test_lifetime_is_configurable_and_placeholder_schedules_do_not_expire(database, participant, monkeypatch):
    now = now_utc()
    monkeypatch.setattr(inbox, 'now_utc', lambda: now)
    monkeypatch.setattr(settings, 'notification_grace_hours', 24)
    store.create_contest_notice('contest', 'Notice', 'body')
    end_at(database, now - timedelta(hours=13))
    token = login(database)
    assert inbox.list_notifications(store, token)['unread_count'] == 1
    assert inbox.cleanup_expired_notifications(store) == 0
    monkeypatch.setattr(settings, 'notification_grace_hours', 0)
    assert inbox.list_notifications(store, token)['items'] == []
    for status in ['draft', 'schedule_tbd']:
        end_at(database, now - timedelta(days=100), status)
        assert inbox.list_notifications(store, token)['unread_count'] == 1
        assert inbox.cleanup_expired_notifications(store) == 0
