import asyncio
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from threading import Lock
from types import SimpleNamespace
from uuid import uuid4

import pytest
import redis
from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError
from sqlalchemy.schema import CreateSchema, DropSchema

os.environ.setdefault("ENABLE_DEMO_SEED", "true")
os.environ.setdefault("ALLOW_EMPTY_OTP", "true")

from app.database import engine
from app.models import ContestStatus, now_utc
from app.orm_models import AccessLogRow, GeneralSessionRow, JudgeAgentLogRow, OperationalAuditLogRow, SubmissionRow
from app.services import node_credentials, result_cache, session_events as sessions
from app.services.log_retention import cleanup_expired_logs
from app.services.security import hash_password
from app.services.store import store
from app.settings import settings


class MemoryRedis:
    def __init__(self):
        self.values = {}
        self.lock = Lock()

    def get(self, key):
        with self.lock:
            value, expiry = self.values.get(key, (None, float("inf")))
            return value if expiry > time.monotonic() else None

    def set(self, key, value, ex):
        with self.lock:
            self.values[key] = (value, time.monotonic() + ex)

    def incr(self, key):
        with self.lock:
            old = self.values.get(key, ("0", float("inf")))[0]
            self.values[key] = (str(int(old) + 1), float("inf"))


@pytest.fixture
def cache(monkeypatch):
    url = os.environ.get("TEST_REDIS_URL")
    backend = redis.Redis.from_url(url, decode_responses=True) if url else MemoryRedis()
    if url:
        assert backend.ping()
    monkeypatch.setattr(settings, "redis_url", url or "redis://test-only")
    monkeypatch.setattr(result_cache, "_client", lambda _: backend)
    monkeypatch.setattr(result_cache, "_unavailable_until", 0)
    return backend


def test_node_cache_rechecks_current_authorization_and_secret_rotation(monkeypatch):
    row = SimpleNamespace(judge_node_id=str(uuid4()), schedulable=True, node_secret_hash=hash_password("original"))
    original = node_credentials.verify_password
    calls = []
    monkeypatch.setattr(node_credentials, "verify_password", lambda secret, hashed: calls.append(secret) or original(secret, hashed))
    for _ in range(20):
        assert node_credentials.valid_node_credential(row, "original")
    assert calls == ["original"]
    row.schedulable = False
    assert not node_credentials.valid_node_credential(row, "original")
    row.schedulable = True
    assert not node_credentials.valid_node_credential(row, "wrong")
    row.node_secret_hash = hash_password("rotated")
    assert not node_credentials.valid_node_credential(row, "original")
    assert node_credentials.valid_node_credential(row, "rotated")
    now = time.monotonic()
    monkeypatch.setattr(node_credentials.time, "monotonic", lambda: now + 31)
    assert node_credentials.valid_node_credential(row, "rotated")
    assert calls == ["original", "wrong", "original", "rotated", "rotated"]


def test_cache_coalesces_concurrent_misses_and_preserves_result_types(cache):
    identity = uuid4().hex
    calls = []
    moment = now_utc()
    def compute():
        calls.append(1)
        time.sleep(0.02)
        return {"rows": [{"at": moment, "value": 7}]}
    def read():
        return result_cache.cached_result("test", identity, 2, compute)
    with ThreadPoolExecutor(max_workers=12) as pool:
        values = list(pool.map(lambda _: read(), range(24)))
    assert len(calls) == 1
    assert all(value["rows"][0]["at"] == moment for value in values)
    values[0]["rows"][0]["value"] = 99
    assert read()["rows"][0]["value"] == 7


def test_cache_expires_and_database_fallback_is_not_cached(monkeypatch, cache):
    identity = uuid4().hex
    calls = []
    def read():
        return result_cache.cached_result("test", identity, 1, lambda: calls.append(1) or len(calls))
    assert read() == read() == 1
    time.sleep(1.05)
    assert read() == 2
    class Broken:
        def get(self, key):
            raise redis.ConnectionError("unavailable")
    monkeypatch.setattr(result_cache, "_client", lambda _: Broken())
    assert read() == 3
    assert read() == 4


@pytest.fixture
def board():
    now = now_utc()
    contest = store.create_contest("Cache checks", "Test", "", start_at=now-timedelta(hours=1),
        freeze_at=now+timedelta(seconds=10), end_at=now+timedelta(hours=1), status=ContestStatus.RUNNING)
    cid = contest.contest_id
    division = store.create_contest_division(cid, "A", "A")
    team = store.create_participant_team(cid, division.division_id, "Team", "Member", f"cache-{uuid4().hex}@example.com", [])
    problem = store.create_problem(cid, division.division_id, "A", "A", "secret statement", 1000, 128, {}, 1)
    with store._session() as db:
        submission = SubmissionRow(contest_id=cid, division_id=division.division_id,
            participant_team_id=team.participant_team_id, team_member_id=team.members[0].team_member_id,
            problem_id=problem.problem_id, language="cpp17", source_code="DO NOT LOAD INTO SCOREBOARD",
            status="accepted", submitted_at=now+timedelta(seconds=20))
        db.add(submission)
        db.commit()
        sid = submission.submission_id
    return cid, division.division_id, sid, now


def test_scoreboard_cache_separates_audience_and_obeys_freeze_time(monkeypatch, cache, board):
    cid, did, _, now = board
    import app.services.store as store_module
    monkeypatch.setattr(store_module, "now_utc", lambda: now)
    sql = []
    def observed(conn, cursor, statement, parameters, context, executemany):
        sql.append(statement)
    event.listen(engine, "before_cursor_execute", observed)
    try:
        public = store.scoreboard_rows(cid, did, True)
        assert not public["frozen"] and public["rows"][0]["solved"] == 1
        assert "problem_stats" not in public
        assert any("FROM submissions" in query for query in sql)
        assert all("source_code" not in query for query in sql if "FROM submissions" in query)
        sql.clear()
        assert store.scoreboard_rows(cid, did, True) == public
        assert not any("FROM submissions" in query for query in sql)
        internal = store.scoreboard_rows(cid, did, False)
        assert "problem_stats" in internal
        monkeypatch.setattr(store_module, "now_utc", lambda: now+timedelta(seconds=11))
        frozen = store.scoreboard_rows(cid, did, True)
        assert frozen["frozen"] and frozen["rows"][0]["solved"] == 0
        assert store.scoreboard_rows(cid, did, False)["rows"][0]["solved"] == 1
    finally:
        event.remove(engine, "before_cursor_execute", observed)


def test_scoreboard_cache_invalidated_on_commit_but_not_rollback(cache, board):
    cid, did, sid, _ = board
    assert store.scoreboard_rows(cid, did, False)["rows"][0]["solved"] == 1
    with store._session() as db:
        row = db.get(SubmissionRow, sid)
        row.status = "wrong_answer"
        db.flush()
        db.rollback()
    assert store.scoreboard_rows(cid, did, False)["rows"][0]["solved"] == 1
    with store._session() as db:
        db.get(SubmissionRow, sid).status = "wrong_answer"
        db.commit()
    assert store.scoreboard_rows(cid, did, False)["rows"][0]["solved"] == 0


def test_access_statistics_refresh_after_new_log(cache):
    cid = str(uuid4())
    assert store.access_log_stats(contest_id=cid)["total_count"] == 0
    store.append_access_log(event_type="login_failed", account_scope="general", contest_id=cid, email="test@example.com")
    stats = store.access_log_stats(contest_id=cid)
    assert stats["total_count"] == stats["failed_count"] == stats["unique_account_count"] == 1


def test_thousand_sessions_use_two_queries_and_detect_expiry_and_revocation():
    now = now_utc()
    ids = [str(uuid4()) for _ in range(1000)]
    with store._session() as db:
        db.add_all([GeneralSessionRow(general_session_id=identity, email="batch-check@example.com",
            access_token_hash=uuid4().hex, refresh_token_hash=uuid4().hex,
            access_expires_at=now+timedelta(minutes=1),
            refresh_expires_at=now+timedelta(hours=1) if index != 998 else now-timedelta(seconds=1),
            revoked_at=now if index == 999 else None) for index,identity in enumerate(ids)])
        db.commit()
    watches = {sessions.SessionWatch("general_access", identity) for identity in ids}
    queries = []
    def observed(conn, cursor, statement, parameters, context, executemany):
        queries.append(statement)
    event.listen(engine, "before_cursor_execute", observed)
    try:
        active = sessions.active_session_watches(watches)
        assert len(active) == 998
        assert len(queries) == 2
    finally:
        event.remove(engine, "before_cursor_execute", observed)
        with store._session() as db:
            db.query(GeneralSessionRow).filter(GeneralSessionRow.general_session_id.in_(ids)).delete(synchronize_session=False)
            db.commit()


def test_monitor_shares_one_check_for_many_tabs_and_releases_subscriptions(monkeypatch):
    checks = []
    monkeypatch.setattr(sessions, "active_session_watches", lambda watches: checks.append(watches) or set())
    async def verify():
        monitor = sessions.SessionMonitor()
        watch = sessions.SessionWatch("general_access", "revoked")
        events = [monitor.subscribe(watch) for _ in range(1000)]
        await asyncio.wait_for(asyncio.gather(*(event.wait() for event in events)), timeout=3)
        assert checks == [{watch}]
        for event in events:
            await monitor.unsubscribe(watch, event)
        assert not monitor.watchers and monitor.task is None
    asyncio.run(verify())


def test_monitor_retries_database_failure_without_revoking_sessions(monkeypatch):
    calls = []
    def check(watches):
        calls.append(1)
        if len(calls) == 1:
            raise OperationalError("test", {}, Exception("database temporarily unavailable"))
        return watches if len(calls) == 2 else set()
    monkeypatch.setattr(sessions, "active_session_watches", check)
    monkeypatch.setattr(sessions, "SESSION_CHECK_INTERVAL_SECONDS", 0.05)
    async def verify():
        monitor = sessions.SessionMonitor()
        watch = sessions.SessionWatch("general_access", "retry")
        revoked = monitor.subscribe(watch)
        try:
            while len(calls) < 2:
                await asyncio.sleep(0.01)
            assert not revoked.is_set()
            await asyncio.wait_for(revoked.wait(), timeout=2)
            assert len(calls) >= 3
        finally:
            await monitor.unsubscribe(watch, revoked)
    asyncio.run(asyncio.wait_for(verify(), timeout=3))


@pytest.mark.skipif(not os.getenv("ZOJ_TEST_POSTGRES_URL"), reason="Requires disposable PostgreSQL")
def test_postgres_retention_skips_locked_rows_and_bounds_deletion(monkeypatch):
    import app.services.log_retention as retention
    schema = "retention_" + uuid4().hex
    admin = create_engine(os.environ["ZOJ_TEST_POSTGRES_URL"])
    with admin.begin() as db:
        db.execute(CreateSchema(schema))
    local_engine = create_engine(os.environ["ZOJ_TEST_POSTGRES_URL"],
        connect_args={"options": f"-csearch_path={schema}"})
    factory = sessionmaker(local_engine)
    monkeypatch.setattr(retention, "SessionLocal", factory)
    monkeypatch.setattr(settings, "log_cleanup_batch_size", 2)
    monkeypatch.setattr(settings, "access_log_retention_days", 365)
    monkeypatch.setattr(settings, "audit_log_retention_days", 0)
    monkeypatch.setattr(settings, "judge_log_retention_days", 0)
    try:
        AccessLogRow.__table__.create(local_engine)
        now = now_utc()
        with factory() as db:
            db.add_all([AccessLogRow(access_log_id=str(index), event_type="login_failed",
                account_scope="general", created_at=now-timedelta(days=400-index)) for index in range(4)])
            db.add(AccessLogRow(access_log_id="recent", event_type="login_failed",
                account_scope="general", created_at=now))
            db.commit()
        with factory() as locked:
            locked.scalar(select(AccessLogRow).where(AccessLogRow.access_log_id == "0").with_for_update())
            assert cleanup_expired_logs()["access_logs"] == 2
            with factory() as db:
                assert set(db.scalars(select(AccessLogRow.access_log_id))) == {"0", "3", "recent"}
            locked.rollback()
        assert cleanup_expired_logs()["access_logs"] == 2
        with factory() as db:
            assert list(db.scalars(select(AccessLogRow.access_log_id))) == ["recent"]
    finally:
        local_engine.dispose()
        with admin.begin() as db:
            db.execute(DropSchema(schema, cascade=True))
        admin.dispose()


def test_retention_removes_only_expired_rows_in_bounded_batches(monkeypatch, tmp_path):
    import app.services.log_retention as retention
    local_engine = create_engine(f"sqlite:///{tmp_path / 'retention.db'}")
    models = [AccessLogRow, OperationalAuditLogRow, JudgeAgentLogRow]
    for model in models:
        model.__table__.create(local_engine)
    factory = sessionmaker(local_engine)
    monkeypatch.setattr(retention, "SessionLocal", factory)
    monkeypatch.setattr(settings, "log_cleanup_batch_size", 2)
    monkeypatch.setattr(settings, "access_log_retention_days", 365)
    monkeypatch.setattr(settings, "audit_log_retention_days", 365)
    monkeypatch.setattr(settings, "judge_log_retention_days", 90)
    now = now_utc()
    with factory() as db:
        for days in [400, 399, 398, 364]:
            db.add(AccessLogRow(event_type="login_failed", account_scope="general", created_at=now-timedelta(days=days)))
        for days in [400, 364]:
            db.add(OperationalAuditLogRow(scope="operator", action="test", method="POST", path="/test", status_code=200, created_at=now-timedelta(days=days)))
        for days in [100, 99, 89]:
            db.add(JudgeAgentLogRow(judge_node_id="deleted-node", node_name="old", message="test", created_at=now-timedelta(days=days)))
        db.commit()
    assert cleanup_expired_logs() == {"access_logs": 2, "operational_audit_logs": 1, "judge_agent_logs": 2}
    assert cleanup_expired_logs() == {"access_logs": 1, "operational_audit_logs": 0, "judge_agent_logs": 0}
    with factory() as db:
        assert all(len(list(db.scalars(select(model)))) == 1 for model in models)
    monkeypatch.setattr(settings, "access_log_retention_days", 0)
    with factory() as db:
        db.add(AccessLogRow(event_type="login_failed", account_scope="general", created_at=now-timedelta(days=1000)))
        db.commit()
    assert cleanup_expired_logs()["access_logs"] == 0
    local_engine.dispose()
