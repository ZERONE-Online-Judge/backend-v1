import asyncio
from types import SimpleNamespace

import pytest

from app.workers import notice_worker as worker


@pytest.mark.parametrize('fail_first', [False, True])
def test_notice_worker_schedules_notification_cleanup_and_retries_without_stopping(monkeypatch, fail_first):
    calls, clock, cycles = [], [0.0], []
    fake_store = SimpleNamespace(enqueue_due_contest_emergency_notices=lambda: cycles.append('notice') or 0)
    def cleanup(store):
        assert store is fake_store
        calls.append(clock[0])
        if fail_first and len(calls) == 1:
            raise RuntimeError('temporary database failure')
        return 0
    class StopWorker(Exception):
        pass
    async def sleep(_):
        if len(cycles) >= 2:
            raise StopWorker()
        clock[0] += 60
    monkeypatch.setattr(worker, 'store', fake_store)
    monkeypatch.setattr(worker, 'cleanup_expired_notifications', cleanup)
    monkeypatch.setattr(worker, 'cleanup_expired_logs', lambda: {})
    monkeypatch.setattr(worker, 'purge_usage_events', lambda: 0)
    monkeypatch.setattr(worker, 'time', SimpleNamespace(monotonic=lambda: clock[0]))
    monkeypatch.setattr(worker.asyncio, 'sleep', sleep)
    monkeypatch.setattr(worker.settings, 'notification_cleanup_interval_seconds', 60)
    with pytest.raises(StopWorker):
        asyncio.run(worker.run_forever())
    assert calls == [0.0, 60.0] and len(cycles) == 2
