import asyncio
import time

from app.services.usage_ingest import purge_usage_events
from app.services.log_retention import cleanup_expired_logs

from app.services.store import store
from app.settings import settings


async def run_forever() -> None:
    next_usage_cleanup = 0.0
    next_log_cleanup = 0.0
    while True:
        if time.monotonic() >= next_log_cleanup:
            try:
                removed = await asyncio.to_thread(cleanup_expired_logs)
                batch = max(1, min(settings.log_cleanup_batch_size, 5000))
                next_log_cleanup = time.monotonic() + (60 if any(count >= batch for count in removed.values()) else 3600)
                if any(removed.values()):
                    print(f"[notice-worker] expired logs removed: {removed}", flush=True)
            except Exception as exc:
                print(f"[notice-worker] log cleanup failed: {type(exc).__name__}", flush=True)
                next_log_cleanup = time.monotonic() + 60
        if time.monotonic() >= next_usage_cleanup:
            try:
                removed = purge_usage_events()
                next_usage_cleanup = time.monotonic() + (60 if removed >= 10000 else 3600)
            except Exception as exc:
                print(f"[notice-worker] usage cleanup failed: {exc}")
                next_usage_cleanup = time.monotonic() + 60
        try:
            created_count = store.enqueue_due_contest_emergency_notices()
            if created_count:
                print(f"[notice-worker] created {created_count} scheduled emergency notices")
        except Exception as exc:
            print(f"[notice-worker] failed to create scheduled emergency notices: {exc}")
        await asyncio.sleep(settings.notice_worker_poll_interval_seconds)


def main() -> None:
    asyncio.run(run_forever())


if __name__ == "__main__":
    main()
