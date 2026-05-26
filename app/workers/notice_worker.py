import asyncio

from app.services.store import store
from app.settings import settings


async def run_forever() -> None:
    while True:
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
