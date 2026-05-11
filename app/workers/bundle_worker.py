import asyncio

from app.services.store import store
from app.settings import settings


async def run_forever() -> None:
    while True:
        jobs = store.claim_bundle_warm_jobs(settings.bundle_worker_batch_size)
        if not jobs:
            await asyncio.sleep(settings.bundle_worker_poll_interval_seconds)
            continue
        for job_id, contest_id, problem_id, attempts in jobs:
            try:
                store.warm_problem_judge_bundle(contest_id, problem_id)
            except Exception as exc:
                requeue = attempts < settings.bundle_worker_max_attempts
                store.fail_bundle_warm_job(job_id, str(exc), requeue=requeue)
                print(f"[bundle-worker] failed job={job_id} contest={contest_id} problem={problem_id} requeue={requeue}: {exc}")
            else:
                store.complete_bundle_warm_job(job_id)
                print(f"[bundle-worker] warmed contest={contest_id} problem={problem_id}")


def main() -> None:
    asyncio.run(run_forever())


if __name__ == "__main__":
    main()
