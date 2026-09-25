"""Bounded parallel consumers; claims and budgets are serialized in the DB."""

import signal
import threading
from concurrent.futures import ThreadPoolExecutor
from app.services import verification_ai, verification_agent
from app.settings import settings


def consume(stopping):
    while not stopping.is_set():
        try:
            worked = verification_agent.process_one() or verification_ai.process_one()
        except Exception:
            print("[verification-ai] worker cycle failed; retrying", flush=True)
            worked = False
        stopping.wait(0.1 if worked else max(1, settings.verification_ai_poll_seconds))


def main():
    stopping = threading.Event()

    def stop(signum, frame):
        stopping.set()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    with ThreadPoolExecutor(max_workers=verification_ai.concurrency()) as pool:
        futures = [
            pool.submit(consume, stopping) for _ in range(verification_ai.concurrency())
        ]
        for future in futures:
            future.result()


if __name__ == "__main__":
    main()
