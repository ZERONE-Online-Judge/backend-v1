"""Single bounded consumer; restart-safe reports live in the database."""

import time
from app.services import verification_ai
from app.settings import settings


def main():
    while True:
        try:
            verification_ai.enqueue_completed()
            worked = verification_ai.process_one()
        except Exception:
            # Never log request contents or provider exceptions containing secrets.
            print("[verification-ai] worker cycle failed; retrying", flush=True)
            worked = False
        time.sleep(0.1 if worked else max(1, settings.verification_ai_poll_seconds))


if __name__ == "__main__":
    main()
