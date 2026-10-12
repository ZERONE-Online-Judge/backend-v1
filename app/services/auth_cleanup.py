"""Bound expired authentication records without deleting accounts or sessions."""

import time
from sqlalchemy import delete, select, tuple_, text
from app.database import SessionLocal
from app.orm_models import AuthRateLimitRow, PasswordChallengeRow


def cleanup_expired_auth(batch=1000):
    at = int(time.time())
    counts = {}
    for model, keys, expired in [
        (
            AuthRateLimitRow,
            [AuthRateLimitRow.key],
            AuthRateLimitRow.started_at < at - 86400,
        ),
        (
            PasswordChallengeRow,
            [PasswordChallengeRow.email, PasswordChallengeRow.purpose],
            PasswordChallengeRow.expires_at < at - 3600,
        ),
    ]:
        with SessionLocal() as db:
            if db.bind.dialect.name == "postgresql":
                db.execute(text("SET LOCAL statement_timeout = '3s'"))
                db.execute(text("SET LOCAL lock_timeout = '500ms'"))
            rows = list(
                db.execute(
                    select(*keys)
                    .where(expired)
                    .limit(batch)
                    .with_for_update(skip_locked=True)
                )
            )
            if rows:
                condition = (
                    keys[0].in_([row[0] for row in rows])
                    if len(keys) == 1
                    else tuple_(*keys).in_([tuple(row) for row in rows])
                )
                db.execute(delete(model).where(expired, condition))
            db.commit()
            counts[model.__tablename__] = len(rows)
    return counts
