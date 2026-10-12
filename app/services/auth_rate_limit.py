"""Persistent atomic counters shared across API workers."""
import hashlib
import time
from sqlalchemy import update
from sqlalchemy.exc import IntegrityError
from app.database import SessionLocal
from app.orm_models import AuthRateLimitRow
from app.services.errors import AppError


def consume(scope, identity, limit, seconds):
    key = scope + ':' + hashlib.sha256(identity.lower().encode()).hexdigest()
    at = int(time.time())
    with SessionLocal() as db:
        row = db.get(AuthRateLimitRow, key)
        if not row:
            db.add(AuthRateLimitRow(key=key, started_at=at, count=1))
            try:
                db.commit()
                return
            except IntegrityError:
                db.rollback()
                row = db.get(AuthRateLimitRow, key)
        if at - row.started_at >= seconds:
            reset = db.execute(update(AuthRateLimitRow).where(AuthRateLimitRow.key==key, AuthRateLimitRow.started_at==row.started_at).values(started_at=at,count=1))
            db.commit()
            if reset.rowcount:
                return
        increment = db.execute(update(AuthRateLimitRow).where(AuthRateLimitRow.key==key, AuthRateLimitRow.count < limit).values(count=AuthRateLimitRow.count+1))
        db.commit()
        if not increment.rowcount:
            raise AppError(429, 'auth_rate_limited', '요청이 많습니다. 잠시 후 다시 시도하세요.')
