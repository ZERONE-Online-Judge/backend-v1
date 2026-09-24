"""Short-lived, shared result caching. Authorization always remains in the DB."""
import hashlib
import json
import logging
import time
from datetime import datetime
from functools import lru_cache
from threading import Lock

import redis
from redis.backoff import NoBackoff
from redis.retry import Retry
from sqlalchemy import event, inspect
from sqlalchemy.orm import Session

from app.settings import settings

logger = logging.getLogger(__name__)
_locks = [Lock() for _ in range(64)]
_unavailable_until = 0.0
_BOARD_TABLES = {"contests", "contest_divisions", "participant_teams", "problems", "submissions", "scoreboard_releases"}
_SESSION_TABLES = {"general_sessions", "team_sessions"}


def _encode(value):
    if isinstance(value, datetime):
        return {"__zoj_datetime__": value.isoformat()}
    raise TypeError(f"Unsupported cache value: {type(value).__name__}")


def _decode(value):
    if set(value) == {"__zoj_datetime__"}:
        return datetime.fromisoformat(value["__zoj_datetime__"])
    return value


@lru_cache(maxsize=1)
def _client(url):
    return redis.Redis.from_url(url, socket_connect_timeout=0.15, socket_timeout=0.15,
                               max_connections=16, retry=Retry(NoBackoff(), 0), decode_responses=True)


def _failed():
    global _unavailable_until
    now = time.monotonic()
    if now >= _unavailable_until:
        logger.warning("Result cache unavailable; using database results")
    _unavailable_until = now + 3


def invalidate(group: str) -> None:
    if not settings.redis_url:
        return
    try:
        # Do not skip invalidations during the read circuit breaker's cooldown.
        _client(settings.redis_url).incr(f"zoj:cache:v1:generation:{group}")
    except redis.RedisError:
        _failed()


def cached_result(group: str, identity, ttl: int, compute):
    if not settings.redis_url or ttl <= 0 or time.monotonic() < _unavailable_until:
        return compute()
    client = _client(settings.redis_url)
    try:
        generation = client.get(f"zoj:cache:v1:generation:{group}") or "0"
        material = json.dumps([settings.release_version, group, generation, identity],
                              sort_keys=True, default=_encode, separators=(",", ":"))
        digest = hashlib.sha256(material.encode()).hexdigest()
        key = f"zoj:cache:v1:result:{group}:{digest}"
        value = client.get(key)
        if value is not None:
            return json.loads(value, object_hook=_decode)
    except (redis.RedisError, ValueError):
        _failed()
        return compute()
    # Bound simultaneous recomputation in each API worker; Redis shares the
    # computed result across workers. Locks are fixed-size, not per arbitrary URL.
    with _locks[int(digest[:8], 16) % len(_locks)]:
        try:
            value = client.get(key)
            if value is not None:
                return json.loads(value, object_hook=_decode)
        except (redis.RedisError, ValueError):
            _failed()
            return compute()
        value = compute()
        try:
            encoded = json.dumps(value, default=_encode, separators=(",", ":"))
            # Do not let an unusually large contest monopolize Redis memory.
            if len(encoded.encode()) <= 2 * 1024 * 1024:
                client.set(key, encoded, ex=ttl)
        except redis.RedisError:
            _failed()
        return value


@event.listens_for(Session, "before_flush")
def _collect_changes(session, flush_context, instances):
    groups = session.info.setdefault("result_cache_groups", set())
    for row in session.new | session.dirty | session.deleted:
        table = getattr(row, "__tablename__", "")
        if table in _BOARD_TABLES:
            groups.add("scoreboard")
        if table == "access_logs" or (table in _SESSION_TABLES and (
            row in session.new or row in session.deleted or inspect(row).attrs.revoked_at.history.has_changes()
        )):
            groups.add("access-stats")


@event.listens_for(Session, "do_orm_execute")
def _collect_bulk_changes(state):
    if not (state.is_update or state.is_delete):
        return
    table = getattr(getattr(state.statement, "table", None), "name", "")
    groups = state.session.info.setdefault("result_cache_groups", set())
    if table in _BOARD_TABLES:
        groups.add("scoreboard")
    if table == "access_logs" or table in _SESSION_TABLES:
        groups.add("access-stats")


@event.listens_for(Session, "after_commit")
def _publish_changes(session):
    for group in session.info.pop("result_cache_groups", ()):
        invalidate(group)


@event.listens_for(Session, "after_rollback")
def _discard_changes(session):
    session.info.pop("result_cache_groups", None)
