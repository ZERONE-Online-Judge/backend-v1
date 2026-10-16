"""Reuse one connection inside a synchronous request, never across requests."""
from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps

from app.database import SessionLocal

_current = ContextVar("request_database_session", default=None)


@contextmanager
def borrowed_session(session):
    try:
        yield session
    except BaseException:
        session.rollback()
        raise


def current_session(fallback=SessionLocal):
    session = _current.get()
    return borrowed_session(session) if session is not None else fallback()


def request_database(function=None, *, session_factory=SessionLocal):
    if function is None:
        return lambda target: request_database(target, session_factory=session_factory)
    @wraps(function)
    def wrapped(*args, **kwargs):
        if _current.get() is not None:
            return function(*args, **kwargs)
        with session_factory() as session:
            token = _current.set(session)
            try:
                return function(*args, **kwargs)
            finally:
                _current.reset(token)
    return wrapped
