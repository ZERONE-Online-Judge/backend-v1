from collections.abc import Generator

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from app.settings import settings


connect_args = {"check_same_thread": False} if settings.database_url.startswith("sqlite") else {}
engine = create_engine(settings.database_url, connect_args=connect_args, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False, future=True)


class Base(DeclarativeBase):
    pass


def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def create_schema() -> None:
    from app import orm_models  # noqa: F401

    Base.metadata.create_all(bind=engine)
    if settings.database_url.startswith("sqlite"):
        inspector = inspect(engine)
        if "judge_jobs" in inspector.get_table_names():
            columns = {column["name"] for column in inspector.get_columns("judge_jobs")}
            if "leased_at" not in columns:
                with engine.begin() as connection:
                    connection.execute(text("ALTER TABLE judge_jobs ADD COLUMN leased_at DATETIME"))
        if "submissions" in inspector.get_table_names():
            columns = {column["name"] for column in inspector.get_columns("submissions")}
            with engine.begin() as connection:
                if "judge_message" not in columns:
                    connection.execute(text("ALTER TABLE submissions ADD COLUMN judge_message TEXT"))
                if "failed_testcase_order" not in columns:
                    connection.execute(text("ALTER TABLE submissions ADD COLUMN failed_testcase_order INTEGER"))
                if "progress_current" not in columns:
                    connection.execute(text("ALTER TABLE submissions ADD COLUMN progress_current INTEGER"))
                if "progress_total" not in columns:
                    connection.execute(text("ALTER TABLE submissions ADD COLUMN progress_total INTEGER"))
                if "runtime_ms" not in columns:
                    connection.execute(text("ALTER TABLE submissions ADD COLUMN runtime_ms INTEGER"))
                if "memory_kb" not in columns:
                    connection.execute(text("ALTER TABLE submissions ADD COLUMN memory_kb INTEGER"))
