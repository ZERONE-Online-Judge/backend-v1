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
    with engine.begin() as connection:
        connection.execute(text("CREATE INDEX IF NOT EXISTS idx_submissions_submitted_id ON submissions (submitted_at DESC, submission_id DESC)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS idx_submissions_contest_submitted_id ON submissions (contest_id, submitted_at DESC, submission_id DESC)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS idx_submissions_contest_division_submitted_id ON submissions (contest_id, division_id, submitted_at DESC, submission_id DESC)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS idx_submissions_contest_team_submitted_id ON submissions (contest_id, participant_team_id, submitted_at DESC, submission_id DESC)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS idx_submissions_contest_problem_submitted_id ON submissions (contest_id, problem_id, submitted_at DESC, submission_id DESC)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS idx_judge_jobs_contest_status_queue ON judge_jobs (contest_id, status, queue_position)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS idx_judge_jobs_submission_created ON judge_jobs (submission_id, created_at DESC)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS idx_judge_agent_logs_node_created ON judge_agent_logs (judge_node_id, created_at DESC, judge_agent_log_id DESC)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS idx_operational_audit_scope_created ON operational_audit_logs (scope, created_at DESC, operational_audit_log_id DESC)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS idx_operational_audit_contest_created ON operational_audit_logs (contest_id, created_at DESC, operational_audit_log_id DESC)"))
        connection.execute(text("CREATE INDEX IF NOT EXISTS idx_operational_audit_actor_created ON operational_audit_logs (actor_email, created_at DESC, operational_audit_log_id DESC)"))
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
        if "contests" in inspector.get_table_names():
            columns = {column["name"] for column in inspector.get_columns("contests")}
            with engine.begin() as connection:
                if "problem_access_after_end" not in columns:
                    connection.execute(text("ALTER TABLE contests ADD COLUMN problem_access_after_end VARCHAR(32) DEFAULT 'private' NOT NULL"))
                if "scoreboard_access_after_end" not in columns:
                    connection.execute(text("ALTER TABLE contests ADD COLUMN scoreboard_access_after_end VARCHAR(32) DEFAULT 'private' NOT NULL"))
                if "submission_access_after_end" not in columns:
                    connection.execute(text("ALTER TABLE contests ADD COLUMN submission_access_after_end VARCHAR(32) DEFAULT 'private' NOT NULL"))
                if "board_access_after_end" not in columns:
                    connection.execute(text("ALTER TABLE contests ADD COLUMN board_access_after_end VARCHAR(32) DEFAULT 'participants' NOT NULL"))
                if "board_write_after_end" not in columns:
                    connection.execute(text("ALTER TABLE contests ADD COLUMN board_write_after_end BOOLEAN DEFAULT 0 NOT NULL"))
                if "notice_access_after_end" not in columns:
                    connection.execute(text("ALTER TABLE contests ADD COLUMN notice_access_after_end VARCHAR(32) DEFAULT 'public' NOT NULL"))
                if "scoreboard_freeze_mode" not in columns:
                    connection.execute(text("ALTER TABLE contests ADD COLUMN scoreboard_freeze_mode VARCHAR(32) DEFAULT 'auto' NOT NULL"))
                if "mock_judging_enabled" not in columns:
                    connection.execute(text("ALTER TABLE contests ADD COLUMN mock_judging_enabled BOOLEAN DEFAULT 0 NOT NULL"))
                if "participant_progress_visible" not in columns:
                    connection.execute(text("ALTER TABLE contests ADD COLUMN participant_progress_visible BOOLEAN DEFAULT 1 NOT NULL"))
                if "mock_judging_progress_visible" not in columns:
                    connection.execute(text("ALTER TABLE contests ADD COLUMN mock_judging_progress_visible BOOLEAN DEFAULT 0 NOT NULL"))
        if "mail_queue" in inspector.get_table_names():
            columns = {column["name"] for column in inspector.get_columns("mail_queue")}
            if "body_html" not in columns:
                with engine.begin() as connection:
                    connection.execute(text("ALTER TABLE mail_queue ADD COLUMN body_html TEXT"))
        if "judge_nodes" in inspector.get_table_names():
            columns = {column["name"] for column in inspector.get_columns("judge_nodes")}
            if "agent_version" not in columns:
                with engine.begin() as connection:
                    connection.execute(text("ALTER TABLE judge_nodes ADD COLUMN agent_version VARCHAR(64) DEFAULT 'unknown' NOT NULL"))
