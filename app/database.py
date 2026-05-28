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
                if "submission_kind" not in columns:
                    connection.execute(text("ALTER TABLE submissions ADD COLUMN submission_kind VARCHAR(32) DEFAULT 'participant' NOT NULL"))
                if "submitted_by_name" not in columns:
                    connection.execute(text("ALTER TABLE submissions ADD COLUMN submitted_by_name VARCHAR(120)"))
                if "submitted_by_email" not in columns:
                    connection.execute(text("ALTER TABLE submissions ADD COLUMN submitted_by_email VARCHAR(255)"))
            submission_columns = {
                column["name"]: column for column in inspect(engine).get_columns("submissions")
            }
            if (
                "participant_team_id" in submission_columns
                and not submission_columns["participant_team_id"].get("nullable", True)
            ) or (
                "team_member_id" in submission_columns
                and not submission_columns["team_member_id"].get("nullable", True)
            ):
                with engine.begin() as connection:
                    connection.execute(text("PRAGMA foreign_keys=OFF"))
                    connection.execute(text("ALTER TABLE submissions RENAME TO submissions_old"))
                    connection.execute(
                        text(
                            """
                            CREATE TABLE submissions (
                                submission_id VARCHAR(36) NOT NULL PRIMARY KEY,
                                contest_id VARCHAR(36) NOT NULL,
                                division_id VARCHAR(36) NOT NULL,
                                problem_id VARCHAR(36) NOT NULL,
                                participant_team_id VARCHAR(36),
                                team_member_id VARCHAR(36),
                                submission_kind VARCHAR(32) DEFAULT 'participant' NOT NULL,
                                submitted_by_name VARCHAR(120),
                                submitted_by_email VARCHAR(255),
                                language VARCHAR(32) NOT NULL,
                                source_code TEXT NOT NULL,
                                status VARCHAR(32) NOT NULL,
                                submitted_at DATETIME,
                                status_updated_at DATETIME,
                                compile_message TEXT,
                                judge_message TEXT,
                                failed_testcase_order INTEGER,
                                progress_current INTEGER,
                                progress_total INTEGER,
                                runtime_ms INTEGER,
                                memory_kb INTEGER,
                                FOREIGN KEY(contest_id) REFERENCES contests (contest_id),
                                FOREIGN KEY(division_id) REFERENCES contest_divisions (division_id),
                                FOREIGN KEY(problem_id) REFERENCES problems (problem_id),
                                FOREIGN KEY(participant_team_id) REFERENCES participant_teams (participant_team_id),
                                FOREIGN KEY(team_member_id) REFERENCES team_members (team_member_id)
                            )
                            """
                        )
                    )
                    connection.execute(
                        text(
                            """
                            INSERT INTO submissions (
                                submission_id, contest_id, division_id, problem_id,
                                participant_team_id, team_member_id, submission_kind,
                                submitted_by_name, submitted_by_email, language,
                                source_code, status, submitted_at, status_updated_at,
                                compile_message, judge_message, failed_testcase_order,
                                progress_current, progress_total, runtime_ms, memory_kb
                            )
                            SELECT
                                submission_id, contest_id, division_id, problem_id,
                                participant_team_id, team_member_id, submission_kind,
                                submitted_by_name, submitted_by_email, language,
                                source_code, status, submitted_at, status_updated_at,
                                compile_message, judge_message, failed_testcase_order,
                                progress_current, progress_total, runtime_ms, memory_kb
                            FROM submissions_old
                            """
                        )
                    )
                    connection.execute(text("DROP TABLE submissions_old"))
                    connection.execute(text("CREATE INDEX IF NOT EXISTS ix_submissions_contest_id ON submissions (contest_id)"))
                    connection.execute(text("CREATE INDEX IF NOT EXISTS ix_submissions_division_id ON submissions (division_id)"))
                    connection.execute(text("CREATE INDEX IF NOT EXISTS ix_submissions_problem_id ON submissions (problem_id)"))
                    connection.execute(text("CREATE INDEX IF NOT EXISTS ix_submissions_participant_team_id ON submissions (participant_team_id)"))
                    connection.execute(text("CREATE INDEX IF NOT EXISTS ix_submissions_team_member_id ON submissions (team_member_id)"))
                    connection.execute(text("CREATE INDEX IF NOT EXISTS ix_submissions_status ON submissions (status)"))
                    connection.execute(text("CREATE INDEX IF NOT EXISTS ix_submissions_submission_kind ON submissions (submission_kind)"))
                    connection.execute(text("CREATE INDEX IF NOT EXISTS idx_submissions_submitted_id ON submissions (submitted_at DESC, submission_id DESC)"))
                    connection.execute(text("CREATE INDEX IF NOT EXISTS idx_submissions_contest_submitted_id ON submissions (contest_id, submitted_at DESC, submission_id DESC)"))
                    connection.execute(text("CREATE INDEX IF NOT EXISTS idx_submissions_contest_division_submitted_id ON submissions (contest_id, division_id, submitted_at DESC, submission_id DESC)"))
                    connection.execute(text("CREATE INDEX IF NOT EXISTS idx_submissions_contest_team_submitted_id ON submissions (contest_id, participant_team_id, submitted_at DESC, submission_id DESC)"))
                    connection.execute(text("CREATE INDEX IF NOT EXISTS idx_submissions_contest_problem_submitted_id ON submissions (contest_id, problem_id, submitted_at DESC, submission_id DESC)"))
                    connection.execute(text("PRAGMA foreign_keys=ON"))
        if "contests" in inspector.get_table_names():
            columns = {column["name"] for column in inspector.get_columns("contests")}
            with engine.begin() as connection:
                if "problem_access_after_end" not in columns:
                    connection.execute(text("ALTER TABLE contests ADD COLUMN problem_access_after_end VARCHAR(32) DEFAULT 'private' NOT NULL"))
                if "scoreboard_access_after_end" not in columns:
                    connection.execute(text("ALTER TABLE contests ADD COLUMN scoreboard_access_after_end VARCHAR(32) DEFAULT 'private' NOT NULL"))
                if "submission_access_after_end" not in columns:
                    connection.execute(text("ALTER TABLE contests ADD COLUMN submission_access_after_end VARCHAR(32) DEFAULT 'private' NOT NULL"))
                for legacy_column in ("problem_public_after_end", "scoreboard_public_after_end", "submission_public_after_end"):
                    if legacy_column in columns:
                        connection.execute(text(f"ALTER TABLE contests DROP COLUMN {legacy_column}"))
                if "board_access_after_end" not in columns:
                    connection.execute(text("ALTER TABLE contests ADD COLUMN board_access_after_end VARCHAR(32) DEFAULT 'participants' NOT NULL"))
                if "board_write_after_end" not in columns:
                    connection.execute(text("ALTER TABLE contests ADD COLUMN board_write_after_end BOOLEAN DEFAULT 0 NOT NULL"))
                if "notice_access_after_end" not in columns:
                    connection.execute(text("ALTER TABLE contests ADD COLUMN notice_access_after_end VARCHAR(32) DEFAULT 'public' NOT NULL"))
                if "editorial_access_after_end" not in columns:
                    connection.execute(text("ALTER TABLE contests ADD COLUMN editorial_access_after_end VARCHAR(32) DEFAULT 'private' NOT NULL"))
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
        if "team_members" in inspector.get_table_names():
            columns = {column["name"] for column in inspector.get_columns("team_members")}
            if "session_revoked_at" not in columns:
                with engine.begin() as connection:
                    connection.execute(text("ALTER TABLE team_members ADD COLUMN session_revoked_at DATETIME"))
        if "judge_nodes" in inspector.get_table_names():
            columns = {column["name"] for column in inspector.get_columns("judge_nodes")}
            if "agent_version" not in columns:
                with engine.begin() as connection:
                    connection.execute(text("ALTER TABLE judge_nodes ADD COLUMN agent_version VARCHAR(64) DEFAULT 'unknown' NOT NULL"))
        if "staff_accounts" in inspector.get_table_names():
            columns = {column["name"] for column in inspector.get_columns("staff_accounts")}
            if "password_hash" in columns:
                with engine.begin() as connection:
                    connection.execute(text("ALTER TABLE staff_accounts DROP COLUMN password_hash"))
        if "problems" in inspector.get_table_names():
            columns = {column["name"] for column in inspector.get_columns("problems")}
            if "language_resource_limits" not in columns:
                with engine.begin() as connection:
                    connection.execute(text("ALTER TABLE problems ADD COLUMN language_resource_limits JSON DEFAULT '{}' NOT NULL"))
            if "editorial" not in columns:
                with engine.begin() as connection:
                    connection.execute(text("ALTER TABLE problems ADD COLUMN editorial TEXT DEFAULT '' NOT NULL"))
            if "max_score" in columns:
                with engine.begin() as connection:
                    connection.execute(text("ALTER TABLE problems DROP COLUMN max_score"))
        if "submissions" in inspector.get_table_names():
            columns = {column["name"] for column in inspector.get_columns("submissions")}
            if "awarded_score" in columns:
                with engine.begin() as connection:
                    connection.execute(text("ALTER TABLE submissions DROP COLUMN awarded_score"))
