"""Record contest scope and actual email sending times."""
from alembic import op
import sqlalchemy as sa

from app.services.mail_log_backfill import backfill_mail_contests

revision = "0033_mail_delivery_logs"
down_revision = "0032_scoreboard_release_undo"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    columns = {column["name"] for column in sa.inspect(bind).get_columns("mail_queue")}
    for name, kind in (("contest_id", sa.String(36)), ("last_attempt_at", sa.DateTime(timezone=True)), ("sent_at", sa.DateTime(timezone=True))):
        if name not in columns:
            op.add_column("mail_queue", sa.Column(name, kind, nullable=True))
    indexes = {index["name"] for index in sa.inspect(bind).get_indexes("mail_queue")}
    for name, columns in (("idx_mail_queue_created", ["created_at", "mail_queue_id"]),
                          ("idx_mail_queue_contest_created", ["contest_id", "created_at", "mail_queue_id"])):
        if name not in indexes:
            op.create_index(name, "mail_queue", columns)
    backfill_mail_contests(bind)


def downgrade() -> None:
    op.drop_index("idx_mail_queue_contest_created", table_name="mail_queue")
    op.drop_index("idx_mail_queue_created", table_name="mail_queue")
    for name in ("sent_at", "last_attempt_at", "contest_id"):
        op.drop_column("mail_queue", name)
