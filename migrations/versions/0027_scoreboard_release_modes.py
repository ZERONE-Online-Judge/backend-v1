"""Configure automatic, manual rank, and submission resolver releases."""
from alembic import op
import sqlalchemy as sa

revision = "0027_scoreboard_release_modes"
down_revision = "0026_participant_preview"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    columns = {column["name"] for column in sa.inspect(bind).get_columns("contests")}
    if "scoreboard_release_mode" not in columns:
        op.add_column("contests", sa.Column("scoreboard_release_mode", sa.String(32), server_default="manual", nullable=False))
    if "scoreboard_frozen_at" not in columns:
        op.add_column("contests", sa.Column("scoreboard_frozen_at", sa.DateTime(timezone=True), nullable=True))
        # Earlier manual freezes had no click timestamp. Preserve the scheduled
        # freeze when it has passed; otherwise stop the rolling cutoff now.
        op.execute(sa.text("UPDATE contests SET scoreboard_frozen_at = CURRENT_TIMESTAMP WHERE scoreboard_freeze_mode = 'frozen'"))
    columns = {column["name"] for column in sa.inspect(bind).get_columns("scoreboard_releases")}
    if "strategy" not in columns:
        op.add_column("scoreboard_releases", sa.Column("strategy", sa.String(32), server_default="manual", nullable=False))
    if "resolver_state" not in columns:
        op.add_column("scoreboard_releases", sa.Column("resolver_state", sa.JSON(), nullable=True))


def downgrade() -> None:
    op.drop_column("scoreboard_releases", "resolver_state")
    op.drop_column("scoreboard_releases", "strategy")
    op.drop_column("contests", "scoreboard_frozen_at")
    op.drop_column("contests", "scoreboard_release_mode")
