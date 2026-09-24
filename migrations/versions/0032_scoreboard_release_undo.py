"""Persist reversible scoreboard release actions and their revision."""
from alembic import op
import sqlalchemy as sa

revision = "0032_scoreboard_release_undo"
down_revision = "0031_contest_visibility"
branch_labels = None
depends_on = None


def upgrade() -> None:
    columns = {column["name"] for column in sa.inspect(op.get_bind()).get_columns("scoreboard_releases")}
    if "undo_history" not in columns:
        # NULL distinguishes older releases without an action history.
        op.add_column("scoreboard_releases", sa.Column("undo_history", sa.JSON(), nullable=True))
    if "revision" not in columns:
        op.add_column("scoreboard_releases", sa.Column("revision", sa.Integer(), nullable=False, server_default="0"))


def downgrade() -> None:
    op.drop_column("scoreboard_releases", "revision")
    op.drop_column("scoreboard_releases", "undo_history")
