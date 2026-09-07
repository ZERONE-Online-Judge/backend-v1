"""Persist independent final ranking releases for each contest division."""
from alembic import op
import sqlalchemy as sa

revision = "0023_scoreboard_releases"
down_revision = "0022_submission_owners"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Development startup may already have created the table via metadata.
    if "scoreboard_releases" in sa.inspect(op.get_bind()).get_table_names():
        return
    op.create_table(
        "scoreboard_releases",
        sa.Column("division_id", sa.String(36), sa.ForeignKey("contest_divisions.division_id"), primary_key=True),
        sa.Column("contest_id", sa.String(36), sa.ForeignKey("contests.contest_id"), nullable=False),
        sa.Column("mode", sa.String(16), nullable=False),
        sa.Column("snapshot_rows", sa.JSON(), nullable=False),
        sa.Column("revealed_ranks", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_scoreboard_releases_contest_id", "scoreboard_releases", ["contest_id"])


def downgrade() -> None:
    op.drop_table("scoreboard_releases")
