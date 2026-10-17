"""Persist per-problem submission languages."""
from alembic import op
import sqlalchemy as sa

revision = "0051_allowed_languages"
down_revision = "0050_contest_deletion"
branch_labels = depends_on = None


def upgrade():
    if "source_language" not in {c["name"] for c in sa.inspect(op.get_bind()).get_columns("problem_assets")}:
        op.add_column("problem_assets", sa.Column("source_language", sa.String(32), nullable=True))
    if "allowed_languages" not in {c["name"] for c in sa.inspect(op.get_bind()).get_columns("problems")}:
        op.add_column("problems", sa.Column("allowed_languages", sa.JSON(), nullable=False,
            server_default='["cpp17","python313","java8","c99","pypy3","kotlin"]'))


def downgrade():
    op.drop_column("problem_assets", "source_language")
    op.drop_column("problems", "allowed_languages")
