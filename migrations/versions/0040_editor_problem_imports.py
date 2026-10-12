"""Record signed Editor requests and atomic import receipts."""
from alembic import op
import sqlalchemy as sa
revision = "0040_editor_problem_imports"
down_revision = "0039_restore_verification_runs"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table("editor_bridge_nonces", sa.Column("nonce", sa.String(32), primary_key=True), sa.Column("expires_at", sa.Integer(), nullable=False))
    op.create_index("ix_editor_bridge_nonces_expires_at", "editor_bridge_nonces", ["expires_at"])
    op.create_table("editor_problem_imports", sa.Column("operation_key", sa.String(64), primary_key=True),
        sa.Column("request_digest", sa.String(64), nullable=False), sa.Column("email", sa.String(255), nullable=False),
        sa.Column("problem_id", sa.String(36), sa.ForeignKey("problems.problem_id", ondelete="CASCADE"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False))


def downgrade():
    op.drop_table("editor_problem_imports")
    op.drop_table("editor_bridge_nonces")
