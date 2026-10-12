"""Share authentication quotas across processes."""
from alembic import op
import sqlalchemy as sa
revision = '0045_auth_rate_limits'
down_revision = '0044_editor_problem_imports'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('auth_rate_limits', sa.Column('key',sa.String(100),primary_key=True), sa.Column('started_at',sa.Integer(),nullable=False), sa.Column('count',sa.Integer(),nullable=False))
    op.create_index('ix_auth_rate_limits_started_at','auth_rate_limits',['started_at'])


def downgrade():
    op.drop_table('auth_rate_limits')
