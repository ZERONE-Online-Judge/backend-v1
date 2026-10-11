"""Index contest-based notification expiry for bounded cleanup batches."""
from alembic import op

revision = '0043_notification_expiry'
down_revision = '0042_notification_inbox'
branch_labels = None
depends_on = None


def upgrade():
    op.create_index('ix_notification_contest', 'notifications', ['contest_id', 'notification_id'], if_not_exists=True)


def downgrade():
    op.drop_index('ix_notification_contest', table_name='notifications')
