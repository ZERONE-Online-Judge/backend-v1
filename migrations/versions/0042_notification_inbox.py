"""Unify participant notifications without losing verdict-change history."""
from alembic import op
import sqlalchemy as sa

revision = '0042_notification_inbox'
down_revision = '0041_rejudging_notifications'
branch_labels = None
depends_on = None
OPTIONAL = {'rejudging_item_id': sa.String(36), 'submission_id': sa.String(36),
    'problem_id': sa.String(36), 'problem_code': sa.String(16),
    'old_status': sa.String(32), 'new_status': sa.String(32), 'reason': sa.Text()}
ADDED = {'event_key': sa.String(120), 'recipient_email': sa.String(255),
    'title': sa.String(512), 'body': sa.Text(), 'href': sa.Text(),
    'dismissed_at': sa.DateTime(timezone=True)}


def upgrade():
    op.rename_table('rejudging_notifications', 'notifications')
    with op.batch_alter_table('notifications') as batch:
        for name, datatype in OPTIONAL.items():
            batch.alter_column(name, existing_type=datatype, nullable=True)
        batch.add_column(sa.Column('notification_type', sa.String(32), nullable=False, server_default='rejudging'))
        for name, datatype in ADDED.items():
            batch.add_column(sa.Column(name, datatype, nullable=True))
        batch.create_unique_constraint('uq_notification_event_recipient', ['event_key', 'team_member_id'])
        batch.drop_index('ix_rejudge_notice_feed')
        batch.create_index('ix_notification_feed', ['team_member_id', 'deleted_at', 'created_at'])
    op.execute(sa.text("UPDATE notifications SET event_key = 'rejudging:' || rejudging_item_id, recipient_email = (SELECT lower(email) FROM team_members WHERE team_members.team_member_id = notifications.team_member_id)"))


def downgrade():
    # Older releases only understand verdict-change events. Keep those and their states.
    op.execute(sa.text("DELETE FROM notifications WHERE notification_type <> 'rejudging'"))
    with op.batch_alter_table('notifications') as batch:
        batch.drop_constraint('uq_notification_event_recipient', type_='unique')
        batch.drop_index('ix_notification_feed')
        batch.create_index('ix_rejudge_notice_feed', ['team_member_id', 'deleted_at', 'created_at'])
        for name in [*ADDED, 'notification_type']:
            batch.drop_column(name)
        for name, datatype in OPTIONAL.items():
            batch.alter_column(name, existing_type=datatype, nullable=False)
    op.rename_table('notifications', 'rejudging_notifications')
