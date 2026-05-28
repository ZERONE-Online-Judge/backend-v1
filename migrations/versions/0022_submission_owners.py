"""separate non participant submission owners

Revision ID: 0022_submission_owners
Revises: 0021_editorials
Create Date: 2026-05-29 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "0022_submission_owners"
down_revision = "0021_editorials"
branch_labels = None
depends_on = None


def _has_column(table_name: str, column_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if table_name not in inspector.get_table_names():
        return False
    return column_name in {column["name"] for column in inspector.get_columns(table_name)}


def _has_index(table_name: str, index_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if table_name not in inspector.get_table_names():
        return False
    return index_name in {index["name"] for index in inspector.get_indexes(table_name)}


def upgrade() -> None:
    with op.batch_alter_table("submissions") as batch_op:
        if not _has_column("submissions", "submission_kind"):
            batch_op.add_column(
                sa.Column(
                    "submission_kind",
                    sa.String(length=32),
                    nullable=False,
                    server_default="participant",
                )
            )
        if not _has_column("submissions", "submitted_by_name"):
            batch_op.add_column(sa.Column("submitted_by_name", sa.String(length=120), nullable=True))
        if not _has_column("submissions", "submitted_by_email"):
            batch_op.add_column(sa.Column("submitted_by_email", sa.String(length=255), nullable=True))
        batch_op.alter_column("participant_team_id", existing_type=sa.String(length=36), nullable=True)
        batch_op.alter_column("team_member_id", existing_type=sa.String(length=36), nullable=True)

    if not _has_index("submissions", "ix_submissions_submission_kind"):
        op.create_index("ix_submissions_submission_kind", "submissions", ["submission_kind"], unique=False)

    bind = op.get_bind()
    operator_team_ids = [
        row[0]
        for row in bind.execute(
            sa.text(
                "select participant_team_id from participant_teams "
                "where team_name like :pattern"
            ),
            {"pattern": "__operator_test__:%"},
        )
    ]
    for team_id in operator_team_ids:
        bind.execute(
            sa.text(
                "update submissions "
                "set submission_kind = 'operator_test', "
                "submitted_by_name = '운영자', "
                "participant_team_id = null, "
                "team_member_id = null "
                "where participant_team_id = :team_id"
            ),
            {"team_id": team_id},
        )
        bind.execute(
            sa.text("delete from team_sessions where participant_team_id = :team_id"),
            {"team_id": team_id},
        )
        bind.execute(
            sa.text("delete from team_members where participant_team_id = :team_id"),
            {"team_id": team_id},
        )
        bind.execute(
            sa.text("delete from participant_teams where participant_team_id = :team_id"),
            {"team_id": team_id},
        )


def downgrade() -> None:
    bind = op.get_bind()
    bind.execute(sa.text("delete from submissions where participant_team_id is null"))
    if _has_index("submissions", "ix_submissions_submission_kind"):
        op.drop_index("ix_submissions_submission_kind", table_name="submissions")
    with op.batch_alter_table("submissions") as batch_op:
        batch_op.alter_column("team_member_id", existing_type=sa.String(length=36), nullable=False)
        batch_op.alter_column("participant_team_id", existing_type=sa.String(length=36), nullable=False)
        if _has_column("submissions", "submitted_by_email"):
            batch_op.drop_column("submitted_by_email")
        if _has_column("submissions", "submitted_by_name"):
            batch_op.drop_column("submitted_by_name")
        if _has_column("submissions", "submission_kind"):
            batch_op.drop_column("submission_kind")
