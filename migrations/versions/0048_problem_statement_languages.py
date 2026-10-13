"""Keep alternate statements and protected editorials with each problem."""
from alembic import op
import sqlalchemy as sa

revision = "0048_problem_statement_languages"
down_revision = "0047_editor_judge_runs"
branch_labels = None
depends_on = None


def upgrade():
    columns = {c["name"] for c in sa.inspect(op.get_bind()).get_columns("problems")}
    for column in [
        sa.Column("default_statement_language", sa.String(100), nullable=False, server_default="ko"),
        sa.Column("statement_translations", sa.JSON(), nullable=False, server_default="{}"),
        sa.Column("editorial_translations", sa.JSON(), nullable=False, server_default="{}"),
    ]:
        if column.name not in columns:
            op.add_column("problems", column)


def downgrade():
    with op.batch_alter_table("problems") as batch:
        batch.drop_column("editorial_translations")
        batch.drop_column("statement_translations")
        batch.drop_column("default_statement_language")
