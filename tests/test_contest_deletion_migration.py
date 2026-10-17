"""The deletion marker migration preserves pre-existing contest rows."""
import importlib.util
from pathlib import Path

from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine, inspect, text


def test_deletion_marker_upgrade_is_idempotent_and_downgrade_preserves_rows():
    path = Path(__file__).parents[1] / 'migrations/versions/0050_contest_deletion.py'
    spec = importlib.util.spec_from_file_location('contest_deletion_migration', path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    engine = create_engine('sqlite://')
    with engine.begin() as connection:
        connection.execute(text('CREATE TABLE contests (contest_id VARCHAR(36) PRIMARY KEY, title VARCHAR(255) NOT NULL)'))
        connection.execute(text("INSERT INTO contests VALUES ('retained', 'Original title')"))
        with Operations.context(MigrationContext.configure(connection)):
            migration.upgrade()
            migration.upgrade()
            column = next(c for c in inspect(connection).get_columns('contests') if c['name'] == 'deleted_at')
            assert column['nullable']
            assert connection.execute(text('SELECT title, deleted_at FROM contests')).one() == ('Original title', None)
            migration.downgrade()
            assert 'deleted_at' not in {c['name'] for c in inspect(connection).get_columns('contests')}
            assert connection.execute(text('SELECT title FROM contests')).scalar_one() == 'Original title'
            migration.upgrade()
    engine.dispose()
