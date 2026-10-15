import os
from pathlib import Path
import pytest
from ledgerflow_batch.job import create_spark
from ledgerflow_batch.config import DatabaseSettings

@pytest.fixture(scope="session")
def spark():
    session = create_spark()
    yield session
    session.stop()

@pytest.fixture(scope="session")
def database():
    if os.environ.get("BATCH_INTEGRATION") != "1":
        pytest.skip("Set BATCH_INTEGRATION=1 for PostgreSQL tests")
    settings = DatabaseSettings.from_env()
    if not settings.database.endswith("_batch_test"):
        pytest.fail("Integration tests require a disposable database ending in _batch_test")
    with settings.connect() as connection:
        exists = connection.execute("SELECT to_regclass('analytics.batch_runs')").fetchone()[0]
        if not exists:
            migrations = Path(os.environ.get("MIGRATIONS_DIR", "/migrations"))
            for migration in sorted(migrations.glob("V*__*.sql")):
                connection.execute(migration.read_text())
            if not connection.execute("SELECT to_regclass('analytics.batch_runs')").fetchone()[0]:
                pytest.fail("Migration directory did not contain the required schema")
    return settings

@pytest.fixture
def db(database):
    with database.connect() as connection:
        connection.execute("""
            TRUNCATE analytics.daily_operation_summary,analytics.daily_summary_stage,
                analytics.batch_source_snapshot,analytics.batch_runs,
                analytics.transaction_facts,analytics.processed_events
            """)
    return database
