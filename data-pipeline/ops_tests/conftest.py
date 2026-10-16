import os
from pathlib import Path
import pytest
from ledgerflow_batch.config import DatabaseSettings


@pytest.fixture
def db():
    if os.environ.get("OPS_INTEGRATION") != "1":
        pytest.skip("Set OPS_INTEGRATION=1 for disposable PostgreSQL tests")
    settings = DatabaseSettings.from_env()
    if not settings.database.endswith("_ops_test"):
        pytest.fail("Operations tests require a disposable database ending in _ops_test")
    with settings.connect() as connection:
        migrations = Path(os.environ.get("MIGRATIONS_DIR", "ledgerflow-backend/src/main/resources/db/migration"))
        if not connection.execute("SELECT to_regclass('analytics.alerts')").fetchone()[0]:
            for migration in sorted(migrations.glob("V*__*.sql")):
                connection.execute(migration.read_text(encoding="utf-8"))
        if not connection.execute("SELECT to_regclass('analytics.bi_daily_operations')").fetchone()[0]:
            connection.execute((migrations/"V6__power_bi_views.sql").read_text(encoding="utf-8"))
        connection.execute("""TRUNCATE analytics.daily_operation_summary,analytics.daily_summary_stage,
            analytics.batch_source_snapshot,analytics.batch_runs,analytics.dead_letters,
            analytics.monitor_samples,analytics.alerts,analytics.transaction_facts,
            analytics.processed_events,outbox_events,journals,transfers,accounts CASCADE""")
    return settings
