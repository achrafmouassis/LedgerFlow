"""Power BI credentials are kept outside the versioned report."""
from psycopg import sql


def initialize_bi(connection, password):
    if len(password) < 16:
        raise ValueError("BI_DB_PASSWORD must contain at least 16 characters")
    with connection.transaction():
        if not connection.execute("SELECT 1 FROM pg_roles WHERE rolname='ledgerflow_bi'").fetchone():
            connection.execute("CREATE ROLE ledgerflow_bi LOGIN")
        connection.execute(sql.SQL("ALTER ROLE ledgerflow_bi PASSWORD {}").format(sql.Literal(password)))
        connection.execute("ALTER ROLE ledgerflow_bi SET default_transaction_read_only=on")
        connection.execute("GRANT USAGE ON SCHEMA analytics TO ledgerflow_bi")
        connection.execute("GRANT SELECT ON analytics.bi_daily_operations,analytics.bi_batch_runs TO ledgerflow_bi")
