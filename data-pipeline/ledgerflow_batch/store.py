"""PostgreSQL snapshot, reconciliation and atomic range replacement."""
from contextlib import contextmanager
from uuid import uuid4

LOCK_KEY = 0x4C4544474552

@contextmanager
def exclusive_batch(connection):
    locked = connection.execute("SELECT pg_try_advisory_lock(%s)", (LOCK_KEY,)).fetchone()[0]
    if not locked:
        raise RuntimeError("Another daily batch is already running; retry later")
    try:
        yield
    finally:
        connection.execute("SELECT pg_advisory_unlock(%s)", (LOCK_KEY,))


def start_run(connection, period):
    run_id = uuid4()
    connection.execute("""
        INSERT INTO analytics.batch_runs(run_id,job_name,start_date,end_date,status,started_at)
        VALUES (%s,'daily-operation-summary',%s,%s,'RUNNING',clock_timestamp())
        """, (run_id, period.start, period.end))
    return run_id


def capture_snapshot(connection, run_id, period):
    with connection.transaction():
        connection.execute("UPDATE analytics.batch_runs SET snapshot_at=clock_timestamp() WHERE run_id=%s", (run_id,))
        result = connection.execute("""
            INSERT INTO analytics.batch_source_snapshot(run_id,event_id,event_date,currency,operation_type,amount)
            SELECT %s,event_id,event_date,currency,operation_type,amount
            FROM analytics.transaction_facts WHERE event_date BETWEEN %s AND %s
            """, (run_id, period.start, period.end))
        count = result.rowcount
        connection.execute("UPDATE analytics.batch_runs SET source_count=%s WHERE run_id=%s", (count, run_id))
        return count


def publish_run(connection, run_id, period):
    # Each JDBC task can commit independently, so the public table is never a
    # Spark overwrite sink. Verify the stage, then publish in one SQL transaction.
    with connection.transaction():
        mismatch = connection.execute("""
            WITH expected AS (
                SELECT event_date,currency,operation_type,COUNT(*) AS operation_count,
                    SUM(amount)::numeric(38,2) AS total_amount,MIN(amount) AS min_amount,MAX(amount) AS max_amount
                FROM analytics.batch_source_snapshot WHERE run_id=%s
                GROUP BY event_date,currency,operation_type
            ), actual AS (
                SELECT DISTINCT event_date,currency,operation_type,operation_count,total_amount,min_amount,max_amount
                FROM analytics.daily_summary_stage WHERE run_id=%s
            )
            SELECT COUNT(*) FROM (
                (SELECT * FROM expected EXCEPT SELECT * FROM actual)
                UNION ALL
                (SELECT * FROM actual EXCEPT SELECT * FROM expected)
            ) differences
            """, (run_id, run_id)).fetchone()[0]
        if mismatch:
            raise ValueError("Staged aggregates do not reconcile with the source snapshot")
        connection.execute("DELETE FROM analytics.daily_operation_summary WHERE event_date BETWEEN %s AND %s", (period.start, period.end))
        inserted = connection.execute("""
            INSERT INTO analytics.daily_operation_summary(event_date,currency,operation_type,
                operation_count,total_amount,min_amount,max_amount,run_id,computed_at)
            SELECT DISTINCT event_date,currency,operation_type,operation_count,total_amount,min_amount,max_amount,
                run_id,CURRENT_TIMESTAMP FROM analytics.daily_summary_stage WHERE run_id=%s
            """, (run_id,)).rowcount
        connection.execute("""
            UPDATE analytics.batch_runs SET status='SUCCEEDED',finished_at=clock_timestamp(),group_count=%s
            WHERE run_id=%s
            """, (inserted, run_id))
        cleanup(connection, run_id)
        return inserted


def cleanup(connection, run_id):
    connection.execute("DELETE FROM analytics.daily_summary_stage WHERE run_id=%s", (run_id,))
    connection.execute("DELETE FROM analytics.batch_source_snapshot WHERE run_id=%s", (run_id,))


def fail_run(connection, run_id, error):
    with connection.transaction():
        connection.execute("""
            UPDATE analytics.batch_runs SET status='FAILED',finished_at=clock_timestamp(),error_type=%s
            WHERE run_id=%s
            """, (type(error).__name__[:200], run_id))
        cleanup(connection, run_id)
