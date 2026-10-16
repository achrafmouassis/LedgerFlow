"""Recover abandoned runs only while owning the exact same lock as the batch."""
from datetime import timedelta
from ledgerflow_batch.store import exclusive_batch, cleanup


def recover_runs(connection, older_than_seconds=7200):
    if older_than_seconds < 60:
        raise ValueError("Recovery age must be at least 60 seconds")
    try:
        with exclusive_batch(connection):
            with connection.transaction():
                rows = connection.execute("""
                    UPDATE analytics.batch_runs
                    SET status='FAILED',error_type='AbandonedRun',finished_at=clock_timestamp()
                    WHERE status='RUNNING' AND started_at < clock_timestamp() - %s
                    RETURNING run_id
                    """, (timedelta(seconds=older_than_seconds),)).fetchall()
                for (run_id,) in rows:
                    cleanup(connection, run_id)
                return len(rows)
    except RuntimeError as error:
        if str(error).startswith("Another daily batch"):
            return 0
        raise


def ingestion_ready(connection, period):
    # Published is not sufficient: require a committed analytical receipt for
    # every bank event in the requested UTC dates, including pending Outbox rows.
    missing = connection.execute("""
        SELECT COUNT(*) FROM outbox_events o
        LEFT JOIN analytics.processed_events p ON p.event_id=o.event_id
        WHERE (o.occurred_at AT TIME ZONE 'UTC')::date BETWEEN %s AND %s
          AND p.event_id IS NULL
        """, (period.start, period.end)).fetchone()[0]
    return missing == 0
