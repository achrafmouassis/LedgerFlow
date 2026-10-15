from datetime import date, datetime, timezone
from decimal import Decimal
from uuid import uuid4
import pytest
from ledgerflow_batch.config import DateRange
from ledgerflow_batch.job import run_batch
from ledgerflow_batch.store import (exclusive_batch,start_run,capture_snapshot,publish_run,fail_run)

DAY = date(2026,10,10)
PERIOD = DateRange(DAY,DAY)

def seed(settings, rows):
    with settings.connect() as connection:
        with connection.transaction():
            for day,currency,kind,amount in rows:
                event=uuid4()
                connection.execute("INSERT INTO analytics.processed_events VALUES (%s,%s,CURRENT_TIMESTAMP)",(event,"0"*64))
                debit=uuid4() if kind in ("WITHDRAWAL","TRANSFER") else None
                credit=uuid4() if kind in ("OPENING","DEPOSIT","TRANSFER") else None
                connection.execute("""
                    INSERT INTO analytics.transaction_facts(event_id,journal_id,operation_type,debit_account_id,
                        credit_account_id,amount,currency,occurred_at,event_date) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,(event,event,kind,debit,credit,Decimal(amount),currency,datetime.combine(day,datetime.min.time(),timezone.utc),day))

def sample(settings):
    seed(settings,[(DAY,"MAD","DEPOSIT","10.10"),(DAY,"MAD","DEPOSIT","20.20"),
        (DAY,"MAD","WITHDRAWAL","5"),(DAY,"MAD","TRANSFER","7"),
        (DAY,"EUR","DEPOSIT","8.50"),(DAY,"MAD","OPENING","100")])

def summaries(settings):
    with settings.connect() as connection:
        return connection.execute("""
            SELECT event_date,currency,operation_type,operation_count,total_amount,min_amount,max_amount
            FROM analytics.daily_operation_summary ORDER BY event_date,currency,operation_type
            """).fetchall()

def test_real_spark_jdbc_job_and_rerun_are_idempotent(db,spark):
    sample(db)
    first=run_batch(db,PERIOD,spark)
    before=summaries(db)
    second=run_batch(db,PERIOD,spark)
    assert first["run_id"] != second["run_id"]
    assert first["source_count"] == second["source_count"] == 6
    assert first["group_count"] == second["group_count"] == 5
    assert summaries(db) == before
    assert (DAY,"MAD","DEPOSIT",2,Decimal("30.30"),Decimal("10.10"),Decimal("20.20")) in before
    with db.connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM analytics.batch_runs WHERE status='SUCCEEDED'").fetchone()[0] == 2
        assert connection.execute("SELECT COUNT(*) FROM analytics.batch_source_snapshot").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM analytics.daily_summary_stage").fetchone()[0] == 0

def test_late_arrival_is_included_when_same_day_is_rebuilt(db,spark):
    seed(db,[(DAY,"MAD","DEPOSIT","10")])
    run_batch(db,PERIOD,spark)
    seed(db,[(DAY,"MAD","DEPOSIT","20")])
    run_batch(db,PERIOD,spark)
    assert summaries(db) == [(DAY,"MAD","DEPOSIT",2,Decimal("30"),Decimal("10"),Decimal("20"))]

def test_empty_day_clears_stale_groups_without_changing_other_dates(db,spark):
    other=date(2026,10,11)
    seed(db,[(DAY,"MAD","DEPOSIT","10"),(other,"MAD","DEPOSIT","20")])
    run_batch(db,DateRange(DAY,other),spark)
    with db.connect() as connection:
        connection.execute("DELETE FROM analytics.transaction_facts WHERE event_date=%s",(DAY,))
    result=run_batch(db,PERIOD,spark)
    assert result["source_count"] == result["group_count"] == 0
    assert summaries(db) == [(other,"MAD","DEPOSIT",1,Decimal("20"),Decimal("20"),Decimal("20"))]

def test_invalid_fact_fails_job_and_preserves_previous_publication(db,spark):
    seed(db,[(DAY,"MAD","DEPOSIT","10")])
    run_batch(db,PERIOD,spark)
    before=summaries(db)
    seed(db,[(DAY,"mad","DEPOSIT","1")])
    with pytest.raises(ValueError,match="Invalid analytical fact"):
        run_batch(db,PERIOD,spark)
    assert summaries(db) == before
    with db.connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM analytics.batch_runs WHERE status='FAILED' AND error_type='ValueError'").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM analytics.batch_source_snapshot").fetchone()[0] == 0

def stage(connection,run_id,count,total,minimum,maximum):
    connection.execute("""
        INSERT INTO analytics.daily_summary_stage(run_id,event_date,currency,operation_type,
            operation_count,total_amount,min_amount,max_amount) VALUES (%s,%s,'MAD','DEPOSIT',%s,%s,%s,%s)
        """,(run_id,DAY,count,Decimal(total),Decimal(minimum),Decimal(maximum)))

def test_reconciliation_failure_does_not_erase_existing_summary(db,spark):
    seed(db,[(DAY,"MAD","DEPOSIT","10")])
    run_batch(db,PERIOD,spark)
    before=summaries(db)
    with db.connect() as connection,exclusive_batch(connection):
        run_id=start_run(connection,PERIOD)
        capture_snapshot(connection,run_id,PERIOD)
        stage(connection,run_id,2,"20","10","10")
        with pytest.raises(ValueError,match="do not reconcile") as failure:
            publish_run(connection,run_id,PERIOD)
        fail_run(connection,run_id,failure.value)
    assert summaries(db) == before

def test_identical_jdbc_task_retry_rows_are_deduplicated_at_publication(db):
    seed(db,[(DAY,"MAD","DEPOSIT","10")])
    with db.connect() as connection,exclusive_batch(connection):
        run_id=start_run(connection,PERIOD)
        capture_snapshot(connection,run_id,PERIOD)
        stage(connection,run_id,1,"10","10","10")
        stage(connection,run_id,1,"10","10","10")
        assert publish_run(connection,run_id,PERIOD) == 1
    assert summaries(db) == [(DAY,"MAD","DEPOSIT",1,Decimal("10"),Decimal("10"),Decimal("10"))]

def test_conflicting_task_retry_rows_are_rejected(db):
    seed(db,[(DAY,"MAD","DEPOSIT","10")])
    with db.connect() as connection,exclusive_batch(connection):
        run_id=start_run(connection,PERIOD)
        capture_snapshot(connection,run_id,PERIOD)
        stage(connection,run_id,1,"10","10","10")
        stage(connection,run_id,2,"20","10","10")
        with pytest.raises(ValueError,match="do not reconcile"):
            publish_run(connection,run_id,PERIOD)
    assert summaries(db) == []

def test_snapshot_is_unchanged_by_later_streaming_ingestion(db):
    seed(db,[(DAY,"MAD","DEPOSIT","10")])
    with db.connect() as connection,exclusive_batch(connection):
        run_id=start_run(connection,PERIOD)
        assert capture_snapshot(connection,run_id,PERIOD) == 1
        seed(db,[(DAY,"MAD","DEPOSIT","20")])
        assert connection.execute("SELECT COUNT(*) FROM analytics.batch_source_snapshot WHERE run_id=%s",(run_id,)).fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM analytics.transaction_facts").fetchone()[0] == 2

def test_competing_batch_is_rejected_and_lock_is_released(db):
    with db.connect() as first,db.connect() as second:
        with exclusive_batch(first):
            with pytest.raises(RuntimeError,match="already running"):
                with exclusive_batch(second): pass
        with exclusive_batch(second): pass
