from datetime import date
from decimal import Decimal
from uuid import uuid4
import pytest
from ledgerflow_batch.store import start_run
from ledgerflow_batch.config import DateRange
from ledgerflow_ops.bi import initialize_bi


def test_bi_reader_only_sees_published_aggregates_and_run_history(db):
    day=date(2026,1,1)
    with db.connect() as c:
        initialize_bi(c,"test-only-bi-password")
        run=start_run(c,DateRange(day,day))
        c.execute("UPDATE analytics.batch_runs SET status='SUCCEEDED' WHERE run_id=%s",(run,))
        for currency,total in (("EUR","150.00"),("MAD","200.00")):
            c.execute("""INSERT INTO analytics.daily_operation_summary VALUES
                (%s,%s,'TRANSFER',2,%s,50,100,%s,clock_timestamp())""",(day,currency,Decimal(total),run))
        with c.transaction():
            c.execute("SET LOCAL ROLE ledgerflow_bi")
            rows=c.execute("SELECT currency,operation_label,operation_count,total_amount,total_amount_exact FROM analytics.bi_daily_operations ORDER BY currency").fetchall()
            assert rows==[("EUR","Virement",2,Decimal("150"),"150.00"),("MAD","Virement",2,Decimal("200"),"200.00")]
            assert c.execute("SELECT COUNT(*) FROM analytics.bi_batch_runs").fetchone()[0]==1
        with pytest.raises(Exception) as error:
            with c.transaction():
                c.execute("SET LOCAL ROLE ledgerflow_bi")
                c.execute("SELECT * FROM accounts")
        assert error.value.sqlstate=="42501"
        with pytest.raises(Exception) as error:
            with c.transaction():
                c.execute("SET LOCAL ROLE ledgerflow_bi")
                c.execute("INSERT INTO analytics.daily_operation_summary(event_date) VALUES ('2026-01-02')")
        assert error.value.sqlstate=="42501"
