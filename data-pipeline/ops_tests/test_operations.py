from datetime import date
import json
from uuid import uuid4
import pytest
from ledgerflow_batch.config import DateRange
from ledgerflow_batch.store import exclusive_batch, start_run
from ledgerflow_ops.batch_server import parse_period
from ledgerflow_ops.dead_letters import capture, replay, resolve_replays
from ledgerflow_ops.notifications import update_alert
from ledgerflow_ops.recovery import recover_runs, ingestion_ready
from ledgerflow_ops.monitor import collect


@pytest.mark.parametrize("body", [{}, {"start":"bad","end":"2026-01-01"},
    {"start":"2026-01-02","end":"2026-01-01"}, {"start":"2020-01-01","end":"2026-01-01"},
    {"start":"2026-01-01","end":"2026-01-01","command":"anything"}])
def test_worker_rejects_invalid_or_extra_input(body):
    with pytest.raises((ValueError, TypeError)):
        parse_period(body)


def test_worker_accepts_explicit_utc_range():
    assert parse_period({"start":"2026-01-01","end":"2026-01-03"}) == DateRange(date(2026,1,1),date(2026,1,3))


def test_recovery_does_not_touch_a_live_batch_even_if_old(db):
    with db.connect() as owner, db.connect() as other:
        with exclusive_batch(owner):
            run = start_run(owner, DateRange(date(2026,1,1),date(2026,1,1)))
            owner.execute("UPDATE analytics.batch_runs SET started_at=clock_timestamp()-INTERVAL '3 hours' WHERE run_id=%s",(run,))
            assert recover_runs(other) == 0
            assert owner.execute("SELECT status FROM analytics.batch_runs WHERE run_id=%s",(run,)).fetchone()[0] == "RUNNING"
        assert recover_runs(other) == 1
        assert owner.execute("SELECT status,error_type FROM analytics.batch_runs WHERE run_id=%s",(run,)).fetchone() == ("FAILED","AbandonedRun")
        assert recover_runs(other) == 0


def test_recovery_cleans_intermediates_but_preserves_published_results(db):
    with db.connect() as c:
        day=date(2026,1,1)
        old=start_run(c,DateRange(day,day))
        c.execute("UPDATE analytics.batch_runs SET status='SUCCEEDED' WHERE run_id=%s",(old,))
        c.execute("""INSERT INTO analytics.daily_operation_summary VALUES
            (%s,'EUR','DEPOSIT',1,10,10,10,%s,clock_timestamp())""",(day,old))
        run=start_run(c,DateRange(day,day))
        c.execute("UPDATE analytics.batch_runs SET started_at=clock_timestamp()-INTERVAL '3 hours' WHERE run_id=%s",(run,))
        c.execute("INSERT INTO analytics.batch_source_snapshot VALUES (%s,%s,%s,'EUR','DEPOSIT',10)",(run,uuid4(),day))
        c.execute("INSERT INTO analytics.daily_summary_stage VALUES (%s,%s,'EUR','DEPOSIT',1,10,10,10)",(run,day))
        assert recover_runs(c) == 1
        assert c.execute("SELECT COUNT(*) FROM analytics.daily_operation_summary").fetchone()[0] == 1
        assert c.execute("SELECT COUNT(*) FROM analytics.batch_source_snapshot").fetchone()[0] == 0
        assert c.execute("SELECT COUNT(*) FROM analytics.daily_summary_stage").fetchone()[0] == 0
        assert collect(c,0)["failed"] == 1


def test_notifications_only_on_transitions_and_retry_failed_delivery(db):
    sent=[]
    sender=lambda *args: sent.append(args)
    with db.connect() as c:
        update_alert(c,"test",False,"Healthy",sender)
        assert sent == []
        update_alert(c,"test",True,"Backlog",sender)
        update_alert(c,"test",True,"Still backlog",sender)
        assert len(sent) == 1
        update_alert(c,"test",False,"Recovered",sender)
        assert len(sent) == 2
        def failing(*args):
            raise OSError("SMTP unavailable")
        with pytest.raises(OSError):
            update_alert(c,"test",True,"Backlog",failing)
        assert c.execute("SELECT notified_at FROM analytics.alerts WHERE alert_key='test'").fetchone()[0] is None
        update_alert(c,"test",True,"Backlog",sender)
        assert len(sent) == 3


class Message:
    def topic(self): return "ledgerflow.operations.v1.DLT"
    def partition(self): return 0
    def offset(self): return 42
    def key(self): return b"key"
    def value(self): return b"invalid original message"


class Producer:
    def __init__(self,error=None): self.error=error; self.sent=[]
    def produce(self,topic,**kwargs):
        self.sent.append((topic,kwargs))
        kwargs["on_delivery"](self.error,None)
    def flush(self,timeout): return 0


def test_dlt_capture_is_idempotent_and_replay_waits_for_kafka_ack(db,tmp_path):
    event=uuid4(); customer=uuid4()
    replacement=tmp_path/"corrected.json"
    replacement.write_text(json.dumps({"schemaVersion":1,"eventId":str(event),"journalId":str(event),
        "operationType":"DEPOSIT","debitAccountId":None,"creditAccountId":str(customer),
        "currency":"EUR","amount":"10.00","occurredAt":"2026-01-01T12:00:00Z"}))
    with db.connect() as c:
        capture(c,Message()); capture(c,Message())
        assert c.execute("SELECT COUNT(*) FROM analytics.dead_letters").fetchone()[0] == 1
        letter=c.execute("SELECT id FROM analytics.dead_letters").fetchone()[0]
        with pytest.raises(RuntimeError):
            replay(c,Producer(RuntimeError()),letter,"ledgerflow.operations.v1",replacement)
        assert c.execute("SELECT status FROM analytics.dead_letters").fetchone()[0] == "OPEN"
        replay(c,Producer(),letter,"ledgerflow.operations.v1",replacement)
        resolve_replays(c)
        assert c.execute("SELECT status,payload FROM analytics.dead_letters").fetchone() == ("REPLAYED",Message().value())
        c.execute("INSERT INTO analytics.processed_events VALUES (%s,%s,clock_timestamp())",(event,"0"*64))
        c.execute("""INSERT INTO analytics.transaction_facts VALUES
            (%s,%s,'DEPOSIT',NULL,%s,11,'EUR','2026-01-01T12:00:00Z','2026-01-01')""",(event,event,customer))
        resolve_replays(c)
        assert c.execute("SELECT status FROM analytics.dead_letters").fetchone()[0] == "REPLAYED"
        c.execute("UPDATE analytics.transaction_facts SET amount=10 WHERE event_id=%s",(event,))
        resolve_replays(c)
        assert c.execute("SELECT status FROM analytics.dead_letters").fetchone()[0] == "RESOLVED"


def test_empty_period_is_ready(db):
    with db.connect() as c:
        assert ingestion_ready(c,DateRange(date(2026,1,1),date(2026,1,1)))


def test_published_event_without_committed_receipt_is_not_ready(db):
    with db.connect() as c:
        account=uuid4(); event=uuid4(); day=date(2026,1,1)
        c.execute("INSERT INTO accounts VALUES (%s,'Test','EUR',10)",(account,))
        c.execute("""INSERT INTO journals(id,idempotency_key,type,credit_account_id,amount,currency,created_at)
            VALUES (%s,%s,'DEPOSIT',%s,10,'EUR','2026-01-01T12:00:00Z')""",(event,str(event),account))
        c.execute("""INSERT INTO outbox_events(event_id,journal_id,schema_version,operation_type,
            credit_account_id,amount,currency,occurred_at,created_at,published_at,next_attempt_at)
            VALUES (%s,%s,1,'DEPOSIT',%s,10,'EUR','2026-01-01T12:00:00Z',clock_timestamp(),
                clock_timestamp(),clock_timestamp())""",(event,event,account))
        assert not ingestion_ready(c,DateRange(day,day))
        assert collect(c,-1)["unprocessed"] == 1
        c.execute("INSERT INTO analytics.processed_events VALUES (%s,%s,clock_timestamp())",(event,"0"*64))
        assert ingestion_ready(c,DateRange(day,day))
        assert collect(c,0)["unprocessed"] == 0
