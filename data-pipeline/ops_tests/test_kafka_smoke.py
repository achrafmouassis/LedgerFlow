"""Opt-in real backend/Kafka smoke test against a separate disposable database."""
import json
import os
import time
from uuid import uuid4
from urllib.request import Request, urlopen
import pytest
from ledgerflow_batch.config import DatabaseSettings
from ledgerflow_ops.dead_letters import capture, replay, resolve_replays
from ledgerflow_ops.monitor import consumer_lag


def wait_until(check, seconds=45):
    deadline=time.monotonic()+seconds
    while time.monotonic()<deadline:
        result=check()
        if result:
            return result
        time.sleep(0.2)
    raise AssertionError("Timed out waiting for the isolated pipeline")


def test_real_dlt_replay_does_not_duplicate_banking_facts(tmp_path):
    if os.environ.get("OPS_SMOKE") != "1":
        pytest.skip("Requires an isolated backend, database and Kafka topic")
    from confluent_kafka import Producer, Consumer
    settings=DatabaseSettings.from_env()
    if not settings.database.endswith("_smoke_test"):
        pytest.fail("Smoke test requires a disposable _smoke_test database")
    topic=os.environ["SMOKE_TOPIC"]
    if not topic.startswith("ledgerflow.smoke."):
        pytest.fail("Smoke topic must be isolated from the main pipeline")
    api=os.environ.get("SMOKE_API", "http://localhost:18080")
    def post(path,body,key=None):
        headers={"Content-Type":"application/json"}
        if key: headers["Idempotency-Key"]=key
        with urlopen(Request(api+path,data=json.dumps(body).encode(),headers=headers),timeout=10) as response:
            return json.load(response)
    account=post("/api/accounts",{"owner":"Operations smoke test","currency":"EUR","openingBalance":"0.00"})
    deposit=post("/api/accounts/"+account["id"]+"/deposits",{"amount":"10.00"},str(uuid4()))
    bootstrap=os.environ.get("KAFKA_BOOTSTRAP_SERVERS","localhost:9092")
    producer=Producer({"bootstrap.servers":bootstrap,"enable.idempotence":True})
    consumer=Consumer({"bootstrap.servers":bootstrap,"group.id":"smoke-dlt-"+str(uuid4()),
        "auto.offset.reset":"earliest","enable.auto.commit":False})
    probe=Consumer({"bootstrap.servers":bootstrap,"group.id":"ledgerflow-smoke-analytics","enable.auto.commit":False})
    consumer.subscribe([topic+".DLT"])
    try:
        with settings.connect() as c:
            wait_until(lambda:c.execute("SELECT COUNT(*) FROM analytics.transaction_facts WHERE event_id=%s",(deposit["id"],)).fetchone()[0]==1)
            event={"schemaVersion":1,"eventId":deposit["id"],"journalId":deposit["id"],
                "operationType":"DEPOSIT","debitAccountId":None,"creditAccountId":account["id"],
                "amount":"10.00","currency":"EUR","occurredAt":deposit["createdAt"]}
            bad={**event,"schemaVersion":999}
            producer.produce(topic,key=deposit["id"].encode(),value=json.dumps(bad).encode())
            assert producer.flush(10)==0
            def rejected():
                message=consumer.poll(0.5)
                if message and not message.error() and json.loads(message.value()).get("eventId")==deposit["id"]:
                    return message
                return None
            message=wait_until(rejected)
            capture(c,message)
            letter=c.execute("SELECT id FROM analytics.dead_letters WHERE topic=%s AND partition_id=%s AND offset_id=%s",
                (message.topic(),message.partition(),message.offset())).fetchone()[0]
            corrected=tmp_path/"corrected.json"
            corrected.write_text(json.dumps(event))
            replay(c,producer,letter,topic,corrected)
            wait_until(lambda:consumer_lag(probe,topic,"ledgerflow-smoke-analytics")==0)
            resolve_replays(c)
            assert c.execute("SELECT status FROM analytics.dead_letters WHERE id=%s",(letter,)).fetchone()[0]=="RESOLVED"
            assert c.execute("SELECT COUNT(*) FROM analytics.transaction_facts WHERE event_id=%s",(deposit["id"],)).fetchone()[0]==1
    finally:
        consumer.close()
        probe.close()
