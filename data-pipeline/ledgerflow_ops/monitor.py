"""DLT inbox, real consumer lag, thresholds, and safe abandoned-run recovery."""
import json
import os
import time
from datetime import datetime, timezone
from ledgerflow_batch.config import DatabaseSettings
from .dead_letters import capture, resolve_replays
from .notifications import notify, update_alert
from .recovery import recover_runs


def kafka_settings():
    return {"bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092"),
            "socket.timeout.ms": 5000}


def consumer_lag(probe, topic, group):
    from confluent_kafka import TopicPartition
    metadata = probe.list_topics(topic, timeout=5)
    entry = metadata.topics.get(topic)
    if entry is None or entry.error:
        raise RuntimeError("Kafka topic unavailable")
    partitions = [TopicPartition(topic, part) for part in entry.partitions]
    committed = probe.committed(partitions, timeout=5)
    lag = 0
    for partition in committed:
        if partition.error:
            raise RuntimeError("Unable to read consumer offset")
        low, high = probe.get_watermark_offsets(partition, timeout=5, cached=False)
        lag += max(0, high - max(low, partition.offset))
    return lag


def collect(connection, lag):
    pending, age = connection.execute("""
        SELECT COUNT(*),COALESCE(EXTRACT(EPOCH FROM clock_timestamp()-MIN(created_at)),0)
        FROM outbox_events WHERE published_at IS NULL
        """
    ).fetchone()
    unprocessed = connection.execute("""
        SELECT COUNT(*) FROM outbox_events o LEFT JOIN analytics.processed_events p
        ON p.event_id=o.event_id WHERE o.published_at IS NOT NULL AND p.event_id IS NULL
        """
    ).fetchone()[0]
    dead = connection.execute("SELECT COUNT(*) FROM analytics.dead_letters WHERE status<>'RESOLVED'").fetchone()[0]
    failed, running = connection.execute("""
        SELECT COUNT(*) FILTER (WHERE status='FAILED' AND COALESCE(finished_at,started_at)>clock_timestamp()-INTERVAL '24 hours'),
            COUNT(*) FILTER (WHERE status='RUNNING') FROM analytics.batch_runs
        """
    ).fetchone()
    connection.execute("""
        INSERT INTO analytics.monitor_samples VALUES (clock_timestamp(),%s,%s,%s,%s,%s,%s,%s)
        """, (pending, float(age), unprocessed, lag, dead, failed, running))
    # Keep monitoring bounded without touching banking facts or published summaries.
    connection.execute("DELETE FROM analytics.monitor_samples WHERE sampled_at<clock_timestamp()-INTERVAL '30 days'")
    return {"pending": pending, "age": float(age), "unprocessed": unprocessed,
            "lag": lag, "dead": dead, "failed": failed, "running": running}


def check_alerts(connection, sample):
    conditions = [
        ("outbox_backlog", sample["pending"] >= int(os.environ.get("ALERT_PENDING_COUNT", "100"))
            or sample["age"] >= int(os.environ.get("ALERT_PENDING_SECONDS", "120")), "Messages bancaires en attente de publication"),
        ("consumer_backlog", sample["unprocessed"] >= int(os.environ.get("ALERT_UNPROCESSED_COUNT", "100")), "Evenements publies non encore integres"),
        ("kafka_lag", sample["lag"] >= int(os.environ.get("ALERT_KAFKA_LAG", "100")), "Retard de lecture Kafka"),
        ("kafka_unavailable", sample["lag"] < 0, "Kafka ne repond pas au controle"),
        ("dead_letters", sample["dead"] > 0, "Messages rejetes a examiner"),
        ("batch_failed", sample["failed"] > 0, "Au moins un batch a echoue dans les dernieres 24 heures"),
    ]
    # Yesterday's data must have a successful publication after the daily deadline.
    now = datetime.now(timezone.utc)
    if now.hour >= int(os.environ.get("BATCH_DEADLINE_UTC_HOUR", "4")):
        missing = not connection.execute("""
            SELECT EXISTS(SELECT 1 FROM analytics.batch_runs WHERE status='SUCCEEDED'
                AND start_date <= (clock_timestamp() AT TIME ZONE 'UTC')::date-1
                AND end_date >= (clock_timestamp() AT TIME ZONE 'UTC')::date-1)
            """
        ).fetchone()[0]
        conditions.append(("daily_batch_missing", missing, "Synthese de la veille non publiee apres l'heure limite UTC"))
    for key, active, message in conditions:
        try:
            update_alert(connection, key, active, message)
        except Exception as error:
            print(json.dumps({"notification_error": type(error).__name__, "alert": key}), flush=True)


def run_monitor():
    from confluent_kafka import Consumer
    topic = os.environ.get("KAFKA_TOPIC", "ledgerflow.operations.v1")
    group = os.environ.get("KAFKA_ANALYTICS_GROUP", "ledgerflow-analytics-v1")
    dlt = Consumer({**kafka_settings(), "group.id": "ledgerflow-dlt-archive-v1",
        "auto.offset.reset": "earliest", "enable.auto.commit": False, "enable.auto.offset.store": False})
    probe = Consumer({**kafka_settings(), "group.id": group, "enable.auto.commit": False})
    dlt.subscribe([topic + ".DLT"])
    failed = False
    try:
        while True:
            try:
                with DatabaseSettings.from_env().connect() as connection:
                    recover_runs(connection, int(os.environ.get("ABANDONED_RUN_SECONDS", "7200")))
                    for _ in range(100):
                        message = dlt.poll(0.1)
                        if message is None:
                            break
                        if message.error():
                            break
                        capture(connection, message)
                        dlt.commit(message=message, asynchronous=False)
                    resolve_replays(connection)
                    try:
                        lag = consumer_lag(probe, topic, group)
                    except Exception:
                        lag = -1  # Unknown is not zero lag.
                    check_alerts(connection, collect(connection, lag))
                    if failed:
                        notify("monitor_unavailable", False, "Surveillance et base de nouveau accessibles")
                        failed = False
            except Exception as error:
                print(json.dumps({"monitor_error": type(error).__name__}), flush=True)
                if not failed:
                    try:
                        notify("monitor_unavailable", True, "La surveillance ne peut plus lire ou ecrire ses donnees")
                        failed = True
                    except Exception:
                        pass
            time.sleep(max(5, int(os.environ.get("MONITOR_INTERVAL_SECONDS", "30"))))
    finally:
        dlt.close()
        probe.close()
