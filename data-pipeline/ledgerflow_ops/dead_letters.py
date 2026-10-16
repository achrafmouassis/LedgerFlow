"""Durable DLT capture and bounded, explicit replay with a delivery receipt."""
import json
import re
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from uuid import UUID, uuid4


def capture(connection, message):
    connection.execute("""
        INSERT INTO analytics.dead_letters
            (id,topic,partition_id,offset_id,message_key,payload,captured_at)
        VALUES (%s,%s,%s,%s,%s,%s,clock_timestamp())
        ON CONFLICT(topic,partition_id,offset_id) DO NOTHING
        """, (uuid4(), message.topic(), message.partition(), message.offset(), message.key(), message.value()))


def replay(connection, producer, letter_id, target_topic, replacement=None):
    with connection.transaction():
        row = connection.execute("""
            SELECT payload,status FROM analytics.dead_letters WHERE id=%s FOR UPDATE
            """, (UUID(str(letter_id)),)).fetchone()
        if not row:
            raise ValueError("Unknown dead letter")
        if row[1] == "RESOLVED":
            raise ValueError("Dead letter already resolved")
        payload = Path(replacement).read_bytes() if replacement else bytes(row[0] or b"")
        if not 0 < len(payload) <= 1048576:
            raise ValueError("Payload must contain 1 to 1048576 bytes")
        event = json.loads(payload, parse_float=Decimal)
        event_id = UUID(event["eventId"])
        if UUID(event["journalId"]) != event_id or event.get("schemaVersion") != 1:
            raise ValueError("Invalid event envelope")
        amount = Decimal(str(event["amount"]))
        if not amount.is_finite() or amount <= 0 or amount > Decimal("99999999999999999.99") or amount.as_tuple().exponent < -2:
            raise ValueError("Invalid amount")
        if not re.fullmatch("[A-Z]{3}", event["currency"]):
            raise ValueError("Invalid currency")
        if datetime.fromisoformat(event["occurredAt"].replace("Z", "+00:00")).tzinfo is None:
            raise ValueError("Timestamp requires a timezone")
        debit = UUID(event["debitAccountId"]) if event.get("debitAccountId") else None
        credit = UUID(event["creditAccountId"]) if event.get("creditAccountId") else None
        kind = event["operationType"]
        if not ((kind in ("OPENING", "DEPOSIT") and debit is None and credit is not None)
                or (kind == "WITHDRAWAL" and debit is not None and credit is None)
                or (kind == "TRANSFER" and debit is not None and credit is not None and debit != credit)):
            raise ValueError("Invalid operation accounts")
        receipt = []
        producer.produce(target_topic, key=str(event_id).encode(), value=payload,
                         on_delivery=lambda error, message: receipt.append(error))
        if producer.flush(15) != 0 or len(receipt) != 1 or receipt[0] is not None:
            raise RuntimeError("Kafka did not acknowledge replay")
        # At-least-once: crash after Kafka ack can cause another replay. The Java
        # inbox still deduplicates. REPLAYED means sent, not successfully ingested.
        connection.execute("""
            UPDATE analytics.dead_letters SET status='REPLAYED',replay_event_id=%s,replay_payload=%s,
                replayed_at=clock_timestamp(),replay_count=replay_count+1 WHERE id=%s
            """, (event_id, payload.decode("utf-8"), UUID(str(letter_id))))


def resolve_replays(connection):
    # A matching ID alone is insufficient: a conflicting replay must not be
    # reported as resolved. Compare the requested data to the committed fact.
    connection.execute("""
        UPDATE analytics.dead_letters d SET status='RESOLVED',resolved_at=clock_timestamp()
        FROM analytics.transaction_facts f
        WHERE d.status='REPLAYED' AND d.replay_event_id=f.event_id
          AND f.journal_id=(d.replay_payload::jsonb->>'journalId')::uuid
          AND f.operation_type=d.replay_payload::jsonb->>'operationType'
          AND f.currency=d.replay_payload::jsonb->>'currency'
          AND f.amount=(d.replay_payload::jsonb->>'amount')::numeric
          AND f.occurred_at=(d.replay_payload::jsonb->>'occurredAt')::timestamptz
          AND f.debit_account_id IS NOT DISTINCT FROM (d.replay_payload::jsonb->>'debitAccountId')::uuid
          AND f.credit_account_id IS NOT DISTINCT FROM (d.replay_payload::jsonb->>'creditAccountId')::uuid
        """
    )
