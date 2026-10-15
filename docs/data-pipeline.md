# Event-driven data ingestion

## Implemented patterns

| Pattern | Implementation | Purpose |
| --- | --- | --- |
| Transactional Outbox | Balance, journal and outbox insertion commit together in PostgreSQL | Avoid the database/Kafka dual-write gap |
| Polling Publisher | Bounded scheduled relay; one locked event per transaction, `FOR UPDATE SKIP LOCKED` | Allow retries and multiple relay instances |
| Event-driven streaming ETL | Validate a versioned JSON event, derive a UTC date, load an analytical fact | Incrementally ingest committed operations |
| At-least-once delivery | Mark an event published only after Kafka acknowledges; retain pending events on failure | Recover when Kafka is temporarily unavailable |
| Idempotent Consumer / Inbox | Insert event ID and canonical payload hash together with the fact; duplicate IDs are ignored if identical | Prevent duplicated analytics after replay |
| CQRS-style read projection | Separate `analytics` schema and reporting API; asynchronous updates | Keep analytical reads separate from banking writes |
| Dead Letter Queue | Invalid JSON, unsupported versions and conflicting event payloads are preserved in `<topic>.DLT` | Quarantine bad events while retaining evidence |

The ingestion stage is streaming ETL. A separate [daily PySpark batch](batch-pyspark.md)
now materializes summaries from its facts. The streaming stage itself is not a
CDC connector, Lambda architecture or full event-sourced banking system. The relay polls explicit
business events; it does not read the PostgreSQL WAL. There is no Debezium or
Schema Registry in this increment.

The read model currently shares the same PostgreSQL server, in a separate schema.
It is a first analytical fact table, not yet a separate data warehouse or a star
schema. There is no customer name or API idempotency key in the event.

## Reliability contract

- Every nonzero opening, deposit, withdrawal and transfer creates one event.
  Failed operations and idempotent API retries create none. V3 backfills events
  for the journals created before the pipeline existed.
- The broker key is the event/journal UUID. Each operation is independent;
  events are not guaranteed to arrive in per-account or global time order.
  Facts and daily aggregations tolerate reordered events.
- Producer idempotence and `acks=all` help broker-level reliability, but the
  PostgreSQL/Kafka boundary is still **at least once**, not end-to-end exactly once.
  A crash after broker acknowledgement and before marking the outbox may resend
  the same event. The durable inbox makes the fact insertion idempotent.
- Outbox failures record attempt counts and an error class, and retry with
  bounded exponential delay. No pending row is discarded automatically.
- Consumer offsets use RECORD acknowledgement with auto-commit disabled.
  They are committed after the projection transaction completes. Database
  failures retry indefinitely; permanent event errors go to the dead-letter topic.
  If DLT publication fails, recovery also fails and the source record is retried.
- Inbox insertion and fact insertion commit atomically. A reused event ID with
  different content is rejected, rather than silently changing an existing fact.
- Analytics is eventually consistent. The daily total is grouped by UTC date,
  currency and operation type; amounts in different currencies are never summed.
  OPENING is reported separately and is not a customer deposit.
- Published outbox rows, inbox rows and facts are retained. Kafka uses its broker
  retention configuration. Retention/archival and DLT replay tooling are future work.

## Run

For the backend, PostgreSQL and a local single-node KRaft Kafka broker:

```powershell
docker compose -f compose.yaml -f compose.pipeline.yaml --profile app up --build -d
```

Kafka data has a named volume. A small initialization container assigns its
directory to Kafka's normal application user. Kafka listens on localhost:9092
for host tools and `kafka:19092` inside Compose. The API listens on localhost:8080.
This is a local development broker (replication factor 1), not a production HA cluster.

The existing `docker compose --profile app up --build -d` workflow runs banking
without Kafka; it still captures the outbox. Enabling the pipeline later drains
the backlog. For a local Java launch with Kafka running:

```powershell
$env:PIPELINE_ENABLED = 'true'
$env:KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
cd ledgerflow-backend
.\mvnw.cmd spring-boot:run
```

Windows machines affected by the Java Unix-domain socket issue can use the
Docker launch instead of local Java.

After running the banking API examples, inspect:

```powershell
Invoke-RestMethod http://localhost:8080/api/analytics/pipeline
Invoke-RestMethod http://localhost:8080/api/analytics/daily
Invoke-RestMethod 'http://localhost:8080/api/analytics/daily?from=2026-10-01&to=2026-10-15'
```

`pending` is the outbox backlog, `published` is broker-acknowledged events, and
`processed` is the durable consumer inbox count. Published does not mean processed.
The default daily range is the last seven UTC dates; explicit ranges are limited
to 366 days inclusive.

## Event contract v1

Topic: `ledgerflow.operations.v1`. Dead-letter topic: `ledgerflow.operations.v1.DLT`.
Consumer group: `ledgerflow-analytics-v1`.

```json
{
  "schemaVersion": 1,
  "eventId": "4bde1f4d-6be6-4396-8400-3da7c8c7f457",
  "journalId": "4bde1f4d-6be6-4396-8400-3da7c8c7f457",
  "operationType": "DEPOSIT",
  "debitAccountId": null,
  "creditAccountId": "0d29786a-d772-4f9d-bf8f-c6d063a3497a",
  "amount": 50.00,
  "currency": "MAD",
  "occurredAt": "2026-10-15T10:00:00Z"
}
```

`eventId` equals `journalId`. A null leg means the bank cash clearing account.
OPENING/DEPOSIT have only a credit customer leg; WITHDRAWAL only a debit customer
leg; TRANSFER has different debit and credit customer IDs. The amount is positive
with up to 17 integer digits and 2 decimal places. Unknown versions and malformed
contracts are rejected. A canonical payload hash detects conflicting reuse.

## Verification

`.\mvnw.cmd test` runs banking and database pipeline tests without needing Kafka.
The real Kafka tests are explicitly enabled by setting `TEST_KAFKA_BOOTSTRAP`.
For an isolated PostgreSQL/Kafka test stack on Linux or in a Java 21 container:

```sh
TEST_DB_URL=jdbc:postgresql://postgres:5432/ledgerflow_test \
TEST_DB_USERNAME=ledgerflow TEST_DB_PASSWORD=test_only \
TEST_KAFKA_BOOTSTRAP=kafka:19092 ./mvnw --batch-mode verify
```

Tests cover atomic capture, migration backfill, broker failure/retry, the crash
window after publication, duplicate consumers, projection rollback, UTC
aggregations, real Kafka delivery, committed offsets and dead-letter preservation.
GitHub Actions runs H2 followed by PostgreSQL with a real Kafka broker.

Validated on 2026-10-15: 40 database tests passed on H2; all 43 tests passed
against PostgreSQL 18 and Kafka 4.1.2, including the three real-broker tests.
The Docker image builds successfully. A live HTTP check verified five unique
events, duplicate suppression, successful banking writes during a paused Kafka
broker, recorded delivery failure and automatic backlog recovery after resume
(`pending=0`, `published=5`, `processed=5`).
