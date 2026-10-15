# LedgerFlow data pipeline

The first implemented data stage is event-driven streaming ingestion into a
PostgreSQL analytical read model. Code lives in the backend's
`com.ledgerflow.pipeline` module; the banking domain remains a modular monolith.

```mermaid
flowchart LR
  API[Banking operation] --> TX[Single database transaction]
  TX --> J[Balanced journal and balances]
  TX --> O[Transactional Outbox]
  O --> R[Polling relay]
  R --> K[Kafka operations topic]
  K --> C[Validate and deduplicate]
  C --> F[Analytical transaction facts]
  F --> Q[Daily reporting API]
  C --> D[Dead-letter topic for invalid events]
```

See [Pipeline contract and operations](../docs/data-pipeline.md) for the exact
patterns, event format, failure behavior, startup instructions and tests.

Spark/PySpark, Airflow and Power BI are subsequent stages; they are not required
or implemented in this first ingestion stage.
