# LedgerFlow

Documentation de rÃ©fÃ©rence en franÃ§ais : [guide complet](docs/documentation-complete.md) et [index docs](docs/README.md).

LedgerFlow is a **Scalable Banking Transaction & Data Platform** designed to explore reliable backend engineering, transaction processing, data pipelines, and production-ready infrastructure.

## Project structure

```text
ledgerflow/
|-- ledgerflow-backend/  # Java 21 and Spring Boot API
|-- data-pipeline/       # Data ingestion and processing components
|-- infrastructure/      # Local and cloud infrastructure definitions
`-- docs/                # Architecture and project documentation
```

## Backend stack

- Java 21
- Spring Boot and Maven
- Spring Web
- Spring Data JPA
- Bean Validation
- PostgreSQL
- Spring Boot Actuator

## Getting started

For an entirely Docker-based launch, run `docker compose --profile app up --build -d`.
The API is then available at `http://localhost:8080`. Alternatively, use the local
Java development workflow below.

From the repository root:

```powershell
docker compose up -d
cd ledgerflow-backend
.\mvnw.cmd spring-boot:run
```

PostgreSQL runs in Docker on `localhost:5432`, while Spring Boot runs locally. The default development database is `ledgerflow`, using the `ledgerflow` user. You can override the connection without editing source files:

```powershell
$env:DB_URL = "jdbc:postgresql://localhost:5432/ledgerflow"
$env:DB_USERNAME = "ledgerflow"
$env:DB_PASSWORD = "your-password"
```

Check the database and stop it with:

```powershell
docker compose ps
docker compose down
```

The named Docker volume keeps the database data between container restarts. Use `docker compose down -v` only when you intentionally want to delete the development data.

## Roadmap

The banking backend implements accounts, balances, deposits, withdrawals,
atomic same-currency transfers, globally idempotent retries and paginated history.
A balanced double-entry journal records every movement, including opening funds.
JPA and Flyway manage persistence and backward-compatible schema migrations.
See [Banking API](docs/banking-api.md) for examples, guarantees, limitations and tests.
GitHub Actions verifies the backend on H2 and PostgreSQL 18.
The first data stage now implements a transactional Outbox, Kafka delivery,
idempotent analytical ingestion and daily reporting. See
[Data pipeline](docs/data-pipeline.md) for patterns, guarantees and tests.
Daily PySpark materialization now builds persistent summaries with a consistent source snapshot,
staging reconciliation and atomic date-range replacement. See [Daily batch](docs/batch-pyspark.md).
Optional Airflow scheduling, operational recovery, email alerts and Grafana
dashboards are available; see [Operations](docs/operations.md).
A native Power BI project is available in [bi/powerbi](bi/powerbi/README.md).
Banking authentication remains future work.

Run banking plus Kafka with:

```powershell
docker compose -f compose.yaml -f compose.pipeline.yaml --profile app up --build -d
```

- [x] Core banking domain as a modular monolith, with balanced journal.
- [x] PostgreSQL persistence, Flyway migrations and automated tests.
- [x] Transactional Outbox, Kafka and an idempotent analytical read model.
- [x] Daily PySpark summaries, safe reruns, run history and integration tests.
- [x] Airflow orchestration, safe abandoned-run recovery and explicit DLT replay.
- [x] Operational monitoring, local email alerts and Grafana dashboards.
- [x] Native Power BI project, aggregate-only SQL views and read-only BI access.
- [ ] Expanded dimensional analytics and production deployment.

## Daily PySpark batch

Start the backend first so Flyway applies the schema, then run the optional batch
(default: previous UTC day):

```powershell
docker compose -f compose.yaml -f compose.pipeline.yaml -f compose.batch.yaml --profile app up --build -d
docker compose -f compose.yaml -f compose.batch.yaml --profile batch run --rm --build daily-batch
```

Use `--date YYYY-MM-DD` or `--from YYYY-MM-DD --to YYYY-MM-DD` for a rebuild.
Read `/api/analytics/batch/daily` and `/api/analytics/batch/runs` for results and status.
The standalone batch is launched explicitly. The optional operations stack adds
daily Airflow scheduling at 02:00 UTC with retries and a three-day lookback:

```powershell
.\scripts\platform.ps1 up -d --build
```

Open Grafana on `http://localhost:3000`, Airflow on `http://localhost:8081`, and
the local alert inbox on `http://localhost:8025`. Read [Operations](docs/operations.md)
for credentials, incident recovery, notification delivery and configuration.
