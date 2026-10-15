# LedgerFlow

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
Spark/PySpark, Airflow, Power BI and authentication remain future work.

Run banking plus Kafka with:

```powershell
docker compose -f compose.yaml -f compose.pipeline.yaml --profile app up --build -d
```

- [x] Core banking domain as a modular monolith, with balanced journal.
- [x] PostgreSQL persistence, Flyway migrations and automated tests.
- [x] Transactional Outbox, Kafka and an idempotent analytical read model.
- [ ] Spark/PySpark transformations and Airflow orchestration.
- [ ] Dimensional analytics, Power BI and expanded operational monitoring.
