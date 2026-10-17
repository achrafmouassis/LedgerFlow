# LedgerFlow

Interface bancaire locale : **http://localhost:8080/**. Connexion, rôles et
permissions : [guide sécurité et interface](docs/security-and-interface.md).
Les mots de passe sont dans le fichier local ignoré `.work/local-access.json`.
Avant un premier démarrage : `python scripts/init_local_auth.py`, puis
`.\scripts\platform.ps1 up -d --build`. Ne jamais publier les identifiants.
Pour savoir ce qui est déjà validé et ce qui reste à faire, voir
[l'état actuel du projet](docs/etat-actuel.md).

Campagne de tests avec données fictives, charge et incidents : voir
[le guide QA](docs/qa.md). Lancement PowerShell : `.\scripts\qa.ps1`.

Documentation de référence en français : [guide complet](docs/documentation-complete.md) et [index docs](docs/README.md).

LedgerFlow is a **banking transaction and data platform demonstration** designed to explore reliable backend engineering, transaction processing, and data pipelines. Current load-test results and deployment limits are documented in [the project status](docs/etat-actuel.md).

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

For the full local platform, initialize local credentials as above, then run `.\scripts\platform.ps1 up -d --build`.
The API is then available at `http://localhost:8080`. For a standalone Java
development run, start PostgreSQL and provide the same database settings plus
`LEDGERFLOW_ADMIN_PASSWORD` (at least 16 characters) before starting Spring Boot.

From the repository root:

```powershell
docker compose up -d
cd ledgerflow-backend
$env:DB_URL = "jdbc:postgresql://localhost:5432/ledgerflow"
$env:DB_USERNAME = "ledgerflow"
$env:DB_PASSWORD = "ledgerflow_dev"
$env:LEDGERFLOW_ADMIN_PASSWORD = "replace-with-a-long-local-password"
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
Banking authentication, roles and explicit account grants are implemented; see [security and interface](docs/security-and-interface.md). This remains a local demonstration, not a production banking deployment.

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
