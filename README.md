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

1. Build the core banking domain as a modular monolith.
2. Add PostgreSQL persistence and database migrations.
3. Introduce event-driven processing and an outbox pattern.
4. Add data ingestion, observability, testing, and deployment infrastructure.
