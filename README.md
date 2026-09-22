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
cd ledgerflow-backend
.\mvnw.cmd spring-boot:run
```

The application requires a PostgreSQL datasource configuration. Until PostgreSQL is configured, startup is expected to stop with a datasource or database connection error.

## Roadmap

1. Build the core banking domain as a modular monolith.
2. Add PostgreSQL persistence and database migrations.
3. Introduce event-driven processing and an outbox pattern.
4. Add data ingestion, observability, testing, and deployment infrastructure.
