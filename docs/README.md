# Documentation LedgerFlow

Commencer par la [documentation technique et fonctionnelle complète](documentation-complete.md).
Elle présente les technologies, les choix d'architecture, les règles bancaires,
les flux streaming et batch, le modèle de données et les limites actuelles.

| Document | Contenu |
| --- | --- |
| [Documentation complète](documentation-complete.md) | Vue de référence du projet et de ce qui est réellement implémenté |
| [API bancaire](banking-api.md) | Comptes, dépôts, retraits, virements et exemples PowerShell |
| [Pipeline événementiel](data-pipeline.md) | Outbox, Kafka, consommation idempotente et reprise sur erreur |
| [Batch quotidien PySpark](batch-pyspark.md) | Exécution, snapshot, staging, publication atomique et relance |
| [Validation](validation.md) | Tests exécutés, scénarios et commandes reproductibles |

Les fonctionnalités futures sont identifiées explicitement. Airflow, Power BI,
l'authentification et un entrepôt dimensionnel ne sont pas encore implémentés.
