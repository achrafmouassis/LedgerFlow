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
| [Exploitation et graphiques](operations.md) | Airflow, alertes, reprise des incidents et Grafana |
| [Power BI](power-bi.md) | Rapport natif, connexion PostgreSQL, chargement et limites de validation |

L'extension d'exploitation ajoute Airflow et Grafana. Le projet Power BI est
disponible, avec chargement et rendu Desktop à vérifier. L'authentification
bancaire et un entrepôt dimensionnel restent à développer. Le guide d'exploitation
décrit cette extension ; la documentation complète décrit le socle initial.
