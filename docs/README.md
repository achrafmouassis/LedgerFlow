# Documentation LedgerFlow

Commencer par la [documentation technique et fonctionnelle complète](documentation-complete.md).
Elle présente les technologies, les choix d'architecture, les règles bancaires,
les flux streaming et batch, le modèle de données et les limites actuelles.

| Document | Contenu |
| --- | --- |
| [État actuel](etat-actuel.md) | Résultats vérifiés, limites et prochaines étapes au 17 octobre 2026 |
| [Documentation complète](documentation-complete.md) | Vue de référence du projet et de ce qui est réellement implémenté |
| [API bancaire](banking-api.md) | Comptes, dépôts, retraits, virements et exemples PowerShell |
| [Pipeline événementiel](data-pipeline.md) | Outbox, Kafka, consommation idempotente et reprise sur erreur |
| [Batch quotidien PySpark](batch-pyspark.md) | Exécution, snapshot, staging, publication atomique et relance |
| [Validation](validation.md) | Tests exécutés, scénarios et commandes reproductibles |
| [Campagne QA](qa.md) | Générateur de données fictives, erreurs, charge, panne Kafka et rapport HTML |
| [Données de démonstration](demo-data.md) | Plusieurs milliers d'opérations fictives cohérentes dans la base principale |
| [Exploitation et graphiques](operations.md) | Airflow, alertes, reprise des incidents et Grafana |
| [Power BI](power-bi.md) | Rapport natif, connexion PostgreSQL, chargement et limites de validation |
| [Interface et sécurité](security-and-interface.md) | Connexion, rôles, comptes autorisés et démonstration locale |
| [Charge et incidents étendus](scalability-and-incidents.md) | Comparaison multi-instances et pannes isolées |
| [Présentation LinkedIn](linkedin-demo.md) | Parcours de démonstration et limites à annoncer honnêtement |

L'extension d'exploitation ajoute Airflow et Grafana. Le projet Power BI est
disponible ; son chargement PostgreSQL et sa première page ont été confirmés
dans Desktop. L'authentification
bancaire est implémentée ; un entrepôt dimensionnel reste à développer. Le guide d'exploitation
décrit cette extension ; la documentation complète décrit le socle initial.
