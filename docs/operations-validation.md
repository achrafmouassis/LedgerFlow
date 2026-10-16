# Validation de l'extension — 16 octobre 2026

Campagne locale distincte de l'historique du 15 octobre. Les tests utilisent
des bases dediees ; aucun compte de demonstration n'a ete ajoute a la base
bancaire principale.

| Verification | Resultat |
| --- | --- |
| Backend H2, migrations V1 a V5 | 43 reussis ; 3 tests Kafka ignores |
| Backend PostgreSQL 18 et Kafka reel | 46 reussis ; aucun echec ni ignore |
| Python/Spark, JDBC et PostgreSQL | 26 reussis |
| Reprise, alertes, DLT et garde d'ingestion sur PostgreSQL | 12 reussis |
| API → Kafka → DLT → correction → rejeu reel | 1 reussi ; un seul fait apres rejeu |
| Graphiques interroges via l'API SQL de Grafana | 12 panneaux sans erreur |
| Regle Grafana surveillant le moniteur | evaluation saine, sans erreur |
| Airflow : run quotidien et run manuel | deux publications SUCCEEDED |
| Notifications | alerte et resolution recues dans Mailpit |
| Lancement du batch sans cle | refuse avec HTTP 401 |
| Compose et diff | configuration valide ; git diff --check sans erreur |

Les 46 + 26 + 12 + 1 representent **85 cas distincts**. Le passage H2 rejoue
des tests du backend et n'est pas ajoute au total. Les controles des graphiques
et de la configuration sont des verifications supplementaires.

Le premier demarrage Airflow a produit une tentative avant que son API interne
reponde. La tache est passee en reprise puis a termine correctement. Grafana
a refuse une premiere adresse SMTP locale sans domaine : les adresses par
defaut ont ete corrigees en `example.test`. Les services de l'extension attendent
le controle de sante du backend pour ne pas acceder aux tables avant Flyway.

## Reproduire

Demarrage et commandes : [operations.md](operations.md).

Backend : definir TEST_DB_URL, TEST_DB_USERNAME, TEST_DB_PASSWORD et
TEST_KAFKA_BOOTSTRAP vers une base de tests et le broker, puis `mvnw verify`.

Operations : installer `data-pipeline/requirements.txt`, definir PYTHONPATH vers
`data-pipeline`, OPS_INTEGRATION=1 et les variables PG* vers une base jetable
suffixee `_ops_test`, puis :

```powershell
python -m pytest data-pipeline/ops_tests -q
```

Le test Kafka reel est opt-in. Il exige un backend isole sur le port 18080 avec
une base suffixee `_smoke_test`, PIPELINE_ENABLED=true,
LEDGERFLOW_PIPELINE_TOPIC=ledgerflow.smoke.operations.v1 et
LEDGERFLOW_PIPELINE_CONSUMER_GROUP=ledgerflow-smoke-analytics. Cote test,
definir OPS_SMOKE=1, SMOKE_TOPIC=ledgerflow.smoke.operations.v1,
KAFKA_BOOTSTRAP_SERVERS et PG* vers cette meme base.
Il cree un compte de test et un depot dans cet environnement uniquement.

```powershell
python -m pytest data-pipeline/ops_tests/test_kafka_smoke.py -q
python scripts/check_dashboards.py
```

Les tests Spark sont decrits dans [batch-pyspark.md](batch-pyspark.md).
La CI ajoute un job `operations` pour les 12 cas PostgreSQL courants. Le test
isole de bout en bout et les interfaces ont ete verifies localement ; ces
resultats ne sont pas annonces comme une campagne CI deja publiee.

## Limites

Ces controles ne constituent pas un test de charge ou une preuve de haute
disponibilite. Airflow reste standalone, Kafka a un broker, Spark reste local
et l'authentification bancaire n'a pas ete ajoutee. Les emails par defaut restent
dans Mailpit ; configurer SMTP ou un webhook pour notifier hors du poste.
