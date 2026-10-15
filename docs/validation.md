# Validation de LedgerFlow

Campagne locale du 15 octobre 2026. Les tests sont exÃ©cutÃ©s sur des bases et
conteneurs dÃ©diÃ©s ; les fichiers de rÃ©fÃ©rence du projet ne sont pas modifiÃ©s.

## RÃ©sultats automatisÃ©s

| Ensemble | Environnement | RÃ©sultat |
| --- | --- | --- |
| Backend sans broker | Java 21 + H2, migrations V1 Ã  V4 | 43 rÃ©ussis ; 3 tests Kafka explicitement ignorÃ©s |
| Backend avec broker | Java 21/Linux + PostgreSQL 18 + Kafka 4.1.2 | 46 rÃ©ussis, aucun Ã©chec ni test ignorÃ© |
| Batch | Spark 4.1.0 + Python + PostgreSQL 18 + JDBC | 26 rÃ©ussis, aucun Ã©chec |
| Construction | Images Docker backend et batch | Images construites |
| Configuration | Fusion des trois fichiers Compose | Configuration valide |

Les 46 tests backend et les 26 tests Python reprÃ©sentent 72 cas distincts.
Les tests H2 rejouent une partie des mÃªmes cas, et ne doivent pas Ãªtre ajoutÃ©s
comme s'ils couvraient 43 fonctionnalitÃ©s supplÃ©mentaires.

## ScÃ©narios backend

- Comptes, montants, devises et validation des requÃªtes.
- DÃ©pÃ´ts, retraits, virements et historiques.
- Fonds insuffisants, dÃ©passement de capacitÃ© et opÃ©rations refusÃ©es sans effet.
- RequÃªtes rÃ©pÃ©tÃ©es, clÃ©s contradictoires et demandes concurrentes.
- Virements opposÃ©s sans inversion d'ordre des verrous.
- Ã‰quilibre du journal et cohÃ©rence des soldes avec ses Ã©critures.
- Migrations et conservation des donnÃ©es existantes.
- Capture Outbox atomique, panne de publication, reprise et fenÃªtre de crash.
- Inbox/fait atomiques, consommations rÃ©pÃ©tÃ©es ou concurrentes, rejet d'un contenu modifiÃ©.
- Kafka rÃ©el, offsets acquittÃ©s et conservation des messages invalides dans la DLT.
- Lecture des synthÃ¨ses batch, traÃ§abilitÃ© du run et bornes des paramÃ¨tres d'API.

## ScÃ©narios batch

17 cas portent sur les transformations Spark, la prÃ©cision dÃ©cimale et la
configuration ; 9 vÃ©rifient PostgreSQL et les garanties de publication.

- Regroupement par date/devise/type, sans mÃ©lange de devises.
- `0.10 + 0.20 = 0.30` et somme dÃ©passant la prÃ©cision d'un montant individuel.
- Source vide et faits invalides : date, devise, type, montant nul/nÃ©gatif/absent.
- Plages de dates invalides et mot de passe absent/non exposÃ© dans la configuration.
- Vrai calcul Spark avec lecture et Ã©criture JDBC.
- Deux exÃ©cutions identiques sans double comptage.
- Nouvel Ã©vÃ©nement pris en compte aprÃ¨s recalcul de sa date.
- JournÃ©e vide retirant ses anciennes synthÃ¨ses, autres dates prÃ©servÃ©es.
- Ã‰chec de qualitÃ© ou de rÃ©conciliation conservant la publication prÃ©cÃ©dente.
- Ã‰critures JDBC rÃ©pÃ©tÃ©es identiques acceptÃ©es ; contradictions rejetÃ©es.
- Snapshot inchangÃ© malgrÃ© une ingestion ultÃ©rieure.
- Verrou de session rejetant un batch concurrent, puis libÃ©rÃ© correctement.

## Reproduire

Backend sans Kafka, depuis `ledgerflow-backend` avec Java 21 :

```powershell
.\mvnw.cmd test
```

Pour la campagne PostgreSQL/Kafka, dÃ©finir TEST_DB_URL, TEST_DB_USERNAME,
TEST_DB_PASSWORD et TEST_KAFKA_BOOTSTRAP, puis lancer `mvnw verify` dans un
Java 21/Linux ou environnement oÃ¹ les sockets Java fonctionnent correctement.
Les trois tests Kafka ne sont activÃ©s que si TEST_KAFKA_BOOTSTRAP est renseignÃ©.

Batch : construire l'image puis exÃ©cuter pytest comme indiquÃ© dans
[Batch PySpark](batch-pyspark.md). Les tests PostgreSQL nÃ©cessitent une base
jetable `_batch_test` et le montage des migrations. Leur initialisation SQL
est rÃ©servÃ©e aux fixtures ; le schÃ©ma normal de l'application est gÃ©rÃ© par Flyway.

Le workflow GitHub exÃ©cute les deux jobs. Les rÃ©sultats CI doivent Ãªtre consultÃ©s
sur le commit publiÃ© ; ils sont distincts des rÃ©sultats locaux consignÃ©s ici.

## VÃ©rification rÃ©elle et limites de la campagne

Le parcours HTTP bancaire â†’ Outbox â†’ Kafka â†’ faits avait dÃ©jÃ  Ã©tÃ© validÃ© avec
une pause du broker, une Ã©criture bancaire rÃ©ussie pendant cette pause et la
reprise automatique du backlog. La campagne actuelle ajoute le calcul batch et
sa consultation dans l'API ; le rÃ©sultat de cette vÃ©rification est consignÃ©
aprÃ¨s son exÃ©cution dans cette section.

Ces vÃ©rifications ne constituent pas des tests de charge, une certification
bancaire ou une dÃ©monstration de haute disponibilitÃ©. Airflow, Power BI et la
sÃ©curitÃ© ne sont pas inclus, puisqu'ils ne sont pas encore dÃ©veloppÃ©s.
