# LedgerFlow â€” documentation technique et fonctionnelle complÃ¨te

Ã‰tat de rÃ©fÃ©rence : 15 octobre 2026. LedgerFlow est une plateforme pÃ©dagogique
qui simule un backend bancaire fiable et transforme ses opÃ©rations en donnÃ©es
analytiques. Cette documentation dÃ©crit le code prÃ©sent, ses garanties et ses
limites ; elle ne prÃ©sente pas les Ã©lÃ©ments de la feuille de route comme acquis.

## 1. Technologies, techniques et choix de conception

### 1.1 Technologies effectivement utilisÃ©es

| Technologie | Version ou configuration | RÃ´le dans le projet |
| --- | --- | --- |
| Java | 21 | Logique mÃ©tier bancaire et ingestion Ã©vÃ©nementielle |
| Spring Boot | 4.1.1 | Application web, injection, configuration et intÃ©grations |
| Maven | Wrapper 3.9.16 | DÃ©pendances, compilation, tests et JAR exÃ©cutable |
| Spring Web MVC / Tomcat | Versions gÃ©rÃ©es par Spring Boot | API REST JSON |
| Spring Data JPA / Hibernate | Versions gÃ©rÃ©es par Spring Boot | Persistance des comptes, virements et journaux |
| Bean Validation | Jakarta Validation | Validation des requÃªtes HTTP |
| Spring JDBC | Version gÃ©rÃ©e par Spring Boot | Outbox, projection analytique et lectures SQL |
| PostgreSQL | 18 | DonnÃ©es transactionnelles, faits analytiques et rÃ©sultats batch |
| Flyway | Version gÃ©rÃ©e par Spring Boot | Migrations SQL V1 Ã  V4 |
| Apache Kafka | Broker 4.1.2, KRaft | Transport durable des Ã©vÃ©nements d'opÃ©ration |
| Spring for Apache Kafka | Version gÃ©rÃ©e par Spring Boot | Producteur, consommateur, acquittement et rejet des messages |
| Jackson | BibliothÃ¨ques gÃ©rÃ©es par Spring Boot | Contrat JSON des Ã©vÃ©nements et API |
| Python | Python 3 de l'image Spark | Orchestration du traitement batch |
| Apache Spark / PySpark | 4.1.0, Java 21, `local[2]` par dÃ©faut | Calcul des agrÃ©gats par DataFrames |
| Pilote JDBC PostgreSQL | 42.7.13 dans l'image batch, checksum SHA-256 fixÃ© | Lecture et Ã©criture Spark/PostgreSQL |
| Psycopg | 3.3.3 | Transactions de prÃ©paration, contrÃ´le et publication du batch |
| Py4J | 0.10.9.9 | Communication Python/JVM pour PySpark |
| JUnit / Spring Boot Test / MockMvc | Versions gÃ©rÃ©es par Spring Boot | Tests mÃ©tier, de concurrence, de migration et d'API |
| H2 | DÃ©pendance de test | ExÃ©cution rapide sans serveur PostgreSQL |
| pytest | 8.4.2 | Tests Python, Spark rÃ©el et intÃ©gration PostgreSQL |
| Docker et Docker Compose | Images et services dÃ©clarÃ©s dans le dÃ©pÃ´t | Environnement reproductible de dÃ©veloppement et de test |
| Git et GitHub Actions | Workflow `backend.yml` | Versionnement et vÃ©rification automatique backend/batch |
| Spring Boot Actuator | Endpoints `health` et `info` | Ã‰tat de santÃ© de l'application |

Les versions gÃ©rÃ©es restent dÃ©finies par le parent Maven. Le broker Kafka et
les clients Java ne doivent pas Ãªtre confondus : leurs versions peuvent diffÃ©rer.
Le projet ne dÃ©pend pas de React, FastAPI, Talend, Kubernetes ou d'un service
cloud pour son fonctionnement actuel.

### 1.2 Techniques structurantes

- **Monolithe modulaire** : le domaine bancaire et l'ingestion Kafka restent
  dans une application Spring Boot. Le batch PySpark est un processus sÃ©parÃ©.
- **Transactions ACID** : un mouvement financier, ses Ã©critures et son Ã©vÃ©nement
  Outbox sont validÃ©s ensemble ou annulÃ©s ensemble.
- **Verrouillage pessimiste** : les opÃ©rations concurrentes sÃ©rialisent l'accÃ¨s
  aux comptes concernÃ©s. Les virements verrouillent les UUID dans un ordre fixe.
- **Idempotence** : une rÃ©pÃ©tition de requÃªte avec la mÃªme clÃ© et les mÃªmes
  paramÃ¨tres retourne le rÃ©sultat initial sans refaire le mouvement.
- **Montants dÃ©cimaux exacts** : `BigDecimal`, `NUMERIC` et `DecimalType` Ã©vitent
  les erreurs d'arrondi des nombres flottants binaires.
- **SÃ©paration Ã©criture/lecture, style CQRS** : les opÃ©rations bancaires Ã©crivent
  leur modÃ¨le mÃ©tier ; les analyses lisent des projections dÃ©diÃ©es.
- **Ingestion streaming puis matÃ©rialisation batch** : Kafka alimente les faits,
  puis Spark recalcule les synthÃ¨ses des dates demandÃ©es.
- **Snapshot â†’ staging â†’ publication atomique** : un batch incomplet ne remplace
  pas une synthÃ¨se valide, et une relance ne cumule pas ses anciens rÃ©sultats.

Ce fonctionnement hybride ne constitue pas une architecture Lambda complÃ¨te :
il n'existe pas deux chemins indÃ©pendants dont les rÃ©sultats seraient fusionnÃ©s.
Le batch travaille sur les faits canoniques dÃ©jÃ  ingÃ©rÃ©s par le streaming.

## 2. Objectif et pÃ©rimÃ¨tre fonctionnel

LedgerFlow permet de crÃ©er des comptes de simulation, d'effectuer des dÃ©pÃ´ts,
des retraits et des virements, de conserver un journal Ã©quilibrÃ© et d'exploiter
les opÃ©rations Ã  des fins d'analyse. Le but est de dÃ©montrer la cohÃ©rence mÃ©tier,
la gestion de la concurrence, la fiabilitÃ© des Ã©changes et la reproductibilitÃ©
des calculs data.

Le pÃ©rimÃ¨tre actuel comprend :

1. Comptes, soldes et montants d'ouverture de simulation.
2. DÃ©pÃ´ts et retraits avec contrÃ´le de fonds et de capacitÃ© du solde.
3. Virements atomiques entre comptes de mÃªme devise.
4. Journal en partie double et historique paginÃ©.
5. Capture transactionnelle des Ã©vÃ©nements, publication Kafka et dÃ©duplication.
6. Faits analytiques, indicateurs calculÃ©s Ã  la lecture et Ã©tat du pipeline.
7. Batch PySpark quotidien, synthÃ¨ses persistÃ©es et historique d'exÃ©cution.
8. Conteneurisation, migrations, tests automatisÃ©s et CI.

Il n'y a pas encore d'interface graphique, d'authentification, de rÃ´les ou de
vÃ©rification de propriÃ©tÃ© des comptes. Un titulaire est un libellÃ© texte, pas
une entitÃ© utilisateur authentifiÃ©e. Aucun argent rÃ©el ni connexion Ã  une banque
n'est impliquÃ©.

## 3. Organisation du dÃ©pÃ´t

```text
ledgerflow-backend/
  src/main/java/com/ledgerflow/banking/   # comptes, virements, journal, API mÃ©tier
  src/main/java/com/ledgerflow/pipeline/  # Outbox, Kafka, projection et API analytique
  src/main/resources/db/migration/        # V1, V2, V3, V4
  src/test/                              # tests Java et bases de test
  Dockerfile
  pom.xml
data-pipeline/
  ledgerflow_batch/                       # configuration, Spark, stockage, CLI
  tests/                                 # tests de transformations et d'intÃ©gration
  Dockerfile
  requirements.txt
docs/                                    # documentation de rÃ©fÃ©rence
infrastructure/                          # emplacement rÃ©servÃ©, pas de dÃ©ploiement cloud
compose.yaml                             # PostgreSQL et backend optionnel
compose.pipeline.yaml                    # ajout de Kafka et activation du streaming
compose.batch.yaml                       # traitement quotidien optionnel
.github/workflows/backend.yml            # tests backend et job PySpark
```

## 4. Logique bancaire et garanties

### 4.1 CrÃ©ation d'un compte

Un compte contient un UUID, un titulaire de 1 Ã  120 caractÃ¨res non blancs, un
code devise de trois lettres majuscules et un solde. Le code n'est pas vÃ©rifiÃ©
contre un registre ISO ; aucun change de devise n'est proposÃ©.

Le solde initial peut Ãªtre nul ou positif. Un solde non nul produit un journal
`OPENING`. Une ouverture Ã  zÃ©ro ne produit pas de mouvement financier. Ces fonds
sont fournis par le scÃ©nario de simulation ; ce ne sont pas des dÃ©pÃ´ts externes.

### 4.2 DÃ©pÃ´t et retrait

Un dÃ©pÃ´t augmente le solde ; un retrait le diminue et est refusÃ© si les fonds
sont insuffisants. Une opÃ©ration requiert un montant strictement positif, au
plus 17 chiffres entiers et 2 dÃ©cimales. Un solde ne peut dÃ©passer
`99999999999999999.99` ni devenir nÃ©gatif.

Le compte est verrouillÃ© jusqu'Ã  la fin de la transaction. Le nouveau solde,
le journal et l'Outbox sont enregistrÃ©s ensemble. Une panne d'Ã©criture entraÃ®ne
le rollback, y compris si le journal avait dÃ©jÃ  Ã©tÃ© envoyÃ© Ã  la base par un flush.
Un flush n'est pas un commit.

### 4.3 Virement

Le compte source et le compte destination doivent exister, Ãªtre diffÃ©rents et
avoir la mÃªme devise. Le service verrouille leurs UUID dans un ordre stable,
vÃ©rifie les fonds et le plafond du destinataire, puis dÃ©bite et crÃ©dite dans une
seule transaction. Un seul Ã©vÃ©nement reprÃ©sente le virement, pas deux opÃ©rations
analytiques distinctes.

### 4.4 ClÃ© d'idempotence

`Idempotency-Key` est obligatoire pour les dÃ©pÃ´ts, retraits et virements. Elle
contient 1 Ã  100 caractÃ¨res parmi les lettres, chiffres et `._:-`.
Son espace est global aux opÃ©rations : une clÃ© de dÃ©pÃ´t ne peut pas servir Ã  un
retrait ou Ã  un virement diffÃ©rent.

- MÃªme clÃ©, mÃªme opÃ©ration, mÃªmes comptes et mÃªme montant numÃ©rique : rÃ©sultat initial.
- MÃªme clÃ©, paramÃ¨tres diffÃ©rents : conflit HTTP 409.
- Deux demandes concurrentes : les verrous et contraintes uniques empÃªchent un double effet.

Les clÃ©s sont conservÃ©es avec les opÃ©rations. Les dates sont ramenÃ©es Ã  la
prÃ©cision microseconde pour garder un rÃ©sultat stable aprÃ¨s relecture en base.

### 4.5 Journal en partie double

Du point de vue de la banque, les fonds d'un client sont un passif : un crÃ©dit
client augmente son solde, un dÃ©bit client le diminue.

| Mouvement | DÃ©bit | CrÃ©dit |
| --- | --- | --- |
| Ouverture non nulle | TrÃ©sorerie de simulation | Compte client |
| DÃ©pÃ´t | TrÃ©sorerie de simulation | Compte client |
| Retrait | Compte client | TrÃ©sorerie de simulation |
| Virement | Compte client source | Compte client destination |

Un enregistrement `journals` stocke les deux jambes avec un montant et une
devise communs. Les contraintes SQL rendent impossible une seule jambe ou un
montant diffÃ©rent entre les jambes de cet enregistrement. L'API restitue deux
entrÃ©es, `DEBIT` et `CREDIT`, sous `CUSTOMER:<uuid>` ou `CASH:<devise>`.

Il n'existe pas de table sÃ©parÃ©e pour chaque jambe ni de plan comptable complet.
Le journal est immuable dans l'application, sans API de modification/suppression ;
un administrateur de base reste techniquement capable d'Ã©diter les donnÃ©es.

## 5. ModÃ¨le de donnÃ©es et migrations

| Table | Grain et rÃ´le |
| --- | --- |
| `accounts` | Un compte et son solde courant |
| `transfers` | Un virement avec clÃ© d'idempotence |
| `journals` | Une opÃ©ration financiÃ¨re Ã©quilibrÃ©e |
| `outbox_events` | Un Ã©vÃ©nement immuable de journal et son Ã©tat de publication |
| `analytics.processed_events` | Un Ã©vÃ©nement consommÃ© et son hash canonique |
| `analytics.transaction_facts` | Un fait par Ã©vÃ©nement validÃ© |
| `analytics.batch_runs` | Une tentative de calcul sur une plage de dates |
| `analytics.batch_source_snapshot` | Une copie temporaire des faits retenus pour un run |
| `analytics.daily_summary_stage` | Des agrÃ©gats intermÃ©diaires, avant publication |
| `analytics.daily_operation_summary` | Une synthÃ¨se par date UTC, devise et type d'opÃ©ration |

Le modÃ¨le analytique partage actuellement PostgreSQL avec le modÃ¨le bancaire,
dans un schÃ©ma distinct. Ce n'est pas encore un entrepÃ´t dimensionnel sÃ©parÃ©.
Les faits n'ont pas de clÃ© Ã©trangÃ¨re vers les comptes/journaux du modÃ¨le bancaire :
la projection consomme le contrat Ã©vÃ©nementiel, pas les entitÃ©s JPA.

| Migration | Contenu |
| --- | --- |
| V1 | Comptes, virements, contraintes et index |
| V2 | Journal Ã©quilibrÃ© et reconstitution des ouvertures/virements existants |
| V3 | Outbox, inbox consommateur, faits et capture des journaux historiques |
| V4 | Historique batch, snapshot, staging et synthÃ¨ses quotidiennes |

Flyway applique les migrations dans l'ordre ; Hibernate valide le schÃ©ma.
Une migration appliquÃ©e ne doit pas Ãªtre rÃ©Ã©crite. Les anciennes ouvertures
reconstituÃ©es par V2 ont une date de migration, car V1 n'enregistrait pas leur date
originale. Cela limite l'interprÃ©tation historique des dates d'ouverture.

## 6. Pipeline streaming et contrat d'Ã©vÃ©nement

```mermaid
flowchart LR
  A[API bancaire] --> T[Transaction ACID]
  T --> B[Solde et journal]
  T --> O[Outbox PostgreSQL]
  O --> R[Relais polling]
  R --> K[Kafka]
  K --> V[Validation et inbox]
  V --> F[Faits analytiques]
  V --> D[Topic de rejet pour messages invalides]
  F --> Q[API analytique Ã  la lecture]
  F --> S[Snapshot batch]
  S --> P[PySpark]
  P --> G[Staging et rÃ©conciliation]
  G --> M[SynthÃ¨ses persistÃ©es]
  M --> H[API batch]
```

### 6.1 Transactional Outbox et Polling Publisher

Le service bancaire capture l'Ã©vÃ©nement dans sa transaction. Il ne publie pas
synchroniquement vers Kafka pendant le traitement HTTP. L'indisponibilitÃ© de
Kafka n'empÃªche donc pas le dÃ©pÃ´t ou le virement d'Ãªtre validÃ© en PostgreSQL.

Le relais se dÃ©clenche par dÃ©faut toutes les secondes et traite au plus 25
Ã©vÃ©nements par passage. Il sÃ©lectionne une ligne disponible avec
`FOR UPDATE SKIP LOCKED`, attend l'acquittement Kafka, puis la marque publiÃ©e.
Chaque ligne est traitÃ©e dans une transaction propre. Un Ã©chec conserve la ligne,
incrÃ©mente les tentatives et programme une reprise avec dÃ©lai exponentiel bornÃ©.

Le producteur utilise l'idempotence Kafka et `acks=all`. La frontiÃ¨re PostgreSQL /
Kafka reste nÃ©anmoins **au moins une fois** : une panne aprÃ¨s l'acquittement mais
avant le commit de publication peut provoquer une rÃ©Ã©mission.

### 6.2 Idempotent Consumer et Inbox

Le consommateur valide la version et la structure du JSON. Il normalise le montant,
calcule un hash SHA-256 du contrat et insÃ¨re l'ID dans l'inbox avec le fait dans la
mÃªme transaction. Une rÃ©pÃ©tition identique est ignorÃ©e ; un ID rÃ©utilisÃ© avec un
contenu diffÃ©rent est rejetÃ©. L'offset Kafka est acquittÃ© aprÃ¨s le commit de cette
projection (`RECORD`, auto-commit dÃ©sactivÃ©).

Les erreurs de base sont rÃ©essayÃ©es ; les contrats invalides sont conservÃ©s dans
le topic `.DLT`. Si cette publication de rejet Ã©choue, le message source reste
Ã  reprendre. Les erreurs invalides ne sont pas comptÃ©es comme faits rÃ©ussis.

### 6.3 Contrat et ordre

Le topic principal est `ledgerflow.operations.v1`, sa DLT est
`ledgerflow.operations.v1.DLT`, et le groupe est `ledgerflow-analytics-v1`.
Le contrat v1 contient : version, Ã©vÃ©nement/journal UUID, type, UUID des jambes,
montant, devise et instant de l'opÃ©ration. Aucun nom de titulaire ou clÃ© API n'est
transmis. L'ID de l'Ã©vÃ©nement est celui du journal.

La clÃ© Kafka est cet UUID. Il n'y a pas de garantie d'ordre global ou par compte.
Les faits indÃ©pendants et les agrÃ©gations supportent l'arrivÃ©e dans le dÃ©sordre.
La date analytique provient de l'instant converti en UTC. OPENING reste un type
sÃ©parÃ© et ne doit pas Ãªtre assimilÃ© Ã  un dÃ©pÃ´t client.

La projection est Ã©ventuellement cohÃ©rente : un Ã©vÃ©nement publiÃ© n'est pas
nÃ©cessairement dÃ©jÃ  consommÃ©. Les compteurs pending/published/processed et le
contenu de la DLT servent Ã  diagnostiquer ce dÃ©calage.

## 7. Batch quotidien PySpark

### 7.1 EntrÃ©e, grain et indicateurs

Le batch lit les faits validÃ©s de `analytics.transaction_facts`, jamais les soldes
courants pour reconstituer artificiellement les opÃ©rations. Le grain de sortie est :

`date UTC Ã— devise Ã— type d'opÃ©ration`.

Pour chaque groupe, Spark calcule le nombre d'opÃ©rations, le montant total,
le minimum et le maximum. Les devises sont sÃ©parÃ©es et un virement est comptÃ©
une fois. Les montants restent dÃ©cimaux, avec des totaux `NUMERIC(38,2)` afin de
pouvoir dÃ©passer la capacitÃ© d'un montant individuel `NUMERIC(19,2)`.

### 7.2 ExÃ©cution et cohÃ©rence

1. AcquÃ©rir un verrou consultatif PostgreSQL de session : un seul batch publie
   Ã  la fois, y compris si deux plages se chevauchent.
2. CrÃ©er un `run_id`, ses dates et son Ã©tat `RUNNING`.
3. Copier les faits de la plage dans un snapshot par une instruction SQL unique.
   La lecture MVCC forme une vue cohÃ©rente ; les nouveaux faits n'en changent pas
   le contenu pendant le calcul Spark.
4. Lire ce snapshot via JDBC, vÃ©rifier les champs et agrÃ©ger avec PySpark.
5. Ã‰crire les groupes dans le staging, pas directement dans la table exposÃ©e.
6. RÃ©concilier les rÃ©sultats avec une agrÃ©gation SQL indÃ©pendante du snapshot :
   groupes, nombres, totaux, minima et maxima doivent correspondre exactement.
7. Dans une transaction, remplacer les lignes de la plage, enregistrer la rÃ©ussite
   et nettoyer les donnÃ©es temporaires du run.

La rÃ©conciliation ne remplace pas Spark : elle contrÃ´le le rÃ©sultat calculÃ© par
Spark avant de le rendre visible.

### 7.3 Relance, concurrence et Ã©chec

Le batch recalcule complÃ¨tement les partitions de dates choisies. Il ne fait pas
`ancien total + nouveau total`, et ne repose pas sur un watermark incrÃ©mental.
Une relance produit un nouveau `run_id` sans ajouter de doublons aux synthÃ¨ses.
Une journÃ©e vide supprime les anciens groupes de cette journÃ©e, mais ne touche
pas aux autres dates.

Les Ã©critures JDBC des tÃ¢ches Spark peuvent Ãªtre partielles et rÃ©pÃ©tÃ©es. Le
staging accepte des rÃ©pÃ©titions identiques ; la publication sÃ©lectionne les
lignes distinctes. Des rÃ©pÃ©titions contradictoires Ã©chouent Ã  la rÃ©conciliation.
Un Ã©chec de validation ne remplace pas la publication prÃ©cÃ©dente.

Les erreurs prises en charge marquent le run `FAILED` et nettoient ses tables
temporaires. Une terminaison brutale peut laisser un run `RUNNING` et ses donnÃ©es
intermÃ©diaires : le verrou est libÃ©rÃ© Ã  la fermeture de la connexion, mais il n'y
a pas encore de mÃ©canisme automatique de marquage des runs abandonnÃ©s.

### 7.4 Ã‰vÃ©nements tardifs et caractÃ¨re quotidien

Par dÃ©faut, le traitement porte sur **hier en UTC**. Une date ou une plage de
366 jours maximum peut Ãªtre demandÃ©e. Un Ã©vÃ©nement encore dans Kafka au moment
du snapshot n'est pas inclus ; relancer sa date aprÃ¨s consommation corrige le
rÃ©sultat. Un batch rÃ©ussi signifie cohÃ©rence avec son snapshot, pas absence de
retard dans l'ingestion.

Le lancement est manuel par CLI/Compose. Il n'existe pas encore de planification
horaire automatique : ce sera le rÃ´le d'Airflow. Spark s'exÃ©cute par dÃ©faut en
mode local Ã  deux threads dans un conteneur, pas sur un cluster HA distribuÃ©.

## 8. API exposÃ©e

| MÃ©thode | Route | Fonction |
| --- | --- | --- |
| POST | `/api/accounts` | CrÃ©er un compte |
| GET | `/api/accounts/{id}` | Consulter le solde |
| POST | `/api/accounts/{id}/deposits` | DÃ©poser des fonds simulÃ©s |
| POST | `/api/accounts/{id}/withdrawals` | Retirer des fonds |
| POST | `/api/transfers` | ExÃ©cuter un virement |
| GET | `/api/accounts/{id}/transfers` | Historique des virements |
| GET | `/api/accounts/{id}/journal` | Journaux impliquant le compte |
| GET | `/api/analytics/pipeline` | Compteurs de publication/consommation |
| GET | `/api/analytics/daily` | AgrÃ©gation Ã  la lecture des faits actuellement reÃ§us |
| GET | `/api/analytics/batch/daily` | SynthÃ¨ses persistÃ©es par le dernier batch de chaque date |
| GET | `/api/analytics/batch/runs` | Historique rÃ©cent des exÃ©cutions batch |
| GET | `/actuator/health` | Ã‰tat de santÃ© |

Les historiques bancaires sont paginÃ©s Ã  partir de 0, taille 1 Ã  100. Les API
daily acceptent `from` et `to` inclusifs, 366 jours maximum ; leur dÃ©faut est une
fenÃªtre de sept dates UTC. L'historique batch accepte `limit` de 1 Ã  100, dÃ©faut 20.
Les synthÃ¨ses batch exposent Ã©galement `runId` et `computedAt`.

Les validations incorrectes retournent 400 ; un compte absent 404 ; un conflit
d'idempotence, des fonds insuffisants ou un plafond dÃ©passÃ© 409. Les erreurs
mÃ©tier utilisent `ProblemDetail` avec un code applicatif. Il n'existe pas de route
HTTP dÃ©clenchant Spark : le calcul est un processus distinct.

## 9. Installation et exploitation

Depuis la racine du dÃ©pÃ´t, Docker Desktop doit Ãªtre dÃ©marrÃ©. Le lancement
conteneurisÃ© Ã©vite d'installer Java, Python ou Spark sur la machine hÃ´te.

```powershell
docker compose -f compose.yaml -f compose.pipeline.yaml -f compose.batch.yaml --profile app up --build -d
Invoke-RestMethod http://localhost:8080/actuator/health
```

Attendre le statut UP avant d'utiliser l'API ; Flyway crÃ©e les tables lors du
dÃ©marrage. Le profil batch est sÃ©parÃ© pour Ã©viter un calcul implicite lors de
chaque dÃ©marrage du backend.

```powershell
# Hier UTC, par dÃ©faut
docker compose -f compose.yaml -f compose.pipeline.yaml -f compose.batch.yaml --profile batch run --rm --build daily-batch
# Recalculer explicitement une date
docker compose -f compose.yaml -f compose.pipeline.yaml -f compose.batch.yaml --profile batch run --rm daily-batch --date 2026-10-15
# Reprendre une plage, Ã©vÃ©nements tardifs compris
docker compose -f compose.yaml -f compose.pipeline.yaml -f compose.batch.yaml --profile batch run --rm daily-batch --from 2026-10-01 --to 2026-10-15
Invoke-RestMethod 'http://localhost:8080/api/analytics/batch/daily?from=2026-10-01&to=2026-10-15'
Invoke-RestMethod 'http://localhost:8080/api/analytics/batch/runs?limit=10'
```

Une sortie batch rÃ©ussie est un JSON avec `run_id`, `status`, dates, `source_count`
et `group_count`. Le code de sortie est non nul en cas d'Ã©chec. Les paramÃ¨tres de
connexion Python sont PGHOST, PGPORT, PGDATABASE, PGUSER et PGPASSWORD. Le backend
utilise DB_URL, DB_USERNAME, DB_PASSWORD, PIPELINE_ENABLED et KAFKA_BOOTSTRAP_SERVERS.
Les valeurs de Compose sont des identifiants de dÃ©veloppement, pas des secrets
de production. Les mots de passe ne figurent pas dans l'URL JDBC gÃ©nÃ©rÃ©e du batch.

L'API Docker est liÃ©e Ã  localhost:8080, Kafka hÃ´te Ã  localhost:9092 et PostgreSQL
publie le port 5432. Les volumes nommÃ©s conservent PostgreSQL et Kafka entre les
redÃ©marrages. `docker compose down -v` supprime les volumes : ce n'est pas une
commande de maintenance ordinaire.

Pour un lancement Spring local, Java 21 et le wrapper Maven sont nÃ©cessaires.
Sur les machines Windows affectÃ©es par les sockets Unix/NIO de Java ou Docker,
le lancement conteneurisÃ© de Java reste l'option validÃ©e ; le problÃ¨me de l'hÃ´te
n'est pas corrigÃ© par la logique mÃ©tier de LedgerFlow.

## 10. Tests, CI et diagnostic

Les tests Java couvrent rÃ¨gles mÃ©tier, concurrence, rollback, idempotence,
migrations, API, Outbox, inbox, Kafka rÃ©el et messages de rejet. Les tests Python
couvrent configuration, dÃ©cimaux, qualitÃ© des faits, Spark/JDBC rÃ©el, relance,
retard d'ingestion, journÃ©e vide, rÃ©conciliation et verrou concurrent.

La validation dÃ©taillÃ©e et ses commandes figurent dans [Validation](validation.md).
Le workflow GitHub comprend un job backend (H2 puis PostgreSQL/Kafka) et un job
batch (image Spark et tests PostgreSQL). Un succÃ¨s local ne doit pas Ãªtre prÃ©sentÃ©
comme un succÃ¨s CI avant que GitHub Actions ait terminÃ©.

Pour diagnostiquer : consulter la santÃ©, les compteurs du pipeline, la DLT,
`analytics.batch_runs`, puis les logs du composant concernÃ©. Un `pending` nul ne
prouve pas Ã  lui seul que tous les Ã©vÃ©nements publiÃ©s ont Ã©tÃ© consommÃ©s. Une
synthÃ¨se absente peut simplement signifier qu'aucun batch n'a encore Ã©tÃ© lancÃ©
pour la date demandÃ©e.

## 11. Limites et prochaines Ã©tapes

- L'authentification, les autorisations et la propriÃ©tÃ© des comptes restent Ã  faire.
- Le journal n'est pas un plan comptable bancaire complet, ni une preuve inviolable.
- Aucun taux de change, paiement externe, rapprochement bancaire ou argent rÃ©el.
- Broker KRaft local Ã  un nÅ“ud, facteur de rÃ©plication 1 ; pas de HA de production.
- Pas de CDC/WAL, Debezium, Schema Registry ou promesse exactly-once de bout en bout.
- Pas encore de data lake, Delta/Iceberg ou modÃ¨le en Ã©toile avec dimensions.
- Pas encore d'Airflow, de tableaux Power BI ou de dÃ©ploiement cloud.
- Conservation et archivage des Outbox/inbox/faits/runs non automatisÃ©s ; les
  snapshots/stagings sont nettoyÃ©s pour les exÃ©cutions terminÃ©es normalement.
- Le snapshot du batch duplique temporairement les faits sÃ©lectionnÃ©s. C'est un
  choix de cohÃ©rence simple ; une montÃ©e en charge demandera une stratÃ©gie adaptÃ©e.

L'Ã©tape data suivante est l'orchestration Airflow : lancer la date attendue,
rejouer les erreurs, surveiller le retard d'ingestion et organiser les reprises.
Power BI pourra ensuite lire les synthÃ¨ses et un futur modÃ¨le dimensionnel.
