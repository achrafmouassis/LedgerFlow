# Airflow, surveillance, reprise et tableaux de bord

Cette extension ajoute quatre fonctions au pipeline existant, sans changer les
regles bancaires : execution quotidienne, alertes, reprise des incidents et
graphiques. Elle utilise Airflow 3.1.7, Grafana 12.3.1 et une boite mail locale
Mailpit. Un [rapport Power BI](power-bi.md) complete les graphiques Grafana.

## Demarrage local

Prerequis : Docker Desktop demarre, environ 6 Go de memoire disponibles pour
l'ensemble des services et les ports 8080, 8081, 3000, 8025, 5432 et 9092 libres.
Depuis la racine du depot :

```powershell
.\scripts\platform.ps1 up -d --build
.\scripts\platform.ps1 ps
```

Le backend applique Flyway V1 a V6 avant que les outils puissent utiliser leurs
tables. Les services attendent son controle de sante. `dashboard-init` cree
ensuite le compte SQL de lecture et se termine normalement.

| Adresse | Utilite | Connexion locale |
| --- | --- | --- |
| http://localhost:8081 | Airflow : calendrier et executions | admin ; mot de passe genere ci-dessous |
| http://localhost:3000 | Grafana : tableaux de bord | admin / ledgerflow-local-admin |
| http://localhost:8025 | Mailpit : boite des alertes de demonstration | pas de connexion |
| http://localhost:8080/actuator/health | Sante du backend | pas de connexion |

Lire le mot de passe Airflow genere (ne pas publier sa sortie) :

```powershell
.\scripts\platform.ps1 exec airflow cat /opt/airflow/logs/admin-password.json
```

Les mots de passe par defaut sont uniquement destines au poste de developpement.
Les nouvelles interfaces sont liees a localhost ; le service de lancement Spark
n'a aucun port publie. Pour personnaliser les secrets et les seuils, copier
`.env.ops.example` vers `.env.ops`, puis utiliser :

```powershell
.\scripts\platform.ps1 --env-file .env.ops up -d --build
```

Le mot de passe de la base Airflow doit etre compatible avec une URL. Le changer
dans un environnement deja initialise demande aussi de changer le role PostgreSQL
existant ; une variable d'environnement seule ne modifie pas un ancien volume.
Le mot de passe administrateur Grafana est initialise lors du premier demarrage.

## 1. Calcul quotidien Airflow

Le DAG `ledgerflow_daily_summary` se declenche chaque jour a **02 h UTC**,
soit 04 h en France metropolitaine en ete et 03 h en hiver. Il est active a sa
creation, sans rejouer automatiquement tout l'historique.

Par defaut, il recalcule les trois jours se terminant la veille UTC. Cette petite
fenetre permet d'integrer les arrivees tardives recentes. `lookback_days` se
configure de 1 a 30 au lancement manuel. `target_date` permet de choisir une
date de fin explicite ; sinon la date est derivee de la date logique Airflow,
pas de l'heure de la nouvelle tentative. Un retry apres minuit ne change donc
pas silencieusement de jour a traiter.

Avant Spark, le service verifie que chaque evenement bancaire connu de cette
periode possede un recu analytique. Le compteur des messages publies seul ne
suffit pas. Une ingestion incomplete fait echouer temporairement la demande,
puis Airflow reessaie : **3 retries**, attente initiale de **5 minutes**, croissante
et bornee a **20 minutes**. Une seule execution du DAG est active a la fois.
Un verrou PostgreSQL protege egalement les executions manuelles concurrentes.

Le service prive `batch-worker` utilise une cle partagee avec Airflow et accepte
seulement une plage de dates. Il lance le CLI PySpark existant sans monter le
socket Docker. Le calcul est limite a une heure ; la tache Airflow a 65 minutes.
Le mode Airflow `standalone` rassemble ses composants pour le developpement ;
ce n'est pas un deploiement Airflow de haute disponibilite.

Declencher depuis l'interface Airflow ou depuis le terminal :

```powershell
.\scripts\platform.ps1 exec airflow airflow dags trigger ledgerflow_daily_summary --conf '{"target_date":"2026-10-15","lookback_days":1}'
```

Une indisponibilite prolongee ou un evenement plus ancien que la fenetre exige
un recalcul explicite. La verification prealable n'est pas une preuve qu'aucun
nouvel evenement historique n'arrivera apres le snapshot.

## 2. Surveillance et alertes

Le moniteur travaille toutes les 30 secondes par defaut. Il conserve des mesures
dans `analytics.monitor_samples` et les etats d'alerte dans `analytics.alerts`.
Les mesures sont conservees 30 jours. Les compteurs portent sur :

- les messages Outbox en attente et leur age ;
- les evenements publies mais non integres ;
- le vrai retard Kafka : fin des partitions moins positions confirmees du
  groupe `ledgerflow-analytics-v1` ; une valeur -1 signifie inconnu ;
- les messages rejetes non resolus ;
- les batchs echoues dans les dernieres 24 heures et ceux encore en cours ;
- l'absence de publication pour la veille apres 04 h UTC.

Seuils par defaut : 100 messages ou 120 secondes pour l'Outbox ; 100 evenements
pour les compteurs de retard. Une panne Kafka, une DLT non vide ou un batch
echoue declenche egalement une alerte. Le moniteur emet une notification au
passage en alerte puis au retour a la normale, sans repeter les etats inchanges.
Un envoi echoue est reessaye au prochain controle. Comme tout envoi externe,
une panne entre reception et enregistrement de l'acquittement peut produire un
doublon de notification.

Grafana possede en plus un controle independant : plus de 90 secondes sans
mesure du moniteur, maintenu une minute, declenche une alerte. Une erreur SQL
declenche aussi cette alerte. Grafana doit naturellement rester disponible.

**Par defaut, les emails arrivent dans Mailpit**, visible sur le poste local,
pas dans une vraie boite mail. Pour recevoir des emails externes, renseigner
SMTP_HOST, SMTP_PORT, SMTP_USERNAME, SMTP_PASSWORD, SMTP_STARTTLS=true et
ALERT_EMAIL_TO dans `.env.ops`, puis recreer les services. Le moniteur peut aussi
appeler ALERT_WEBHOOK_URL avec un JSON contenant `text`, `alert`, `active` et
`message`. Aucun solde ni contenu d'evenement bancaire n'est inclus.

## 3. Reprise des incidents

### Batch arrete brutalement

Une interruption brutale peut laisser un run RUNNING et ses tables provisoires.
Le moniteur ne le declare abandonne que si :

1. il peut prendre le meme verrou PostgreSQL que le batch ;
2. le run a depasse ABANDONED_RUN_SECONDS (2 heures par defaut, minimum 60 s).

Il marque alors FAILED / AbandonedRun et nettoie seulement le snapshot/staging
de ce run. Les syntheses publiees ne sont pas supprimees. Airflow gere ses retries ;
si ceux-ci sont epuises, relancer la date concernee depuis son interface.
La recuperation manuelle utilise exactement les memes protections :

```powershell
.\scripts\platform.ps1 exec monitor python3 -m ledgerflow_ops recover
```

### Message rejete dans Kafka

Le moniteur archive la DLT dans `analytics.dead_letters` **avant** de confirmer
sa position Kafka. Un arret entre les deux ne cree pas deux lignes : le couple
topic/partition/offset est unique. La DLT conserve son propre historique selon
la retention Kafka ; l'archive SQL n'efface pas le message original.

Lister les 100 derniers messages, sans afficher leurs contenus bancaires :

```powershell
.\scripts\platform.ps1 exec monitor python3 -m ledgerflow_ops list-dlt
```

Rejouer un message apres correction du probleme :

```powershell
.\scripts\platform.ps1 exec monitor python3 -m ledgerflow_ops replay --id UUID_DU_MESSAGE
```

Pour corriger explicitement un contenu invalide, preparer un fichier JSON
conforme au contrat v1, le copier dans le conteneur, puis :

```powershell
.\scripts\platform.ps1 cp .\corrected-event.json monitor:/tmp/corrected-event.json
.\scripts\platform.ps1 exec monitor python3 -m ledgerflow_ops replay --id UUID_DU_MESSAGE --replacement /tmp/corrected-event.json
```

L'original reste inchange. La correction et le nombre de rejeux sont traces.
OPEN signifie a examiner ; REPLAYED signifie accuse de reception Kafka recu ;
RESOLVED exige un fait analytique correspondant au contenu rejoue. Un meme ID
avec un contenu different deja accepte reste un conflit : le rejeu ne remplace
pas un fait existant. Ne pas inventer une nouvelle operation pour contourner
cette protection. Aucun rejeu automatique en boucle des messages invalides.

Apres un rejeu historique reussi, recalculer sa date pour actualiser la synthese.

## 4. Graphiques Grafana

Deux tableaux sont livres automatiquement dans le dossier LedgerFlow :

- **Activite bancaire** : nombre et montants par jour et type d'operation,
  choix de la devise, details publies et historique des calculs.
- **Surveillance et incidents** : attente Outbox, retard Kafka, evenements non
  integres, age du dernier controle, alertes et identifiants DLT.

Les montants de devises differentes ne sont jamais additionnes. Les ouvertures
sont distinguees des depots et les virements representent un volume transfere,
pas un benefice. Les graphiques lisent les syntheses publiees : sans batch,
ils peuvent etre vides. Une table de controle peut donc etre a jour alors que
le graphique metier attend sa prochaine publication.

Grafana utilise `ledgerflow_dashboard`, un role SQL avec SELECT seulement sur
les tables de synthese/suivi et une vue DLT sans contenu de message. Il n'a pas
acces aux soldes ou aux tables bancaires. Les valeurs monetaires exactes restent
en NUMERIC ; les series graphiques utilisent une conversion de presentation.

## Tests et arret

Les nouveaux tests `data-pipeline/ops_tests` couvrent les entrees du service,
la protection des runs actifs, le nettoyage sans perte de publication, les
notifications de transition et les erreurs d'envoi, l'archive DLT et le rejeu
avec accusé Kafka puis resolution apres ingestion.

Les tests PostgreSQL exigent OPS_INTEGRATION=1 et une base jetable suffixee
`_ops_test`. Ils reinitialisent uniquement cette base. Le job CI `operations`
les execute independamment des tests Spark. Consulter `validation.md` pour les
verifications effectivement realisees, distinctes des scenarios disponibles.

```powershell
.\scripts\platform.ps1 logs --tail 50 monitor airflow
.\scripts\platform.ps1 down
```

`down` arrete la plateforme et conserve ses volumes. Ne pas ajouter `-v` si
les comptes, historiques, messages et configurations doivent etre conserves.

Sources officielles : [Airflow standalone](https://airflow.apache.org/docs/apache-airflow/3.1.7/start.html),
[timetables Airflow](https://airflow.apache.org/docs/apache-airflow/3.1.4/authoring-and-scheduling/timetable.html),
[PostgreSQL dans Grafana](https://grafana.com/docs/grafana/latest/datasources/postgres/configure/),
[provisionnement des alertes](https://grafana.com/docs/grafana/latest/alerting/set-up/provision-alerting-resources/file-provisioning/).
