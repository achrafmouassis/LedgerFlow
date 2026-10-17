# QA : données fictives, erreurs, performance et incidents

## Objectif en mots simples

Vérifier que l'argent n'est ni perdu ni créé par erreur, qu'une demande répétée
ne passe qu'une fois, et que chaque opération arrive jusqu'aux statistiques.
Puis observer combien de demandes cette machine traite et comment elle reprend
après une panne. Un test qui échoue est un problème à examiner, pas à masquer.

Les données sont **réalistes mais entièrement fictives**, jamais des données
personnelles réelles. Tout est créé par l'API : comptes de particuliers et
commerçants de démonstration, dépôts, retraits, virements en MAD, EUR et USD.
Les grosses provisions des comptes de charge servent uniquement à éviter des
rejets pour manque de fonds pendant la mesure de performance.

## Lancer la campagne

Prérequis : Docker Desktop démarré, PowerShell, ressources suffisantes pour
PostgreSQL, Kafka, Java et Spark. La première construction nécessite Internet.
Le script refuse de démarrer si le disque du projet a moins de 5 Gio libres ;
prévoir davantage pour une campagne longue et surveiller aussi le disque Docker.
Depuis la racine du dépôt :

```powershell
# Campagne complète : fonctionnel, charge, rapprochement SQL, deux batchs Spark.
.\scripts\qa.ps1

# Premier contrôle rapide sans Spark.
.\scripts\qa.ps1 -Operations 30 -Concurrency '1,4' -SkipBatch

# Campagne plus longue et panne du Kafka DE TEST, suivie de sa remise en route.
.\scripts\qa.ps1 -Operations 1000 -Concurrency '1,4,8,16' -P95Ms 2000 -Faults
```

`Operations` = nombre de demandes **par palier**, en plus des créations et tests
fonctionnels. `Concurrency` = nombre maximum de demandes simultanées à chaque
palier. `P95Ms` = budget choisi : 95 % des demandes doivent finir dans ce délai.
Ce budget est un exemple de cible, pas une capacité garantie de LedgerFlow.
Les erreurs attendues des tests fonctionnels ne comptent pas comme erreurs de charge.

L'environnement porte le nom `ledgerflow-qa`, avec une base `ledgerflow_qa`,
un réseau et un volume PostgreSQL séparés. L'API QA écoute sur `localhost:18080`.
La base et Kafka QA ne publient aucun port hôte. Aucun arrêt ni effacement de
l'environnement principal n'est effectué. Ne pas fusionner `compose.qa.yaml`
avec les fichiers Compose de l'application. Le générateur refuse la base principale.
Il n'y a pas de montage du socket Docker dans les conteneurs.

Les données QA restent disponibles après les tests, pour inspection. Chaque
exécution crée de nouveaux comptes et clés ; les rapprochements SQL portent
sur toute la base QA. Ne pas lancer deux campagnes simultanément ni modifier
les données QA pendant un rapprochement. Les données historiques QA restent
dans les prochains résultats, ce n'est pas une remise à zéro automatique.

```powershell
# Lire les journaux QA uniquement.
docker compose -p ledgerflow-qa -f compose.qa.yaml logs --tail 100 backend kafka

# Arrêter la QA sans effacer ses données.
docker compose -p ledgerflow-qa -f compose.qa.yaml stop
```

## Voir les résultats

Dans `.work/qa/<identifiant-execution>/` :

- `report.html` : rapport lisible, résultats PASS/FAIL/SKIP et barres de latence.
- `report.json` : mêmes mesures pour automatisation.
- `requests.jsonl` : chaque demande, sa clé, son contenu, sa réponse et sa durée.
- `bi_daily_operations.csv` : export des résultats après Spark, ouvrable dans
  Power BI avec **Obtenir les données → Texte/CSV**. Aucun mélange de devises.

Un code de sortie non nul signifie échec. Un test non exécuté n'est pas validé.
Le JSON contient aussi les soldes attendus ; le journal des demandes indique
l'heure UTC et le palier de charge. Les tests unitaires du générateur ne
constituent pas une validation de l'application.
Une demande sans réponse est enregistrée avec statut `0` : elle est ambiguë,
car le serveur peut l'avoir validée avant la coupure. Son effet éventuel doit
être retrouvé grâce à sa clé, jamais ignoré ni rejoué avec une nouvelle clé.

Les opérations créées portent leurs **vraies dates d'exécution UTC**, généralement
aujourd'hui. Le générateur ne modifie pas artificiellement les dates comptables :
il ne produit donc pas une courbe historique sur 30 jours. Pour une telle courbe,
laisser tourner plusieurs jours ou concevoir un import de démonstration séparé.
Le rapport HTML est autonome ; il ne suppose pas Grafana ou Power BI installés.
Les tableaux de bord de l'application principale ne montrent pas ces données
isolées. Utiliser l'export CSV ou l'API QA pour les visualiser.

## Liste des tests

### Automatisés par la campagne normale

| Famille | Vérification attendue |
| --- | --- |
| Comptes | Création et lecture en trois devises |
| Dépôt / retrait / virement | Succès et soldes recalculés indépendamment |
| Doublon | Même clé + même demande : même identifiant et un seul effet |
| Collision de clé | Même clé + montant différent : HTTP 409, aucun effet |
| Requêtes invalides | Zéro, négatif, trop de décimales, dépassement de taille : HTTP 400 |
| Comptes invalides | Nom vide, devise incorrecte, solde initial négatif : HTTP 400 |
| Montant absent / null | HTTP 400 |
| Clé absente / incorrecte | HTTP 400, aucune opération créée |
| Compte inconnu | HTTP 404 |
| Règles métier | Solde insuffisant : 409 ; même compte / devises différentes : 400 |
| Pagination | Taille hors limites : 400 |
| Dates / identifiants | Période inversée et UUID incorrect : HTTP 400 |
| Rejets | Le nombre d'écritures comptables ne change pas |
| Concurrence / doublons | Huit demandes identiques simultanées : un seul effet |
| Concurrence / solde | Deux retraits de 80 sur un solde de 100 : un succès, un rejet, solde 20 |
| Performance | Plusieurs paliers ; débit observé, p50/p95/p99, erreurs inattendues |
| Comptabilité | Chaque solde correspond à son journal ; aucun solde négatif |
| Source → Outbox | Chaque journal a un événement, avec contenu identique |
| Outbox → analytique | Tous les identifiants sont publiés et reçus, contenu identique |
| Événements parasites | Aucun fait analytique sans événement source |
| API statistiques streaming | Nombres et montants égaux à SQL pour aujourd'hui UTC |
| Spark | Calculs réellement exécutés, compte/somme/min/max égaux à SQL |
| Relance Spark | Deux exécutions donnent exactement les mêmes résultats métiers |
| BI | La vue expose tous les groupes du résumé ; export CSV |

L'attente de rattrapage du pipeline est limitée à 180 secondes par défaut.
Un retard non résorbé est un échec du budget QA, pas nécessairement une perte.
Le manifeste et les rapprochements servent à distinguer perte, retard et corruption.
La comparaison des montants utilise `Decimal`/SQL numérique exact, pas les arrondis
affichés dans les graphiques.

### Automatisés avec `-Faults`

1. Arrêter uniquement le Kafka QA.
2. Faire un dépôt : l'API doit rester disponible et le journal doit être enregistré.
3. Vérifier que l'événement reste dans l'Outbox, non publié.
4. Redémarrer Kafka même si une assertion échoue.
5. Attendre la réception du montant attendu, exactement une fois dans l'analytique.
6. Rejouer la demande avec la même clé : le solde ne change pas.

Ces étapes produisent des rapports distincts `outage` et `recovery`. La charge
et la panne ne se chevauchent pas dans ce scénario. Un arrêt brutal de PowerShell
peut empêcher le `finally` : redémarrer alors explicitement le Kafka QA.

### Tests existants à exécuter aussi

La campagne HTTP complète les suites du dépôt, elle ne les remplace pas.
Consulter [validation](validation.md), [batch](batch-pyspark.md) et
[validation exploitation](operations-validation.md) pour leurs environnements
et commandes, notamment :

- Tests Java : transactions atomiques, rollback, transferts opposés sans blocage,
  plafond du solde, contraintes SQL, migrations, API et consommation Kafka.
- Tests Spark : snapshot, staging, réconciliation, doublons, publication atomique.
- Tests d'exploitation : capture/relecture DLT, verrou de batch, reprise d'un run
  abandonné, garde d'ingestion et notifications.
- Vérification des requêtes Grafana et validation des fichiers Power BI.

### Scénarios supplémentaires : à planifier, pas exécutés par ce script

| Scénario | Critère de réussite |
| --- | --- |
| Kafka envoie deux fois le même événement | Un seul fait ; pas de double comptage |
| Même ID événement, contenu contradictoire | Rejet visible, pas de remplacement silencieux |
| JSON Kafka invalide / version inconnue | Message isolé en DLT ; messages valides continuent |
| Relecture DLT corrigée | Événement traité, original conservé, statut résolu vérifié |
| Processus backend tué après commit SQL | Au redémarrage, Outbox livre sans perdre l'opération |
| Réponse HTTP perdue après commit | Nouvelle tentative avec même clé, un seul effet |
| PostgreSQL indisponible / stockage plein | Aucun faux succès ; reprise sans écriture partielle |
| Batch tué pendant snapshot/staging/publication | Ancien résumé cohérent ; run abandonné récupérable |
| Deux batchs sur la même période | Verrou actif ; aucune publication concurrente incohérente |
| Airflow : tâche échouée puis retry | Dates stables, tentatives limitées, résultat final traçable |
| Arrêt du monitor | Alerte externe de fraîcheur Grafana |
| Retard Kafka / échec batch | Alerte puis résolution ; pas de spam inchangé |
| SMTP / webhook indisponible | Échec visible et notification retentée |
| Minuit UTC / fin de mois / année bissextile | Bonne date, pas de double comptage entre jours |
| Opérations tardives | Relance des jours concernés, résumé mis à jour |
| Charge soutenue 30–60 min | Pas de croissance continue mémoire/connexions/retard |
| Pics soudains | Rejet explicite ou rattrapage dans le budget défini |
| Compte très sollicité | Mesurer contention séparément de comptes indépendants |
| 1 puis 2 puis 3 instances backend | Gain mesuré avec répartiteur et ressources comparables |
| Sauvegarde / restauration | Soldes, journaux, Outbox et faits toujours cohérents |
| Power BI Desktop | Import réel, filtres et devises, rendu et rafraîchissement corrects |
| Droits SQL BI | Lecture des vues autorisées, comptes et écritures refusés |
| Sécurité API bancaire | Authentification/autorisation à implémenter avant exposition publique |

Les pannes dangereuses (disque plein, base arrêtée, processus tués) doivent rester
dans un environnement jetable et ne sont volontairement pas déclenchées ici.

## Automatisation et état de validation

Le workflow GitHub contient un job `qa-end-to-end` : petite campagne isolée,
deux batchs Spark et rapports conservés en artefact. Son budget p95 est 5 secondes
pour les machines CI partagées ; le script local utilise 2 secondes par défaut.
Le job ne déclenche pas la panne Kafka. Il prendra effet au prochain push.

Pour les tests rapides du générateur, sans Docker :

```powershell
$env:PYTHONPATH='data-pipeline'
python -m unittest discover -s qa -p test_runner.py
```

État de la validation lors de la création : voir [qa-validation.md](qa-validation.md).
Si le moteur Docker ne répond pas, ne pas en déduire un échec de LedgerFlow ni un
succès des tests QA. Relancer la campagne une fois le moteur disponible. Un
redémarrage de Docker peut interrompre l'application principale : ce script ne
le fait jamais automatiquement.

## Interpréter la performance sans se tromper

Ce générateur utilise un modèle **fermé** : un nombre borné de demandes en vol.
Quand l'application ralentit, le débit émis baisse aussi. Il ne prouve pas qu'elle
supporte un débit d'arrivée imposé, ni qu'elle est scalable horizontalement.
Pour cette preuve : générateur sur une autre machine, charge d'arrivée constante,
plusieurs instances et répartiteur, puis comparaison à infrastructure égale.

Pendant les tests, observer aussi `docker stats`, CPU, mémoire, connexions SQL,
verrous, retard Kafka et temps de rattrapage. Le générateur, Spark et l'application
sur le même ordinateur se partagent les ressources : noter la configuration,
refaire trois fois chaque mesure et séparer charge API et calcul Spark.
La campagne actuelle lance Spark **après** la charge et le rattrapage.

Critère minimal proposé : zéro effet financier incorrect, zéro événement manquant
après rattrapage, zéro erreur inattendue en charge, p95 dans le budget choisi,
résultats batch identiques à SQL et reprise de panne démontrée. Des tests passés
sur un petit jeu de données ne valent pas une certification de production.
