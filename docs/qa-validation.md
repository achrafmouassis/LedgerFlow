# Validation du nouvel outillage QA — 16 octobre 2026

## Résultat final : campagne complète réussie

Après libération d'espace disque (24,91 Go libres au contrôle) et redémarrage
autorisé de Docker, la commande `.\scripts\qa.ps1 -Faults` s'est terminée avec
le code 0. Les blocages décrits plus bas sont l'historique des premières tentatives.

- Campagne principale `20261016T183029-d9c8f60d` : **70 vérifications PASS**,
  aucun FAIL ni SKIP ; 441 demandes HTTP enregistrées, dont 360 demandes de charge.
- Panne Kafka `20261016T183142-f78448b8` : **2 vérifications PASS**.
- Reprise `20261016T183148-eb066467` : **2 vérifications PASS**.
- Tests unitaires du générateur : **7 tests réussis**, distincts des 74 vérifications
  d'intégration ci-dessus.
- Deux batchs Spark SUCCEEDED : 386 événements source, 12 groupes ; résultats
  compte/somme/min/max identiques à SQL et stables après relance.
- Les soldes correspondent au journal et aux effets attendus ; chaque événement
  Outbox a été publié et reçu avec le contenu attendu. Rattrapage final mesuré :
  14,11 secondes après la phase de charge.
- Kafka QA arrêté : dépôt accepté et événement conservé non publié. Après reprise :
  réception analytique unique ; nouvelle tentative avec la même clé sans double effet.
- Contrôle final PostgreSQL QA après reprise : 21 comptes, 387 journaux,
  387 faits analytiques et 0 événement Outbox en attente. Les backends, bases
  et brokers principaux/QA sont sains au dernier inventaire Docker.

### Performance observée sur cette machine

| Demandes simultanées | Demandes | Erreurs inattendues | Débit observé | p95 |
| --- | --- | --- | --- | --- |
| 1 | 120 | 0 | 17,14 requêtes/s | 78,15 ms |
| 4 | 120 | 0 | 30,73 requêtes/s | 126,40 ms |
| 8 | 120 | 0 | 31,69 requêtes/s | 173,07 ms |

Budget p95 choisi : 2 000 ms. Il s'agit d'un petit essai local en boucle fermée,
pas d'une mesure de capacité maximale ni d'une preuve de scalabilité horizontale.
Les services de l'application principale et d'autres conteneurs partageaient
la machine. Les tests longue durée et multi-instances restent à faire.

### Rapports et données de démonstration

Les trois dossiers ci-dessus se trouvent dans `.work/qa/` et contiennent leurs
rapports HTML/JSON ; la campagne principale contient également les demandes
JSONL et `bi_daily_operations.csv`. Le CSV et les deux batchs correspondent à
l'état **avant** le dépôt supplémentaire du test de panne : c'est un instantané,
pas un export actualisé en continu. Un prochain batch intégrera ce dépôt.

L'ancien volume QA `ledgerflow-qa_qa-postgres` avait un fichier `global/pg_control`
vide après l'incident disque. Il est conservé, sans effacement. La configuration
QA utilise désormais `ledgerflow-qa_qa-postgres-recovered`. Le backend principal
a été remis en route et son contrôle de santé a renvoyé `UP` ; son pipeline a
renvoyé pending=0, published=0, processed=0. Aucun jeu QA n'a été injecté dans la
base principale. Les conteneurs QA et leurs données restent disponibles.

## Vérifié localement

- Sept tests unitaires du générateur réussis : isolation obligatoire, refus de la base principale,
  percentiles, demandes réseau ambiguës, conservation des erreurs HTTP,
  précision décimale et rapport qui conserve les échecs et échappe le HTML.
- Analyse syntaxique du script PowerShell.
- Validation locale de la configuration Compose et des fichiers YAML.
- `git diff --check`.

## Non validé en intégration lors de cette création

La tentative de campagne Docker a été interrompue car le moteur Docker ne
répondait pas. Un ping HTTP `/_ping` sur le point d'accès nommé
`dockerDesktopLinuxEngine` a expiré après cinq secondes. Les commandes Docker
de démarrage et d'inventaire restaient également sans réponse.

L'API principale répondait ; elle n'a pas servi de cible de charge ni de panne.
Aucun redémarrage du moteur, arrêt de l'application principale ou effacement
de données n'a été effectué.

Il n'y a donc pas encore de mesure de débit/p95 validée, ni de confirmation
de réussite de la nouvelle campagne complète, des deux batchs ou de la panne
Kafka QA. Les anciennes validations des modules restent distinctes de cette
nouvelle campagne. Exécuter `scripts/qa.ps1` après rétablissement du moteur et
examiner les rapports PASS/FAIL/SKIP avant toute conclusion.

## Nouvelle tentative après redémarrage autorisé

Docker Desktop a été redémarré avec l'accord de l'utilisateur. Le moteur a
répondu (version 29.8.0), les services persistants ont redémarré et les conteneurs
PostgreSQL/Kafka/backend QA ont été créés dans le projet isolé `ledgerflow-qa`.

Le moteur s'est ensuite arrêté de lui-même. Le journal Docker indique à
18:02:56 UTC : `engine linux/wsl shutdown requested`, avec pour cause une erreur
d'écriture du journal VM : `There is not enough space on the disk.`
La mesure Windows suivante ne montrait plus que 0,93 Go libres sur C:.

La campagne et la remise en route du backend principal ont été interrompues.
Les tests HTTP, les batchs et la panne Kafka QA n'ont pas démarré : aucun résultat
de performance ne peut être annoncé. Aucun volume ni fichier utilisateur n'a
été supprimé. Il faut libérer de l'espace disque, puis remettre Docker et les
services en route avant de relancer la campagne. L'application principale peut
rester indisponible tant que le moteur est arrêté.
