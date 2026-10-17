# Injection de démonstration validée — 16 octobre 2026

## Périmètre

Jeu `demo-20261016-v1`, **entièrement fictif**, injecté avec l'autorisation de
l'utilisateur dans la base principale `ledgerflow`. Ce n'est pas la base QA.
La base principale contenait 0 compte, 0 journal et 0 fait avant l'opération.
Son état initial est conservé dans `.work/demo-data/demo-20261016-v1/baseline.json`.
Aucune suppression ni modification de dates comptables en SQL.

## Analyse avant injection

- Plan déterministe validé avant toute écriture : 120 comptes, 6 000 opérations.
- 60 comptes MAD, 36 EUR, 24 USD ; particuliers et commerces marqués `DEMO`.
- 2 988 virements, 1 531 dépôts, 1 481 retraits.
- Clés uniques, montants strictement positifs et exacts en centimes.
- Pas de transfert entre devises différentes ou vers le même compte.
- Simulation intégrale : aucun découvert, reconstruction des soldes et
  conservation des fonds vérifiées séparément par devise.
- 12 tests unitaires réussis : 5 tests du plan et 7 tests du générateur QA existant.
- Empreinte SHA-256 du plan :
  `9e08be79011defb5ea46ec3f41642fe6a991a50efffe5eccf6f75d6c59209784`.

## Injection et contrôles réels

La première exécution `--apply --confirm-database ledgerflow` puis la relance
`scripts/demo-data.ps1 -Apply` se sont terminées avec le code 0.
Les 10 contrôles d'intégrité du générateur ont réussi à chaque exécution.

| Élément | Total final |
| --- | --- |
| Comptes | 120 |
| Opérations hors ouverture | 6 000 |
| Écritures d'ouverture | 120 |
| Journaux | 6 120 |
| Événements Outbox | 6 120 |
| Faits analytiques reçus | 6 120 |
| Événements en attente | 0 |
| Opérations résumées par Spark | 6 120 |
| Groupes statistiques / vue BI | 12 |

Les 6 120 demandes de création ont reçu leur réponse attendue : 120 HTTP 201
et 6 000 HTTP 200 ; aucun rejet ni réponse réseau ambiguë. La relance du même
jeu n'a ajouté aucune demande de création, aucun compte et aucune opération.
Elle a ajouté seulement une nouvelle exécution Spark à l'historique.

Chaque solde final correspond au montant simulé avant injection, chaque journal
à son opération prévue, chaque événement reçu à son événement source. Les dates
analytiques UTC ont également été vérifiées à la relance. Aucun solde négatif.
Le premier rattrapage final du pipeline a pris 20,08 secondes.

Deux batchs Spark SUCCEEDED ont chacun traité 6 120 faits et publié 12 groupes.
Les nombres, sommes, minimums et maximums sont égaux aux agrégats SQL indépendants.
Le dernier run est `7ed50a7e-7acf-42dc-be63-16de43f43831`.

Les **12 requêtes de panneaux Grafana** ont été exécutées via son API SQL sans
erreur après injection. Cela valide leurs requêtes, pas une inspection visuelle
de chaque panneau. Power BI Desktop doit encore être actualisé par l'utilisateur.

## Consultation

- Métier : `http://localhost:3000/d/ledgerflow-business` ; période incluant
  le 16 octobre 2026 UTC, choisir une seule devise pour les montants.
- Rapport local : `.work/demo-data/demo-20261016-v1/verification.html`.
- Plan, demandes HTTP et résultats JSON : dans le même dossier, ignoré par Git.
- Reproduction et reprise : [demo-data.md](demo-data.md).

Les transactions portent leurs vraies dates d'exécution : ce jeu n'invente pas
d'historique sur plusieurs jours. Les statistiques d'aujourd'hui sont disponibles
dans la base et Grafana ; le rapport Power BI est en mode import et nécessite
un rafraîchissement. Ces données ne sont ni de vraies personnes ni de vrais fonds.
