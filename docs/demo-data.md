# Données fictives réalistes dans l'application principale

Ce jeu est une démonstration autorisée, **pas des transactions bancaires réelles**.
Résultats effectivement constatés : [validation de l'injection](demo-data-validation.md).
Il est distinct de la QA isolée : les comptes et les événements sont créés dans
la base principale `ledgerflow`, uniquement via l'API bancaire normale.
Il ne faut pas utiliser ce générateur sur un environnement bancaire de production.

## Plan et précautions

Le jeu `demo-20261016-v1` prépare 120 comptes en MAD, EUR et USD, particuliers
et commerces fictifs, puis 6 000 opérations. Les comptes ont un propriétaire
commençant par `DEMO | demo-20261016-v1`. Les scénarios de revenu, retrait et
achat sont conservés dans le manifeste, pas dans une colonne métier inexistante.

Les ouvertures ont des montants plausibles selon la devise. Les montants suivants
sont exprimés en centimes entiers, jamais calculés avec des nombres flottants.
Avant la moindre création, le programme vérifie : clés uniques, montants positifs,
devises compatibles, comptes différents pour les virements, absence de découvert,
soldes finaux et conservation des fonds par devise (ouvertures + dépôts − retraits).

Si un compte manque de provision dans le scénario, une opération prévue devient
un approvisionnement explicite. Le programme ne modifie jamais directement un
solde ni une date en SQL. Les dates sont les dates réelles d'exécution UTC : ce
jeu remplit les graphiques d'aujourd'hui, pas une courbe historique sur 30 jours.

## Exécuter ou reprendre

Docker et les services principaux doivent être sains. Au moins 5 Gio d'espace
libre sur le disque du projet, et suffisamment d'espace sur le disque Docker.

```powershell
# Analyse et manifeste seulement : aucune création dans la base.
.\scripts\demo-data.ps1

# Créations via API puis vérifications et batch Spark.
.\scripts\demo-data.ps1 -Apply
```

Par défaut, les opérations sont limitées à 20 demandes/s (ce n'est pas un test
de capacité). Les données préexistantes ne sont pas effacées. La configuration
`compose.demo.yaml` rejoint le réseau principal, jamais le réseau QA.

En cas d'interruption, **relancer avec le même Dataset et les mêmes paramètres**.
Les propriétaires déterministes permettent de retrouver les comptes ; les clés
d'idempotence permettent de retrouver et comparer chaque opération déjà validée.
Une création de compte sans réponse arrête le programme au lieu d'être rejouée
aveuglément. Les demandes suivantes réutilisent les mêmes clés. Un verrou SQL
empêche deux générateurs de démonstration d'agir simultanément.

Le contenu du plan ne peut pas changer après son enregistrement. Changer le nom
du jeu crée volontairement un autre jeu de comptes : ne pas le faire pour tenter
de reprendre une injection interrompue. Une relance normale conserve les mêmes
comptes et opérations ; seul un nouveau batch peut être ajouté à son historique.

## Contrôles après injection

1. Chaque solde égale le solde prévu avant injection.
2. Chaque clé, type, compte, montant et devise du plan égale son écriture comptable.
3. Tous les soldes de l'application peuvent être reconstruits depuis le journal.
4. Chaque journal possède son événement Outbox et un contenu identique.
5. Chaque événement est publié, reçu et traité avec un contenu identique ; aucun
   événement manquant, supplémentaire ou en attente après rattrapage (délai 10 min).
6. Aucun solde négatif.
7. Le batch Spark est exécuté pour les dates concernées.
8. Nombre, somme, minimum et maximum des résumés sont comparés à une requête SQL
   indépendante. La vue BI expose les groupes publiés.

Le programme ne déclenche ni panne ni demande volontairement invalide sur la
base principale. Ces scénarios restent réservés à `scripts/qa.ps1`.

## Fichiers et graphiques

Dans `.work/demo-data/demo-20261016-v1/` :

- `plan.json` : comptes, opérations, scénarios et soldes attendus.
- `baseline.json` : état des comptes avant la première injection.
- `requests.jsonl` : demandes et réponses HTTP conservées pour diagnostic.
- `verification.json` et `verification.html` : résultats des contrôles.

Le tableau métier Grafana interroge les résumés publiés ; sélectionner une
période incluant aujourd'hui UTC. Power BI utilise un import : lancer
**Actualiser** dans Desktop pour charger les nouvelles données. Aucun rendu ou
rafraîchissement Power BI Desktop n'est garanti par les seules vérifications SQL.

Il n'y a pas d'effacement automatique : ne pas supprimer simplement des comptes
qui possèdent un journal et un historique. Un éventuel nettoyage doit être
autorisé et conçu séparément. Les rapports sont locaux et ignorés par Git.
