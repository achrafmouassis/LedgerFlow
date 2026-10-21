# État vérifié du projet — 18 octobre 2026

Cette page sépare les fonctions présentes dans le code, les contrôles déjà
effectués et les validations qui restent à terminer. Les données bancaires de
la démonstration sont **synthétiques**. Les rapports détaillés et les identifiants
locaux restent dans `.work/`, ignoré par Git.

## Ce qui fonctionne et a été vérifié

- La base principale contient 120 comptes de démonstration, 6 120 écritures,
  6 120 faits analytiques et 12 groupes de synthèse. Le 17 octobre, la requête
  de contrôle trouvait zéro événement Outbox en attente, quatre batchs
  `SUCCEEDED` et zéro message rejeté non résolu. Voir
  [l'injection et ses contrôles](demo-data-validation.md).
- L'API dispose d'une connexion par token, de rôles administrateur, opérateur,
  analyste et client, et d'une vérification serveur des comptes attribués.
  L'interface locale montre les données renvoyées par l'API. Après le nouveau
  déploiement, le contrôle HTTP en lecture seule de l'interface et des
  permissions contient 21 vérifications réussies.
  Voir [interface et sécurité](security-and-interface.md).
- Le 18 octobre, la suite Java du dernier code a terminé avec **53 tests réussis,
  zéro échec et zéro test ignoré** sur PostgreSQL 18 et Kafka réels. Les sept
  migrations Flyway ont été rejouées dans une base séparée
  `ledgerflow_java_test`. Les 13 tests unitaires du générateur QA réussissent
  également.
- Le projet Power BI contient un rapport natif vérifié au niveau des fichiers,
  du modèle et des droits SQL de lecture. L'actualisation PostgreSQL et le rendu
  de la page **Activité bancaire** ont été confirmés dans Desktop : 6 120
  opérations et les douze synthèses sont affichées. La seconde page reste
  vérifiée au niveau de sa définition, sans capture visuelle conservée. Voir
  [Power BI](power-bi.md).

## Charge et pannes : résultats actuels

Le générateur QA regroupe ses écritures de journal dans un tampon borné et la
base/Kafka ne sont plus redémarrés entre les tailles du backend. Deux campagnes
stabilisées ont proposé 40 demandes par seconde pendant 60 secondes, d'abord
avec 1, 2, 3 instances, puis dans l'ordre inverse 3, 2, 1. Chaque essai a traité
les 2 400 demandes sans perte ni erreur, avec un p95 inférieur à deux secondes :

| Instances | Terminées | Erreurs/pertes | Débit réalisé | p95 aller-retour, deux passages | Vidage du pipeline |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 2 400/2 400 | 0/0 | 40,01/s | 132 ms / 234 ms | 133 s / 102 s |
| 2 | 2 400/2 400 | 0/0 | 40,00/s | 367 ms / 391 ms | 32 s / 47 s |
| 3 | 2 400/2 400 | 0/0 | 39,99/s | 537 ms / 471 ms | 40 s / 44 s |

Cela prouve le budget de **40 demandes/s pendant une minute dans cet environnement
local stabilisé**, ainsi que la distribution exacte entre les instances. Cela ne
prouve pas une capacité de production ni un gain horizontal : une instance avait
la meilleure latence HTTP et PostgreSQL/Kafka restent partagés. Un passage à froid
a aussi échoué après des redémarrages de PostgreSQL ; démarrage à froid et régime
stabilisé doivent donc être présentés séparément. Rapports bruts :
`.work/qa/20261018T163612-f7465ed4/` à
`.work/qa/20261018T165016-9f994d59/`.

La campagne de pannes complète a réussi sur la base isolée `ledgerflow_qa` :
redémarrage après arrêt brutal du backend, indisponibilité puis reprise de
PostgreSQL, correction et relecture d'un message rejeté, et reprise d'un batch
Spark interrompu. Le dépôt rejoué n'a eu qu'un effet financier ; la panne DB a
renvoyé 503 sans faux succès ; le batch interrompu n'a pas publié de résumé
partiel. Les 33 contrôles des six rapports sont tous `PASS`, ainsi que la sonde
DB à 503. Voir [charge et incidents](scalability-and-incidents.md).

## État d'exploitation et prochaines étapes

1. Limiter le port PostgreSQL à `127.0.0.1` en développement : la configuration
   actuelle publie `5432` sur toutes les interfaces, avec un mot de passe local.
   Avant une exposition publique, ajouter HTTPS et une gestion de secrets adaptée.
2. Ajuster le contrôle de santé Kafka : le broker répond aux commandes, mais le
   contrôle Docker actuel dépasse régulièrement son délai de cinq secondes et
   affiche `unhealthy`.
3. Pour aller au-delà de la preuve locale, exécuter un test de plusieurs heures,
   répété sur des machines séparées, et fixer un budget pour le délai de vidage
   du pipeline Kafka.
4. Conserver une capture de la seconde page Power BI si elle doit apparaître
   dans une présentation publique. Le front a été parcouru sur ordinateur et dans ses cinq vues à
   une largeur mobile de 390 px ; le débordement du formulaire mobile a été
   corrigé puis recontrôlé après déploiement (aucun défilement horizontal de
   page sur les cinq vues).

Le projet est une démonstration locale de backend et de data engineering, pas
un service bancaire prêt pour une mise en ligne. Une présentation publique du
code ou une vidéo de démonstration peut utiliser les données fictives sans
publier les mots de passe, tokens, fichiers `.env` ou rapports privés.
