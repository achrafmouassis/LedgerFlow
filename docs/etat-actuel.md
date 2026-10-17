# État vérifié du projet — 17 octobre 2026

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
  L'interface locale montre les données renvoyées par l'API. Le dernier contrôle
  HTTP de l'interface et des permissions contient 21 vérifications réussies.
  Voir [interface et sécurité](security-and-interface.md).
- Le 17 octobre, la suite Java du dernier code a terminé avec 53 tests, zéro
  échec et trois tests Kafka ignorés dans la configuration H2 locale. La suite
  précédente avec PostgreSQL et Kafka réels comptait 52 tests réussis, sans
  échec ni test ignoré ; elle doit être relancée avec les derniers changements.
  Les 12 tests unitaires du générateur QA et la vérification syntaxique du
  JavaScript ont également réussi le 17 octobre.
- Le projet Power BI contient un rapport natif vérifié au niveau des fichiers,
  du modèle et des droits SQL de lecture. Power BI Desktop a été ouvert, mais
  son actualisation, le rendu des graphiques et les filtres n'ont pas encore été
  confirmés après la demande d'identifiants PostgreSQL. Voir
  [Power BI](power-bi.md).

## Charge et pannes : résultats actuels

Les essais locaux proposaient 40 demandes par seconde pendant 60 secondes,
avec 1, 2 puis 3 instances du backend. Les opérations acceptées ont été
réconciliées ; aucune erreur HTTP inattendue n'a été comptée. Le budget de
charge a échoué dans les trois cas :

| Instances | Demandes proposées | Terminées | Arrivées abandonnées | p95 depuis l'arrivée prévue |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 2 400 | 1 244 | 1 156 | 9,34 s |
| 2 | 2 400 | 1 001 | 1 399 | 10,62 s |
| 3 | 2 400 | 1 182 | 1 218 | 9,94 s |

Le générateur ouvre et ferme son fichier de journalisation à chaque réponse
pendant la mesure. Cela peut limiter le débit d'émission sur ce poste Windows.
Il faut corriger ce point, refaire les essais dans des conditions stables et
conserver aussi les résultats échoués. Les chiffres ci-dessus ne prouvent pas
que trois instances sont plus rapides. Rapports bruts :
`.work/qa/20261016T224030-60fcc1a7/`,
`.work/qa/20261016T224515-e23cac7e/` et
`.work/qa/20261016T224838-6a162a45/`.

Les sondes de panne étendues (arrêt brutal du backend, indisponibilité de
PostgreSQL, relecture DLT et interruption de Spark) sont implémentées pour la
base isolée `ledgerflow_qa`, mais **leur campagne complète n'est pas encore
validée**. Voir [charge et incidents](scalability-and-incidents.md).

## État d'exploitation et prochaines étapes

1. Limiter le port PostgreSQL à `127.0.0.1` en développement : la configuration
   actuelle publie `5432` sur toutes les interfaces, avec un mot de passe local.
   Avant une exposition publique, ajouter HTTPS et une gestion de secrets adaptée.
2. Ajuster le contrôle de santé Kafka : le broker répond aux commandes, mais le
   contrôle Docker actuel dépasse régulièrement son délai de cinq secondes et
   affiche `unhealthy`.
3. Relancer les tests Java avec PostgreSQL et Kafka réels sur le dernier code,
   redéployer cette version de l'application et refaire les 21 contrôles HTTP.
4. Corriger la journalisation du générateur, répéter les trois essais de charge,
   puis exécuter et examiner la campagne de pannes isolées.
5. Actualiser Power BI avec le compte de lecture seule et vérifier les visuels
   et filtres. Parcourir également l'interface dans un navigateur sur ordinateur
   et téléphone : le rendu visuel n'a pas encore été validé.

Le projet est une démonstration locale de backend et de data engineering, pas
un service bancaire prêt pour une mise en ligne. Une présentation publique du
code ou une vidéo de démonstration peut utiliser les données fictives sans
publier les mots de passe, tokens, fichiers `.env` ou rapports privés.
