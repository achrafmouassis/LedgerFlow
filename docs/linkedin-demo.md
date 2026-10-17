# Présenter LedgerFlow honnêtement sur LinkedIn

## Démonstration courte (60 à 90 secondes)

1. Se connecter avant d'enregistrer l'écran, avec le profil opérateur. Ne pas
   filmer le mot de passe, un token, un fichier `.env` ou `local-access.json`.
2. Vue d'ensemble : 120 comptes et 6 000 opérations fictives, plus 120 ouvertures.
   Les nombres affichés sont lus dans l'API, pas inscrits en dur dans le front.
3. Répertoire des comptes et journal : montrer les deux écritures d'un mouvement.
4. Faire un petit virement de démonstration, puis réutiliser la même clé : même
   résultat et aucun deuxième effet. Cette action change les données synthétiques
   et n'est pas nécessaire pour simplement parcourir la démonstration.
5. Pipeline : expliquer la différence entre « publié » et « traité », puis ouvrir
   les résultats du batch et Grafana. Power BI est un import à actualiser.
6. Profil client : montrer un seul compte attribué ; un accès à un autre compte
   est refusé par le serveur, pas seulement masqué dans l'interface.
7. Terminer par les rapports QA réels, y compris les budgets de charge échoués.

## Ce qui peut être affirmé

Backend Spring Boot / PostgreSQL, journal bancaire atomique et idempotent,
Outbox transactionnelle, Kafka et réception analytique dédupliquée, calculs
PySpark réconciliés en SQL, Airflow, surveillance, interface authentifiée et
permissions explicites. Le projet relie génie logiciel backend et data engineering.

Dire **données synthétiques réalistes**, jamais « données bancaires réelles ».
Kafka est un broker local, Spark fonctionne en local[2], et les essais backend
multi-instances partagent une seule machine. Ce n'est ni un déploiement bancaire
de production ni une preuve de haute disponibilité du cluster Kafka.

Le code de comparaison ne prouve pas à lui seul un gain de capacité : citer les
mesures effectivement obtenues et la configuration. Un budget échoué est une
limite mesurée à expliquer, pas un résultat à omettre ou une transaction perdue.
Le front HTTP et les fichiers Power BI peuvent être validés sans que leur rendu
ait été inspecté visuellement : préciser cette limite tant que ce n'est pas fait.

## Avant une vraie mise en ligne

HTTPS, secrets externes, contrôle des ports, rôles SQL minimaux, sauvegarde et
restauration testées, limites de débit au proxy et revue de sécurité. Publier une
vidéo ou des captures n'exige pas de rendre l'API bancaire accessible sur Internet.
Aucune publication LinkedIn ni exposition publique n'est effectuée par ces scripts.
