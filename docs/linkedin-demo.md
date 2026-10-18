# Présenter LedgerFlow honnêtement sur LinkedIn

## Texte de publication prêt à adapter

J'ai construit **LedgerFlow**, une démonstration de plateforme bancaire et data
pour relier trois compétences : génie logiciel backend, traitement des données
et exploitation d'un pipeline.

Une opération arrive dans une API Spring Boot, est enregistrée dans PostgreSQL
avec son journal et un événement Outbox dans la même transaction. Cet événement
part ensuite vers Kafka. Un traitement évite de compter deux fois le même
message ; PySpark calcule les synthèses quotidiennes, Airflow organise leur
exécution, et Grafana permet de suivre les incidents. Un front authentifié
permet de parcourir les comptes, opérations et résultats.

J'ai testé des pannes volontairement dans un environnement QA séparé : arrêt
brutal du backend, indisponibilité de PostgreSQL, relecture d'un message rejeté
et interruption d'un calcul Spark. Les scénarios contrôlés ont repris sans
doublon financier détecté. Deux campagnes stabilisées avec un, deux et trois
backends ont chacune accepté 2 400 demandes en une minute, sans perte ni erreur
et avec une latence p95 inférieure à 0,54 seconde. Le pipeline a ensuite mis de
32 à 133 secondes pour finir de traiter son retard. C'est une mesure locale,
pas une promesse de capacité de production.

Les données sont **synthétiques** ; le projet tourne localement et n'est pas
un système bancaire de production. Mon objectif est de montrer le parcours
complet d'une donnée et la manière de vérifier sa cohérence, y compris quand
un composant tombe en panne.

#Backend #DataEngineering #Java #Kafka #PostgreSQL

Avant publication, ajouter un lien vers le dépôt public seulement après avoir
vérifié qu'aucun secret ni fichier `.work/` n'est suivi par Git. Ne pas inclure
Power BI parmi les visuels démontrés tant que l'actualisation dans Desktop n'a
pas été confirmée.

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
7. Terminer par les rapports QA réels : budget stabilisé réussi, délai de vidage
   du pipeline, et essai à froid échoué après redémarrage des dépendances.

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
mesures effectivement obtenues et la configuration. La réussite à 40 demandes/s
ne doit pas être transformée en affirmation de capacité maximale. L'essai à froid
échoué et le retard du pipeline sont des limites à expliquer.
Le front HTTP et les fichiers Power BI peuvent être validés sans que leur rendu
ait été inspecté visuellement : préciser cette limite tant que ce n'est pas fait.

## Avant une vraie mise en ligne

HTTPS, secrets externes, contrôle des ports, rôles SQL minimaux, sauvegarde et
restauration testées, limites de débit au proxy et revue de sécurité. Publier une
vidéo ou des captures n'exige pas de rendre l'API bancaire accessible sur Internet.
Aucune publication LinkedIn ni exposition publique n'est effectuée par ces scripts.
