# Tests multi-instances et incidents étendus

Ces commandes ciblent exclusivement le projet Docker `ledgerflow-qa`, sa base
`ledgerflow_qa` et son Kafka. Aucun socket Docker n'est monté dans les conteneurs.
Les opérations destructives sur les processus sont orchestrées depuis PowerShell.
Les données principales ne servent pas de cible de panne ou de charge.

## Comparer 1, 2 et 3 instances

```powershell
.\scripts\qa-scalability.ps1 -Seconds 60 -Rate 40
```

Trois essais successifs, débit d'arrivée imposé, 1 CPU et 768 Mio par instance,
heap Java fixé à 384 Mio. Nginx répartit les demandes sans affinité de session.
Le test vérifie qu'un même token est accepté par tous les noms d'instances attendus.
Il crée 48 comptes de charge distincts par essai pour limiter la contention artificielle
sur un seul compte, puis mesure les dépôts à montant exact et clés uniques.

Le générateur borne ses demandes en attente. Une arrivée qu'il ne peut émettre
n'est pas comptée comme une transaction acceptée : elle est enregistrée comme
`dropped_arrivals` et fait échouer le budget. Il mesure aussi la durée depuis
l'heure d'arrivée prévue, et pas uniquement le temps HTTP après émission. Un
ralentissement ne se transforme donc pas silencieusement en débit plus faible.

Rapports par essai dans `.work/qa/<run>/scalability.json` et `report.html` : débit
proposé/réalisé, arrivées non émises, erreurs HTTP, p95 HTTP et p95 depuis arrivée.
Après la charge, rapprochement des soldes, journaux, événements et statistiques.
Les assertions de charge échouées restent visibles ; le script tente quand même
les configurations suivantes. Les erreurs d'infrastructure arrêtent la campagne.

Les instances tournent sur la même machine, avec un PostgreSQL et un seul broker
Kafka partagés. Cela teste la répartition et la cohérence à plusieurs processus,
pas la tolérance à la perte d'un serveur physique ni la haute disponibilité Kafka.
Un essai de 60 secondes n'est pas un soak test de plusieurs heures. Pour une
mesure propre, stabiliser les services, arrêter les compilations concurrentes,
utiliser un générateur séparé, répéter trois fois et comparer les mêmes paramètres.

## Pannes étendues

```powershell
.\scripts\qa-incidents.ps1
```

- Dépôt validé, SIGKILL du backend QA, redémarrage puis même demande : même
  opération et un seul effet financier. Ce test est après commit confirmé ; il
  ne prétend pas tuer exactement entre commit SQL et émission de la réponse HTTP.
- PostgreSQL QA arrêté : une écriture authentifiée renvoie 503, pas un faux succès.
  Après reprise, absence de journal pour la demande échouée puis tentative avec
  la même clé : un seul dépôt.
- Message Kafka avec version inconnue, réception dans la vraie DLT, capture durable,
  correction explicite et relecture ; original conservé, statut résolu, fait unique.
  Le message corrigé représente volontairement une opération déjà enregistrée
  pour tester la déduplication sur la relecture.
- Vrai batch Spark arrêté par SIGKILL après état RUNNING. Un verrou de table
  de staging QA retient le batch afin de rendre la fenêtre de panne observable.
  L'ancien résumé ne doit pas changer. Libération du verrou, vieillissement du
  seul horodatage technique QA pour éviter deux heures d'attente, récupération
  AbandonedRun sous verrou officiel, nettoyage du snapshot/staging et nouveau
  batch réussi comparé à SQL.

La préparation et chaque contrôle produisent un rapport, sauf la sonde DB arrêtée
qui produit `db-outage-result.json`. Les tokens et l'état privé sont conservés dans
`.work/qa-private`, hors dossier de rapports exportable. Les conteneurs de panne
arrêtés sont conservés pour diagnostic. Le marqueur stage-lock-ready évite une
relance ambiguë : examiner le précédent essai avant de le réinitialiser.

Les blocs finally tentent de redémarrer le service interrompu ou de libérer le
verrou. En cas d'arrêt forcé du terminal ou panne hôte, vérifier manuellement les
conteneurs QA. Ne pas utiliser cette procédure contre la base principale.
