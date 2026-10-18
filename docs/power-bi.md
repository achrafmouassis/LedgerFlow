# Power BI — activité bancaire LedgerFlow

Le projet natif se trouve dans `bi/powerbi/LedgerFlow.pbip`. Il contient le
rapport PBIR et le modèle de données TMDL sous forme de fichiers texte : ces
fichiers peuvent être suivis dans Git et ouverts dans Power BI Desktop.
Ce n'est pas une image de tableau de bord ni un fichier PBIX déjà actualisé.

## Ce que tu verras

**Page Activité bancaire** : filtres de date UTC, devise et type d'opération ;
nombre d'opérations ; montant total ; montant moyen ; évolution quotidienne ;
montants par type et tableau détaillé des synthèses.

**Page Actualisation des données** : dernière publication des données présentes
dans le modèle et historique des calculs, avec leurs états et erreurs.
Elle ne représente pas une surveillance en direct : Grafana conserve ce rôle.

## Ouvrir et charger les données

1. Démarrer Docker Desktop, puis depuis la racine du projet :

   ```powershell
   .\scripts\platform.ps1 up -d --build
   ```

2. Installer ou ouvrir une version récente de **Power BI Desktop standard**
   (pas l'édition optimisée pour Power BI Report Server). Selon la version,
   activer les options de projet PBIP/PBIR dans les fonctionnalités en aperçu,
   puis redémarrer Desktop si demandé.

3. Ouvrir `bi/powerbi/LedgerFlow.pbip`.

4. Dans **Transformer les données → Paramètres de source de données**, utiliser :

   | Paramètre | Valeur locale |
   | --- | --- |
   | Serveur | `localhost:5432` |
   | Base | `ledgerflow` |
   | Authentification | Base de données |
   | Utilisateur | `ledgerflow_bi` |
   | Mot de passe de développement | `ledgerflow-bi-local-password` |

   Si BI_DB_PASSWORD est personnalisé dans `.env.ops`, utiliser cette valeur.
   Le mot de passe est saisi dans Desktop, jamais enregistré dans le projet Git.
   Le PostgreSQL local ne configure pas TLS : accepter une connexion non
   chiffrée uniquement pour cet environnement local si Desktop le demande.

5. Cliquer **Actualiser**. Choisir une devise dans la première page, puis la
   période et le type d'opération souhaités.

Le mode est **Import** : Power BI conserve une copie lors de l'actualisation.
Un nouveau batch Airflow ne rafraîchit pas automatiquement un Desktop ouvert.
Il faut actualiser le modèle pour voir sa nouvelle publication.

Si les graphiques sont vides, vérifier qu'un batch a publié la période
sélectionnée. La base principale contient actuellement des opérations de
démonstration synthétiques, datées principalement du 16 octobre 2026 : un
filtre limité aux dernières 24 heures peut donc masquer ces résultats. Pour
publier une date à la demande :

```powershell
.\scripts\platform.ps1 exec airflow airflow dags trigger ledgerflow_daily_summary --conf '{"target_date":"2026-10-16","lookback_days":1}'
```

## Où le rapport prend ses données

```text
Opérations bancaires → Outbox → Kafka → faits analytiques
    → batch PySpark → synthèses publiées dans PostgreSQL
    → vues BI en lecture seule → actualisation Power BI → graphiques
```

Flyway V6 crée deux vues :

- `analytics.bi_daily_operations` : une ligne par date UTC, devise et type ;
- `analytics.bi_batch_runs` : une ligne par tentative de calcul.

Le service `bi-init` crée le rôle `ledgerflow_bi`. Il possède SELECT uniquement
sur ces vues ; il ne lit pas les comptes clients, l'Outbox ou les messages
rejetés et ne peut pas écrire dans les tables. Les accès Grafana et BI sont
distincts. Le modèle comporte deux tables sans relation : les filtres d'activité
ne doivent pas masquer ou multiplier l'historique des tentatives.

## Comprendre les chiffres

- **Nombre d'opérations** : somme des nombres déjà calculés par Spark, pas nombre
  de lignes de la table. Un virement compte une fois.
- **Montant total** : volume des mouvements, pas bénéfice ni solde de la banque.
- **Montant moyen** : total divisé par nombre d'opérations. C'est une moyenne
  pondérée, pas une moyenne des moyennes quotidiennes.
- **Minimum / maximum** : bornes des opérations des groupes sélectionnés.
- **Devises** : aucune conversion n'est effectuée. Les mesures monétaires
  renvoient une valeur vide si plus d'une devise est présente dans le contexte,
  même si le filtre visuel est contourné.
- **Ouvertures** : distinguées des dépôts, puisqu'il s'agit des fonds de simulation.
- **Dates** : journées et heures de publication en UTC, pas en heure locale.

PostgreSQL et Spark gardent les décimaux exacts. Les graphiques Power BI utilisent
des nombres à virgule flottante : ils peuvent arrondir des valeurs très grandes.
La colonne **Total exact SQL** conserve le texte exact de chaque groupe pour
contrôle. Ne pas utiliser les cartes BI pour une réconciliation financière à
la précision du centime sur des montants dépassant la précision du modèle.

## Publication et actualisation dans Power BI Service

Cette intégration locale ne publie rien dans un compte Microsoft. Pour partager
le rapport, ouvrir Desktop, charger les données et choisir un espace de travail
autorisé. Les droits et licences dépendent du mode de partage retenu.

Un service cloud ne peut pas joindre le `localhost` de ce poste. Il faudra
configurer une passerelle de données locale ou un stockage accessible au service,
puis les identifiants et un horaire d'actualisation après le batch. Un horaire
seul ne garantit pas que le batch a réussi : pour une dépendance stricte,
ajouter ensuite une demande d'actualisation via l'API Power BI après publication.

## Vérifications effectuées

- Les 20 fichiers JSON/PBIP/PBIR passent les schémas publics Microsoft.
- Les 13 visuels tiennent dans leurs pages ; leurs 31 références pointent vers
  des champs existants.
- Le lecteur TMDL officiel Microsoft lit les deux tables et deux paramètres.
- Un test PostgreSQL vérifie les agrégats multidevises, le texte décimal exact,
  l'accès aux vues et le refus de lecture des comptes et d'écriture.

Ces vérifications ne remplacent pas le chargement des données et le contrôle du
rendu dans Desktop. Power BI Desktop a depuis été ouvert sur le poste. Lors de
l'actualisation, il a demandé les identifiants PostgreSQL ; la réussite du
chargement et les filtres restent à confirmer. Aucune publication cloud n'a
été effectuée.

Validation reproductible des fichiers :

```powershell
python -m pip install -r bi/powerbi/requirements-validation.txt
python scripts/check_powerbi.py --fetch-schemas
# Ensuite, sans téléchargement si le cache .work est conservé :
python scripts/check_powerbi.py
```

Pour le modèle, télécharger le package NuGet officiel Microsoft.AnalysisServices
(version testée : 19.118.3), extraire `lib/net8.0`, puis avec PowerShell 7.4+ :

```powershell
.\scripts\check_powerbi_model.ps1 -AssemblyDirectory CHEMIN_VERS_LIB_NET8
```

Sources Microsoft : [projets PBIP](https://learn.microsoft.com/power-bi/developer/projects/projects-overview),
[format de rapport PBIR](https://learn.microsoft.com/power-bi/developer/projects/projects-report),
[connexion PostgreSQL](https://learn.microsoft.com/power-query/connectors/postgresql).
