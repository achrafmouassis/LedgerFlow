# Interface et sécurité de l’API

L'interface locale est servie par Spring Boot sur **http://localhost:8080/**.
HTML/CSS/JavaScript sans dépendance externe ni CDN ; requêtes vers la même origine.
Elle montre de vraies réponses API et les données fictives du projet, pas des
chiffres codés en dur. Le skill Sites a guidé son organisation et sa présentation
locale ; aucun hébergement Sites ni publication LinkedIn n'a été effectué.

## Premier démarrage et identifiants

```powershell
python scripts/init_local_auth.py
.\scripts\platform.ps1 up -d --build
python scripts/setup_demo_access.py
```

Le premier script crée une seule fois `.env.auth` et `.work/local-access.json`,
tous deux ignorés par Git. Les mots de passe sont aléatoires et ne sont pas
affichés dans les journaux. Ouvrir le JSON local pour connaître les identifiants
`admin`, `operator`, `analyst`, `client`, `client2`. Ne pas les publier, filmer,
envoyer dans une PR ou intégrer à une capture LinkedIn.

Le bootstrap crée seulement l'administrateur initial, sous verrou partagé en
base, si aucun utilisateur n'existe. Un mot de passe initial de 16 à 72 caractères
est obligatoire ; il n'existe pas de bascule permettant de désactiver la sécurité.
Les scripts de démonstration et QA s'authentifient eux aussi.

Le script d'attribution suppose que des comptes existent déjà : il attribue un
compte distinct à chacun des deux clients. Les données financières existantes
ne sont pas modifiées. Sans attribution, un client ne voit aucun compte.

## Permissions côté serveur

| Action | ADMIN | OPERATOR | VIEWER / analyst | CUSTOMER |
| --- | --- | --- | --- | --- |
| Lire comptes et journaux | Tous | Tous | Tous | Uniquement attribués |
| Créer un compte | Oui | Oui | Non | Non |
| Dépôt / retrait | Oui | Oui | Non | Non |
| Virement | Oui | Oui | Non | Depuis un compte attribué |
| Statistiques et pipeline globaux | Oui | Oui | Oui | Non |
| Utilisateurs et attributions | Oui | Non | Non | Non |

Un virement client peut atteindre un destinataire de même devise dont l'UUID est
connu, sans donner accès à son solde ou son journal. Un montant positif, un solde
suffisant et toutes les règles bancaires restent obligatoires.

L'interface adapte ses boutons au rôle ; l'API applique les mêmes interdictions
même si quelqu'un modifie le JavaScript ou envoie directement une requête HTTP.

## Authentification

- `POST /api/auth/login` : JSON username/password ; retourne un Bearer opaque.
- `GET /api/auth/me` : identité authentifiée.
- `POST /api/auth/logout` : révoque le token courant en base.
- Mots de passe BCrypt, coût 12 ; limite explicite de 72 octets UTF-8.
- Tokens aléatoires de 256 bits, empreintes SHA-256 uniquement en base, durée 8 h.
- Vérification de l'utilisateur actif et de son rôle à chaque demande.
- Cinq mots de passe erronés verrouillent ce compte pendant cinq minutes.
- Réponses génériques à une mauvaise connexion ; pas d'énumération volontaire.
- Sessions stockées dans PostgreSQL, partagées entre instances, sans affinité.
- Aucun cookie d'authentification, formulaire Spring ou HTTP Basic. L'authentification
  exige un en-tête explicite ; CSRF désactivé dans ce modèle sans credentials
  attachés automatiquement par le navigateur. CORS n'est pas ouvert.
- Token gardé uniquement en mémoire dans l'interface, ni localStorage ni sessionStorage.
  Recharger la page demande une nouvelle connexion ; la déconnexion efface la vue.
- CSP même origine, protection contre l'encadrement, no-store sur les réponses.
- API anonyme : 401 ; permission absente : 403 ; base d'authentification indisponible : 503.

Exemple d'appel : envoyer `Authorization: Bearer <token>` pour tous les anciens
exemples `/api/**`. Une clé `Idempotency-Key` reste nécessaire pour les opérations.
Les mots de passe, tokens et en-têtes Authorization ne doivent jamais être ajoutés
aux rapports QA ni aux exemples de publication.

## Interface

Vue d'ensemble réelle, répertoire des comptes, journal paginé, formulaires de
dépôt/retrait/virement et création de compte opérateur, réponse HTTP visible,
clé d'idempotence réutilisable, pipeline, historique Spark, atelier API de lecture,
création d'utilisateurs et attribution de comptes pour l'administrateur.
Les graphiques utilisent la dernière journée UTC disponible, sans fabriquer
d'historique. Les devises restent séparées. L'arrondi de l'interface est uniquement
visuel ; les calculs et validations monétaires du serveur utilisent des décimaux.

La création de compte n'est pas idempotente : après une réponse réseau incertaine,
vérifier le répertoire avant de réessayer. Pour un dépôt/retrait/virement, conserver
la même clé et le même contenu. Le bouton Nouvelle clé représente une autre demande.

## Limites avant exposition publique

Il s'agit d'une démonstration locale sécurisée, pas d'une banque prête à produire.
Prévoir HTTPS, secrets de déploiement, limitation de débit par IP/identité au proxy,
MFA/OIDC selon le contexte, rotation des secrets, audit des actions privilégiées,
gestion du cycle de vie des utilisateurs et procédures de récupération de compte.
Le verrouillage par compte seul n'empêche pas tous les dénis de service ou attaques
distribuées. Le compte SQL applicatif local est privilégié : séparer un rôle de
migration et un rôle d'exécution à privilèges minimaux avant hébergement public.
Ne pas exposer PostgreSQL, Kafka, Grafana ou Airflow directement sur Internet.
