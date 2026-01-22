# Développement

## Installation

### Prérequis

- Python 3.13+
- Docker & Docker Compose
- `uv`, `pre-commit`

### Installation rapide

```bash
# Cloner le dépôt
git clone <repo_url>
cd de_projet_perso

# Installer uv (si pas déjà installé)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Synchroniser les dépendances
uv sync --upgrade

# Configurer les variables d'environnement
cp .env.example .env
# Éditer .env avec vos valeurs

# Démarrer Airflow avec Docker
docker compose up --detach

# Accéder à l'interface Airflow
# URL: http://localhost:8080
```

### Structure du projet

```
de_projet_perso/
├── data/                     # Données (git-ignored)
│   ├── landing/              # Téléchargements temporaires
│   ├── bronze/               # Versions historiques (365 jours par défaut)
│   ├── silver/               # current.parquet + backup.parquet
│   └── _state/               # État JSON du pipeline
├── src/de_projet_perso/
│   ├── core/                 # Composants centraux (logger, settings, catalog, ...)
│   ├── pipeline/             # Logique du pipeline (manager, transformer, ...)
│   ├── airflow/              # DAGs et tâches Airflow
│   └── utils/                # Utilitaires (bas niveau, indépendants)
├── tests/                    # Tests unitaires et d'intégration
├── docs/                     # Documentation
│   └── README_*.md
├── airflow.Dockerfile        # Image Docker pour Airflow
├── docker-compose            # Configuration Docker Compose
└── pyproject.toml            # Configuration du projet
```


## Docker

* Cycle de vie

```shell
# Lancer les services en arrière-plan & reconstruit l'image si modifiée
docker compose up --build --detach

# Arrêter les services
docker compose stop

# Redémarrer les services arrêtés sans recréer les conteneurs
docker compose start

# Supprimer les services (conteneurs & réseaux)
# /!\ attention /!\ ajouter `--volumes` pour supprimer les volumes nommés associés
# ajouter `--rmi` pour supprimer les images associées
docker compose down
```

* Inspection & débug

```shell
# Vérifier l'état des conteneurs
docker compose ps

# Suivre les logs des 50 dernières lignes de chaque service
# ajouter `--follow` pour suivre en temps réel
docker compose logs --tail 50

# Entrer dans le conteneur en ligne de commande pour débugger
docker compose exec airflow_service bash
```

* Maintenance ciblée par service

```shell
docker compose restart airflow_service
docker compose up --build airflow_service
docker compose logs --tail 50 airflow_service
docker compose ps airflow_service
```

* Nettoyage système

```shell
# Supprimer les images "dangling" qui ne sont plus associées
docker image prune
# Nettoyage complet (réseaux, caches de build, images)
docker system prune
# Vérifier l'espace disque consommé par Docker
docker system df 
```

## Développement local

```bash
# Environnement
uv sync --upgrade              # Synchroniser les dépendances
source .venv/bin/activate      # Activer l'environnement virtuel

# Qualité du code (avant commit)
uv run ruff check --fix        # Linter + auto-fix
uv run ruff format             # Formatage
uv run ty check                # Vérification de types (doit passer)
uv run pre-commit run --all-files  # Tous les checks

# Tests
uv run pytest                  # Tous les tests avec couverture
uv run pytest -v               # Mode verbose
uv run pytest -k "test_pattern"  # Tests correspondant au pattern

# Docker & Airflow
docker compose up --detach     # Démarrer Airflow
docker compose logs airflow -f # Suivre les logs
docker compose down            # Arrêter les services
```

**Note** : L'installation locale d'Airflow est pour le support IDE uniquement. Airflow réel s'exécute dans Docker.

* [lancer Marimo avec hot-reload](https://docs.marimo.io/guides/editor_features/watching/#watching-for-changes-to-your-notebook)