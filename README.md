# Data Engineering Personal Project

Objectif : pipeline de données énergétiques, météorologiques et géographiques françaises

## Installation

### Prérequis

- Python 3.13+
- Docker & Docker Compose
- `uv` (gestionnaire de dépendances Python)

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
│   ├── bronze/               # Versions historiques (365j)
│   ├── silver/               # current.parquet + backup.parquet
│   └── _state/               # État JSON du pipeline
├── src/de_projet_perso/
│   ├── core/                 # Composants centraux (logger, settings, catalog)
│   ├── pipeline/             # Logique du pipeline (manager, transformer)
│   ├── airflow/              # DAGs et adaptateurs Airflow
│   └── utils/                # Utilitaires (downloader, extractor)
├── tests/                    # Tests unitaires et d'intégration
├── docs/                     # Documentation
│   └── README_*.md
├── AGENTS.md                 # Guide pour assistants IA
├── CLAUDE.md                 # Guide Claude Code
└── pyproject.toml            # Configuration du projet
```

## Architecture

## Sources de données

- IGN CONTOURS-IRIS
- ODRE Installations
- Météo France Stations

## Data Pipeline

Voir [README_DATA.md](docs/README_DATA.md)

### Architecture des données : Médaillon

```
Landing → Bronze (versioned) → Silver (current + backup) → Gold (analytics)
```

**Structure des répertoires** :

```
data/
├── landing/{dataset_name}/              # Temporaire (supprimé après Bronze)
│   └── {nom_original_fichier}          # Préserve les noms du serveur
├── bronze/{dataset_name}/
│   ├── 2026-01-21.parquet              # Versions quotidiennes ({{ ds }})
│   ├── 2026-01-22.parquet              # OU horaires ({{ ts_nodash }})
│   └── latest.parquet → 2026-01-22.parquet  # Lien symbolique vers la dernière
├── silver/{dataset_name}/
│   ├── current.parquet                  # Version active (utilisée en aval)
│   └── backup.parquet                   # Version précédente (rollback)
├── gold/{dataset_name}/                 # Prêt pour analyse (non implémenté)
└── _state/{dataset_name}.json           # État du pipeline
```

**Caractéristiques par couche** :

- **Landing**
    - Données brutes téléchargées
    - Format original (7z, JSON, Parquet)
    - **Supprimées** après transfert réussi vers Bronze
    - Noms de fichiers préservés du serveur

- **Bronze**
    - Renommage des colonnes (snake_case)
    - Conversion en Parquet standardisé
    - **Versions historiques** : 1 fichier par exécution (quotidien/horaire)
    - **Rétention** : 365 jours par défaut (`ENV_BRONZE_RETENTION_DAYS`)
    - **Symlink `latest.parquet`** : mis à jour automatiquement
    - **Nettoyage automatique** : DAG `maintenance_bronze_retention` hebdomadaire

- **Silver**
    - Données nettoyées et normalisées
    - **Uniquement 2 fichiers** : `current.parquet` (actif) + `backup.parquet` (N-1)
    - **Rollback instantané** : `backup → current` sans retraitement
    - **Rotation atomique** : `current → backup` avant chaque nouvelle écriture
    - Lit toujours depuis `bronze/latest.parquet` (pas de version spécifique)

- **Gold**
    - Données enrichies et agrégées entre datasets
    - Prêtes pour analyse/visualisation
    - **TODO** : Non encore implémenté

**Gestion des versions** :

| Fréquence | Format de version | Exemple           | Airflow Template  |
|-----------|-------------------|-------------------|-------------------|
| Quotidien | `YYYY-MM-DD`      | `2026-01-21`      | `{{ ds }}`        |
| Horaire   | `YYYYMMDDTHHmmss` | `20260121T143022` | `{{ ts_nodash }}` |

**Avantages de cette architecture** :

- ✅ **Historique Bronze** : Debug, audit, comparaison de versions (1 an)
- ✅ **Rollback Silver rapide** : <1 seconde sans retraitement
- ✅ **Stockage optimisé** : Silver (2 fichiers), Bronze (rétention configurée)
- ✅ **Traçabilité** : Chaque version Bronze liée à une exécution Airflow

### Logique du pipeline par dataset

TODO: mettre à jour

```
decide_action (branch)
    ├── mark_skipped (si action = SKIP) ✅ affiche raison détaillée
    └── validate_state_coherence (branch) ✅ fusionne log_state_summary + validation
            ├── cleanup_incoherent_state → download_data (si état incohérent) ✅
            └── download_data (si état cohérent) ✅
                    └── check_extraction_needed (branch) ✅
                            ├── extract_archive (si is_archive) ✅ cleanup .7z + validation
                            └── skip_extraction (si pas archive) ✅ validation directe
                                    └── convert_to_bronze ✅
                                            └── transform_to_silver (émet Metadata enrichies) ✅
```

## Architecture Technique

### Gestion des chemins (PathResolver)

Le projet utilise `PathResolver` pour centraliser la construction des chemins du pipeline :

```python
from de_projet_perso.core.path_resolver import PathResolver
from de_projet_perso.core.data_catalog import DataCatalog
from de_projet_perso.core.settings import settings
from datetime import datetime

# Charger le catalogue
catalog = DataCatalog.load(settings.data_catalog_file_path)
dataset = catalog.get_dataset('ign_contours_iris')

# Générer la version (scripts/notebooks)
run_version = dataset.ingestion.frequency.format_datetime_as_version(
    datetime.now(),
    no_dash=True
)  # → "20260121" (quotidien) ou "20260121T143022" (horaire)

# Créer le resolver
resolver = PathResolver(dataset_name=dataset.name)

# Accéder aux chemins
bronze_path = resolver.bronze_path  # data/bronze/ign_contours_iris/20260121.parquet
bronze_latest = resolver.bronze_latest_path  # data/bronze/ign_contours_iris/latest.parquet
silver_current = resolver.silver_current_path  # data/silver/ign_contours_iris/current.parquet
```

**Dans les DAGs Airflow**, utilisez les templates Airflow :

```python
run_version = dataset.ingestion.frequency.get_airflow_version_template(no_dash=True)  # noqa
# → "{{ ds_nodash }}" ou "{{ ts_nodash }}" (remplacé au runtime)
```

Voir [ARCHITECTURE_DECISIONS.md](docs/ARCHITECTURE_DECISIONS.md) pour les choix de design.

## Développement

### Commandes rapides

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

Voir aussi :

- [README_DX.md](docs/README_DX.md) - Guide développeur détaillé
- [AGENTS.md](AGENTS.md) - Guide pour assistants IA
- [CLAUDE.md](CLAUDE.md) - Guide spécifique Claude Code

## Roadmap

### Phase 1 : Architecture & Versioning

- [x] **PathResolver refactoring** : Architecture découplée sans dépendance circulaire
- [x] **Bronze versioning** : Historique complet avec rétention automatique (365j)
- [x] **Silver dual-file pattern** : `current.parquet` + `backup.parquet` pour rollback rapide
- [x] **IngestionFrequency enrichie** : Génération de versions selon la fréquence
- [ ] **DAG de maintenance** : Nettoyage hebdomadaire des anciennes versions Bronze
- [ ] **State management**
    - [ ] Documenter le choix actuel du fichier JSON par dataset
    - [ ] DAG ou task de vérification avant chaque DAG d'ingestion ?
    - → check_state
        - → si ok → check_should_run
        - → sinon → heal_state
            - → download_data → ...
    - [ ] gérer les cas où les fichiers `_state` et/ou les versions bronze ont été supprimées
    - [ ] est-ce utile en l'état d'avoir un champ `history` dans `_state` ?
- [ ] **Datasets incrémentaux** :
    - [ ] passer par API et ne récupérer que les nouvelles données ?
    - [ ] eco2mix : def (une seule fois), cons (une fois par jour), tr (une fois par heure)
    - [ ] meteo_observations & meteo_climatologie

### Phase 2 : Pipeline Robustesse

- [ ] **Fail-fast validation** :
    - [ ] Vérifier transformations enregistrées au démarrage DAG
    - [ ] Au début de chaque task, vérifier que l'état actuel est cohérent ?
- [ ] **Exceptions personnalisées** : Remplacer `raise Exception` par exceptions métier
- [ ] **Gestion d'erreurs cohérente** : Stratégie unifiée logging + exceptions, retirer les raise... from e
- [ ] **Documentation pipeline** : Serializer, transformations, déroulement logique
- [ ] **CLI tool** : `scripts/inspect_bronze.py` pour debug/maintenance manuelle
- [ ] Après ajout de nouvelles transformations silver, ne relancer que ça par exemple, par l'intégralité du DAG

### Phase 3 : Transformations & Qualité

- [ ] **Transformations Bronze** : Typage Parquet, renommage systématique des colonnes
- [ ] **Transformations Silver** : Data quality (Great Expectations / Soda Core)
    - [ ] Normalisation types et unités
    - [ ] Détection données aberrantes
    - [ ] Validation schémas
- [ ] **Mode incrémental** : Support datasets à mise à jour différentielle

### Phase 4 : Documentation, Tests & Validation

- [ ] **Documentation complète** : docs/README_.md
- [ ] **Tests unitaires** :
    - [ ] core
    - [ ] utils
- [ ] **Tests d'intégration pipeline** : Flux complet Landing → Silver
- [ ] **Tests transformations** : Validation Bronze → Silver pour tous les datasets
- [ ] **Test maintenance DAG** : Mock fichiers anciens + vérification nettoyage
- [ ] **Objectif couverture** : 9% → 60%+

### Phase 5 : Production & Observabilité

- [ ] **Déploiement Docker** : Finaliser `docker-compose.yaml` pour production
- [ ] **Améliorations Airflow** :
    - [ ] Parallélisation avec `@task.map`
    - [ ] Cleanup avec @setup & @teardown
- [ ] **OpenTelemetry** : Traces distribuées + métriques
- [ ] **Alerts & SLA** : Monitoring qualité + latence
- [ ] **Open Lineage** : Traçabilité données end-to-end
- [ ] **Optimisations** :
    - [ ] Compression Parquet (zstd)
    - [ ] Incremental loading
    - [ ] Éviter relecture dataframes entre bronze & silver

### Bugs Connus

- [ ] Logs DEBUG invisibles dans Airflow UI
- [ ] Erreur génération assets non capturée proprement lors parsing DAGs

### Idées Futures

- [ ] Couche **Gold** : Agrégations cross-datasets pour analytics
- [ ] **Data catalog web UI** : Interface pour explorer datasets/versions
- [ ] **Rollback automatisé** : Détection anomalies → rollback Silver sans intervention
- [ ] **Archivage long terme** : Export Bronze vers S3 Glacier après rétention