# Projet personnel Data Engineering

Intégrer et enrichir les données suivantes disponibles en accès libre pour prédire et lier les indicateurs météo à
la production et la consommation électrique en France.

- la production et la consommation d'électricité en France par ODRÉ (OpenData Réseaux-Énergies) :
    - la liste des installations de production
    - la production et la consommation d'électricité au niveau régional avec une précision au pas de l'heure
        - consolidées et définitives
        - temps réel


- les observations météorologiques qualifiées par Météo France :
    - la liste des stations
    - les données qualifiées avec une précision au pas de l'heure


- la maille IRIS publiée par l'IGN pour lier les données ci-dessus

---
Ce projet met en place une architecture médaillon orchestrée par un ordonnanceur (_scheduler_) pour traiter la
donnée dans le pas de temps le plus fin disponible.

- Outils techniques utilisés :
    - Python 3.13
    - Airflow 3.1
    - Postgres 17
    - Marimo 0.19 (notebooks)
    - Docker & Docker Compose 5.0
    - GitHub Actions (CI)
    - Code quality : pytest, coverage, ruff, ty, pre-commit hooks


- Déploiement (à venir)

---

- Plus de détails ici :
    - [Documentation de développement](docs/README_DEV.md)
    - [Documentation technique](docs/README_TECH.md)
    - [Roadmap](docs/ROADMAP.md)

## Sources de données

- IGN :
    - Contours iris

- ODRÉ :
    - Registre national des installations de production et de stockage d'électricité
    - Données éCO2mix régionales consolidées et définitives
    - Données éCO2mix régionales temps réel

- Météo France :
    - Informations sur les stations
    - Données climatologiques de base — horaires

---

Plus de détails ici : [documentation des données](docs/README_DATA_SOURCES.md)

## Architecture des données : Médaillon

```
Landing (temporary) → Bronze (versioned) → Silver (current + backup) → Gold (analytics)
```

---

Plus de détails ici : [documentation architecture des données](docs/README_DATA.md)

## Ordonnanceur

- DAG Simple (fichiers directs : Parquet, JSON, etc.)

```
check_remote_changed (short-circuit)
    ├── [SKIP] → downstream skipped (remote unchanged - ETag/Last-Modified identiques)
    └── [CONTINUE] → download_then_check_hash (short-circuit)
                        ├── [SKIP] → downstream skipped (SHA256 identique - false change)
                        └── [CONTINUE] → convert_to_bronze
                                            └── transform_to_silver (émet Asset Metadata)
```

- DAG Archive (fichiers 7z)

```
check_remote_changed (short-circuit)
    ├── [SKIP] → downstream skipped (remote unchanged - ETag/Last-Modified identiques)
    └── [CONTINUE] → download_then_check_hash (short-circuit)
                        ├── [SKIP] → downstream skipped (SHA256 archive identique)
                        └── [CONTINUE] → extract_archive_then_check_hash (short-circuit)
                                            ├── [SKIP] → downstream skipped (SHA256 fichier extrait identique)
                                            └── [CONTINUE] → convert_to_bronze
                                                                └── transform_to_silver (émet Asset Metadata)

```

## Déploiement

...