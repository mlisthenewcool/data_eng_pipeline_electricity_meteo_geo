# Data Engineering Personal Project

Objectif : pipeline de données énergétiques, météorologiques et géographiques françaises

## Installation

## Architecture

## Sources de données

- IGN CONTOURS-IRIS
- ODRE Installations
- Météo France Stations

## Data Pipeline

Voir [README_DATA.md](./README_DATA.md)

Architecture des données

Layers

- Landing (Bronze)
    - Données brutes téléchargées
    - Format original (7z, JSON, Parquet)
- Silver
    - Données nettoyées et normalisées
    - Format Parquet standardisé
- Gold
    - Données enrichies et agrégées
    - Prêtes pour analyse/visualisation

## Développement

Voir [README_DX.md](./README_DX.md)

## TODO

* ancien dossier
    * CLAUDE.md : transférer vers AGENTS
    * tests/test_downloader.py
    * notebooks/

* déplacer data_catalog.yaml
* modifier les raise ... from e
* comment gérer l'erreur de génération d'assets proprement dans Airflow si une erreur arrive durant la validation ?
* tests/
    * test_logger
        * vérifier la redirection vers Airflow
        * vérifier que passer un objet non mutable à la méthode _format_extra ne change rien


* changer structure code python
  src/de_projet_perso/
  ├── core/ # ✅ Déjà bien organisé
  ├── pipelines/ # 🆕 Logique métier des pipelines
  │ ├── __init__.py
  │ ├── base.py # BasePipeline abstrait
  │ └── ign.py # IGNPipeline(BasePipeline)
  ├── airflow_utils/ # 🆕 Utilitaires Airflow
  │ ├── __init__.py
  │ └── dag_factory.py # Générer DAGs depuis catalog
  └── cli/ # 🆕 Scripts CLI
  ├── __init__.py
  └── inspect_catalog.py # Ancien ign_contours_iris.py