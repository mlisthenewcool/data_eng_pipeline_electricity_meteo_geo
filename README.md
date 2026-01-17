# Data Engineering Personal Project

Objectif : pipeline de données énergétiques, météorologiques et géographiques françaises

## Installation

## Architecture

## Sources de données

- IGN CONTOURS-IRIS
- ODRE Installations
- Météo France Stations

## Data Pipeline

Voir [README_DATA.md](docs/README_DATA.md)

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

Voir [README_DX.md](docs/README_DX.md)

## TODO

- [x] passage à pydantic-settings
- [x] déplacer data_catalog.yaml dans dossier data et modifier le build Docker
    - [ ] voir les TODO de mise en prod dans le docker-compose.yaml
- [ ] modifier les raise ... from e
- [ ] comment gérer l'erreur de génération d'assets proprement dans Airflow si une erreur arrive durant le parsing
  des DAGS ?

- tests/
    - [ ] transfert ancien downloader
    - [ ] test_logger
        - [ ] vérifier la redirection vers Airflow
        - [ ] vérifier que passer un objet non mutable à la méthode _format_extra ne change rien

* ajouter configuration Open Lineage
    * https://airflow.apache.org/docs/apache-airflow-providers-openlineage/stable/guides/user.html