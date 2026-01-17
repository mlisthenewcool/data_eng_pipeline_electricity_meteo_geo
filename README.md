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

Architecture des données : Médaillon

- Landing
    - Données brutes téléchargées
    - Format original (7z, JSON, Parquet)
    - Supprimées si le transfert vers la couche bronze a réussi
- Bronze
    - Renommage des colonnes pour suivre la convention snake_case
    - Conversion en format Parquet standardisé
    - Conservées en fonction de la politique de rétention (par dataset ou global)
- Silver
    - Données nettoyées et normalisées
    - Les données des datasets ne doivent plus être modifiées
        - TODO : comment faire dans le cas d'un dataset incrémental ?
- Gold
    - Données enrichies et agrégées entre datasets
    - Prêtes pour analyse/visualisation

## Développement

Voir [README_DX.md](docs/README_DX.md)

## TODO

- [x] déplacer data_catalog.yaml dans le dossier data et modifier le build Docker
    - [ ] TODO de mise en prod dans le docker-compose.yaml
- [x] passage à pydantic-settings
- [ ] modifier les raise ... from e
- [ ] comment gérer l'erreur de génération d'assets proprement dans Airflow si une erreur arrive durant le parsing
  des DAGS ?
- [ ] mettre en place politique de rétention pour la couche bronze

- tests/
    - [ ] transfert ancien downloader
    - [ ] test_logger
        - [ ] vérifier la redirection vers Airflow
        - [ ] vérifier que passer un objet non mutable à la méthode _format_extra ne change rien

* ajouter configuration Open Lineage
    * https://airflow.apache.org/docs/apache-airflow-providers-openlineage/stable/guides/user.html