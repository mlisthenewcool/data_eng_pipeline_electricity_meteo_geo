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

### Architecture des données : Médaillon

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

### Logique du pipeline par dataset

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
- [ ] créer exemple complet pipeline sans Airflow
- [ ] ajout des documentations des données avec les anciens fichiers
- [ ] simplifier pipeline
    - [ ] cohérence des arguments
    - [ ] retirer tâche "landing" qui est juste là pour les XCom ?
    - [ ] ajouter class PipelineContext qui réduit le nombre de passages d'arguments et centralise l'info
    - [ ] documenter choix des Serializer, des transformations et du déroulement logique du pipeline
    - [ ] cohérence avec les sha256
    - [ ] valider par de la data quality les couches bronze & silver
- [ ] ajout mécanisme pour vérifier la "fraîcheur" des données (requête HEAD si possible, ou comparaison hash_sha256)
- [ ] ajout transformations de base silver
    - [ ] normaliser (types, unités)
    - [ ] cohérence générale (pas de données aberrantes)
- [ ] corriger les problèmes de résolution de chemin
    - [ ] calcul dans settings directement
    - [ ] trouver un moyen de gérer proprement les archives (ajout inner_path_extension au Dataset ?)
- [ ] ajout d'un type pour le nom des layers (StrEnum)
- [ ] ajout métrique de performances
- [ ] ajout du mode incrémental ?
- [ ] potentiel problème de performances pour la lecture des dataframes (à documenter, pour l'instant cohérence
  bronze/silver lors du passage des arguments aux fonctions de transformations)
    - puisqu'on change de tâche entre bronze & silver, on doit relire à nouveau le même dataframe
- [ ] complexité du state management: on gère un système indépendant + celui de Airflow
    - [ ] vérifier qu'on ne puisse pas tout passer sous Airflow (vérifier sha256 pour sûr, autre chose ?)
- si besoin de paralléliser, regarder @task.map
- si besoin d'améliorer perfs, faire du incremental loading pour les datasets


- [ ] tests/
    - [ ] transfert ancien downloader
    - [ ] test_logger
        - [ ] vérifier la redirection vers Airflow
        - [ ] vérifier que passer un objet non mutable à la méthode _format_extra ne change rien
    - [ ] tester au moins les fonctions critiques
        - [ ] download, extract, transformations
- [ ] [ajouter configuration Open Lineage](https://airflow.apache.org/docs/apache-airflow-providers-openlineage/stable/guides/user.html)