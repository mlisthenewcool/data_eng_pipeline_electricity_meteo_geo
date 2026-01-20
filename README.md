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

### Priorité 1

- [x] déplacer data_catalog.yaml dans le dossier data et modifier le build Docker
    - [ ] TODO de mise en prod dans le docker-compose.yaml
- [x] passage à pydantic-settings

- [ ] corriger les problèmes de résolution de chemin
    - [x] calcul dans settings directement
    - [x] trouver un moyen de gérer proprement les archives
        - ~~[ ] ajout inner_path_extension au Dataset ?~~
        - [x] calcul avec `Path(...).with_suffix(...)`
    - [x] résoudre les incohérences de nommage de fichiers (avant landing garde les mêmes noms que sur le serveur,
      ce n'est qu'à partir de la couche bronze qu'on renomme avec nos conventions)
    - [ ] basculer sur une architecture de résolution de chemin découplée du Dataset
        - [ ] retirer la propriété `storage` du catalogue de données
        - [ ] ajouter classe PipelinePathResolver (@property landing() → Path...)
        - [ ] ajouter nom du dataset dans sa définition

- [ ] pipeline
    - [x] passage à 2 DAGs spécifiques (un avec et un sans extraction d'archive)
    - [x] retirer tâche "landing" qui est juste là pour les XCom ?
    - ~~[ ] ajouter class PipelineContext qui réduit le nombre de passages d'arguments et centralise l'info~~
    - [ ] documenter choix des Serializer, des transformations et du déroulement logique du pipeline
    - [x] créer exemple complet pipeline sans Airflow
    - [x] cohérence des arguments passés entre chaque tâche
    - [x] passer tous les arguments nécessaires aux métadonnées
    - [ ] fail-fast si transformations bronze/silver pas enregistrées

- [ ] check_should_run doit arriver après contrôle de la cohérence de l'état actuel ? même tâche ?
    - → check_state
        - → si ok → check_should_run
        - → sinon → heal_state
            - → download_data → ...

- [x] ajout mécanisme pour vérifier la "fraîcheur" des données
    - [x] requête HEAD si possible
    - [x] comparaison hash_sha256

- [ ] complexité du state management: on gère un système indépendant + celui de Airflow
    - [ ] vérifier qu'on ne puisse pas tout passer sous Airflow (vérifier sha256 pour sûr, autre chose ?)

- [ ] cohérence de la gestion des exceptions & des logs associés
    - [ ] choix stratégie (fonctions Pipeline ? tasks ? fonctions bas-niveau ?)
    - [ ] documentation de la solution choisie

- [x] cohérence entre ExtractionInfo et ExtractionResult

- [ ] normalement OK d'utiliser short_circuit même sans renvoyer explicitement `True` car le dictionnaire suivant ne
  sera (jamais ?) interprété à `False`. Idem pour la tâche download_and_check_hash_task

## Priorité 2

- [ ] résoudre bug affichage des logs dans airflow pour le niveau DEBUG

- [ ] comment gérer l'erreur de génération d'assets proprement dans Airflow si une erreur arrive durant le parsing
  des DAGS ?

- [ ] ajout transformations de base silver
    - [ ] normaliser (types, unités)
    - [ ] cohérence générale (pas de données aberrantes)
- [ ] valider par de la data quality les couches bronze & silver

- [ ] ajout du mode incrémental ?

## Priorité 3

- [ ] modifier les raise ... from e

- [ ] ajout d'un type pour le nom des layers (StrEnum)
- [ ] uniformiser les chemins entre landing & les autres couches

- [ ] mettre en place politique de rétention pour la couche bronze
- [ ] ajout des documentations des données avec les anciens fichiers

- [ ] regarder @setup & @teardown pour le cleanup Airflow

- [ ] tests/
    - [ ] transfert ancien downloader
    - [ ] test_logger
        - [ ] vérifier la redirection vers Airflow
        - [ ] vérifier que passer un objet non mutable à la méthode _format_extra ne change rien
    - [ ] tester au moins les fonctions critiques
        - [ ] download, extract, transformations

### Priorité 4

- [ ] ajout métrique de performances
- [ ] [ajouter configuration Open Lineage](https://airflow.apache.org/docs/apache-airflow-providers-openlineage/stable/guides/user.html)
- [ ] si besoin de paralléliser, regarder @task.map
- [ ] si besoin d'améliorer perfs, faire du incremental loading pour les datasets, compresser les parquets avec zstd
- [ ] potentiel problème de performances pour la lecture des dataframes (à documenter, pour l'instant cohérence
  bronze/silver lors du passage des arguments aux fonctions de transformations). Puisqu'on change de tâche entre
  bronze & silver, on doit relire à nouveau le même dataframe.