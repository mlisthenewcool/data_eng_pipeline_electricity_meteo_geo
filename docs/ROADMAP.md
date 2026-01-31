# Roadmap

## Phase 1 : Architecture & Versioning

- [x] **PathResolver refactoring** : Architecture découplée sans dépendance circulaire
- [x] **Bronze versioning** : Historique complet avec rétention automatique (365j)
- [x] **Silver dual-file pattern** : `current.parquet` + `backup.parquet` pour rollback rapide
- [x] **IngestionFrequency enrichie** : Génération de versions selon la fréquence
- [ ] **DAG de maintenance** : Nettoyage hebdomadaire des anciennes versions Bronze
- [ ] **State management**
    - [x] Documenter le choix actuel du fichier JSON par dataset
    - [x] Regarder si possibilité de remplacer le JSON par les métadonnées émises pour l'asset (Metadata couche silver)
    - [ ] Passage aux Metadata pour le state management
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
- [ ] Simplification de nommage des fichiers bronze (heure max)
- [ ] Séparation du DAG ingestion jusqu'au fichier parquet silver du DAG insertion dans Postgres

## Phase 2 : Pipeline Robustesse

- [ ] **Fail-fast validation** :
    - [ ] Vérifier transformations enregistrées au démarrage DAG
    - [ ] Au début de chaque task, vérifier que l'état actuel est cohérent ?
- [ ] **Exceptions personnalisées** : Remplacer `raise Exception` par exceptions métier
- [ ] **Gestion d'erreurs cohérente** : Stratégie unifiée logging + exceptions, retirer les raise... from e
- [ ] **Documentation pipeline** : Serializer, transformations, déroulement logique
- [ ] **CLI tool** : `scripts/inspect_bronze.py` pour debug/maintenance manuelle
- [ ] Après ajout de nouvelles transformations silver, relancer automatiquement ? Uniquement les transformations ?
- [ ] Séparation des différents DAGs dans des fichiers différents
- [ ] Ajout de l'étape d'insertion dans Postgres à la fin des DAGs

## Phase 3 : Transformations & Qualité

- [ ] **Transformations Bronze** : Typage Parquet, renommage systématique des colonnes
- [ ] **Transformations Silver** : Data quality (Great Expectations / Soda Core)
    - [ ] Normalisation types et unités
    - [ ] Détection données aberrantes
    - [ ] Validation schémas
- [ ] **Mode incrémental** : Support datasets à mise à jour différentielle

## Phase 4 : Documentation, Tests & Validation

- [ ] **Documentation complète** : docs/README_.md
    - [ ] Ajouter screenshots Airflow
    - [ ] Schéma des liens entre datasets
    - [ ] Vue visuelle des layers plutôt que textuelle
    - [ ] Super exemple sur lequel s'appuyer : https://github.com/abeltavares/batch-data-pipeline
- [ ] **Tests unitaires** :
    - [ ] core
    - [ ] utils
- [ ] **Tests d'intégration pipeline** : Flux complet Landing → Silver
- [ ] **Tests transformations** : Validation Bronze → Silver pour tous les datasets
- [ ] **Test maintenance DAG** : Mock fichiers anciens + vérification nettoyage
- [ ] **Objectif couverture** : 9% → 60%+

## Phase 5 : Production & Observabilité

- [ ] **Déploiement Docker** :
    - [ ] Finaliser `docker-compose.yaml` pour production
    - [ ] Gestion des privilèges pour Postgres
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

## Bugs Connus

- [ ] Logs DEBUG invisibles dans Airflow UI
- [ ] Erreur génération assets non capturée proprement lors parsing DAGs

## Idées Futures

- [ ] Couche **Gold** : Agrégations cross-datasets pour analytics
- [ ] **Data catalog web UI** : Interface pour explorer datasets/versions
- [ ] **Rollback automatisé** : Détection anomalies → rollback Silver sans intervention
- [ ] **Archivage long terme** : Export Bronze vers S3 Glacier après rétention