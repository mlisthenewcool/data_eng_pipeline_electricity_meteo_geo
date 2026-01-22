## Traitements

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
