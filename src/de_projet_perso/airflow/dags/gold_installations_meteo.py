"""Gold layer DAG: installations_meteo.

This DAG creates the analytical dataset linking renewable energy installations
(solar and wind) with their nearest compatible weather station.

Triggered when ANY of the 3 Silver datasets is updated (AssetAny).
Includes validation that all Silver files exist before transformation.

Output:
- Asset: installations_meteo_gold
- Path: data/gold/installations_meteo/current.parquet
"""

from datetime import datetime, timedelta
from typing import Any

import polars as pl
from airflow.sdk import AssetAny, Metadata, dag, task

from de_projet_perso.airflow.assets import ASSETS_GOLD, ASSETS_SILVER
from de_projet_perso.core.data_catalog import DataCatalog, DatasetDerivedConfig
from de_projet_perso.core.logger import logger
from de_projet_perso.core.settings import settings
from de_projet_perso.database.loader import PostgresLoader
from de_projet_perso.pipeline.derived_manager import DerivedDatasetPipeline

# =============================================================================
# Configuration
# =============================================================================

DAG_ID = "gold_installations_meteo"

DEFAULT_ARGS: dict[str, Any] = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=15),
    "depends_on_past": False,
}

# Load Gold dataset from catalog
_catalog = DataCatalog.load(settings.data_catalog_file_path)
_gold_dataset_config = _catalog.get_dataset("installations_meteo")
assert isinstance(_gold_dataset_config, DatasetDerivedConfig), (
    "installations_meteo must be a derived dataset"
)
_gold_dataset: DatasetDerivedConfig = _gold_dataset_config

# =============================================================================
# Assets Definition (imported from centralized assets.py)
# =============================================================================
# Input Silver Assets:
#   - ODRE_INSTALLATIONS_SILVER
#   - IGN_CONTOURS_IRIS_SILVER
#   - METEO_FRANCE_STATIONS_SILVER
# Output Gold Asset:
#   - INSTALLATIONS_METEO_GOLD


# =============================================================================
# DAG Definition
# =============================================================================


@dag(
    dag_id=DAG_ID,
    description="Join renewable installations with nearest weather stations (solar/wind)",
    # AssetAny: triggered when ANY of the 3 Silver assets is updated
    schedule=AssetAny(
        ASSETS_SILVER["odre_installations"],
        ASSETS_SILVER["ign_contours_iris"],
        ASSETS_SILVER["meteo_france_stations"],
    ),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["gold", "analytics", "renewable-energy", "spatial-join"],
    doc_md="""
## Gold Pipeline: installations_meteo

### Description
Creates an analytical dataset linking renewable energy installations
(solar and wind) with their nearest compatible weather station.

### Trigger
**AssetAny**: Runs when ANY of the 3 Silver datasets is updated:
- `odre_installations_silver`
- `ign_contours_iris_silver`
- `meteo_france_stations_silver`

This ensures the Gold layer stays fresh whenever any source data changes.

### Validation
Before transformation, validates that all 3 Silver files exist.
Schema validation is delegated to the Silver layer (fail-fast at source).

### Transformation Logic
1. Load Silver datasets (ODRE, IGN, Meteo France)
2. Filter to solar + wind installations only
3. Join with IGN to get centroid coordinates (via code_iris)
4. Find nearest compatible station (within 20km)
   - Solar installations -> station with `mesure_solaire = True`
   - Wind installations -> station with `mesure_eolien = True`
5. Save to Gold layer with backup rotation

### Output
- **Asset**: `installations_meteo_gold`
- **Path**: `data/gold/installations_meteo/current.parquet`
- **Backup**: `data/gold/installations_meteo/backup.parquet`

### Columns
| Column | Description |
|--------|-------------|
| id_peps | Installation unique identifier |
| nom_installation | Installation name |
| type_energie | 'solaire' or 'eolien' |
| puissance_max_installee | Max installed power (MW) |
| commune, departement, region | Geographic location |
| centroid_lat, centroid_lon | IRIS centroid coordinates (WGS84) |
| station_meteo_id | Nearest compatible weather station ID |
| station_meteo_nom | Weather station name |
| distance_station_km | Distance to station (km) |
""",
)
def gold_installations_meteo_dag():
    """Gold DAG for installations_meteo transformation."""

    @task(outlets=[ASSETS_GOLD["installations_meteo"]])
    def transform_to_gold():
        """Execute Gold transformation using DerivedDatasetPipeline.

        Uses the new DerivedDatasetPipeline which:
        - Validates all Silver dependencies exist
        - Executes the registered transformation
        - Handles backup rotation automatically

        Yields:
            Metadata for the Gold Asset
        """
        # Create manager from catalog dataset
        manager = DerivedDatasetPipeline(_gold_dataset)

        # Execute transformation (validates dependencies + transforms + saves)
        result = manager.to_gold()

        # Compute additional statistics for logging
        df_gold = pl.read_parquet(result.path)
        n_total = result.row_count
        n_with_station = int(df_gold["station_meteo_id"].is_not_null().sum())
        n_without_station = n_total - n_with_station
        n_solaire = len(df_gold.filter(df_gold["type_energie"] == "solaire"))
        n_eolien = len(df_gold.filter(df_gold["type_energie"] == "eolien"))
        coverage_pct = round(n_with_station / n_total * 100, 1) if n_total > 0 else 0

        logger.info(
            "Gold pipeline completed",
            extra={
                "total_installations": n_total,
                "with_station": n_with_station,
                "without_station": n_without_station,
                "coverage_pct": coverage_pct,
                "solaire": n_solaire,
                "eolien": n_eolien,
                "file_size_mb": result.file_size_mib,
                "dependencies": result.dependencies,
            },
        )

        # Emit Asset metadata for Airflow UI
        yield Metadata(
            asset=ASSETS_GOLD["installations_meteo"],
            extra={
                "row_count": n_total,
                "with_station": n_with_station,
                "without_station": n_without_station,
                "coverage_pct": coverage_pct,
                "solaire_count": n_solaire,
                "eolien_count": n_eolien,
                "file_size_mb": result.file_size_mib,
                "dependencies": ", ".join(result.dependencies),  # Convert list to string
            },
        )

    @task
    def load_to_postgres() -> int:
        """Load Gold data to PostgreSQL.

        Returns:
            Number of rows loaded
        """
        logger.info("Loading Gold installations_meteo to PostgreSQL")

        _loader = PostgresLoader()

        # Ensure schema exists (idempotent)
        # loader.initialize_schema()

        # Load Gold table
        # TODO
        # rows = loader.load("installations_meteo", "gold")
        rows = 0

        logger.info(
            "PostgreSQL load completed",
            extra={"table": "gold.installations_meteo", "rows": rows},
        )
        return rows

    # Task dependencies: transform -> load_to_postgres
    transform_to_gold() >> load_to_postgres()


# =============================================================================
# Expose DAG to Airflow
# =============================================================================
dag_instance = gold_installations_meteo_dag()
