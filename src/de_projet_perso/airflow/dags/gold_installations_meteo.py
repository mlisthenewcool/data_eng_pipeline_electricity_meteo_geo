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
from pathlib import Path
from typing import Any

from airflow.sdk import AssetAny, Metadata, dag, task

from de_projet_perso.airflow.assets import (
    ASSET_GOLD_INSTALLATIONS_METEO,
    ASSET_SILVER_IGN_CONTOURS_IRIS,
    ASSET_SILVER_METEO_FRANCE_STATIONS,
    ASSET_SILVER_ODRE_INSTALLATIONS,
)
from de_projet_perso.core.file_manager import FileManager
from de_projet_perso.core.logger import logger
from de_projet_perso.core.path_resolver import PathResolver
from de_projet_perso.database.loader import PostgresLoader
from de_projet_perso.pipeline.transformations.gold.installations_meteo import (
    transform_gold_installations_meteo,
)

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

# Silver datasets required for this Gold transformation
SILVER_DATASETS = [
    "odre_installations",
    "ign_contours_iris",
    "meteo_france_stations",
]

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
        ASSET_SILVER_ODRE_INSTALLATIONS,
        ASSET_SILVER_IGN_CONTOURS_IRIS,
        ASSET_SILVER_METEO_FRANCE_STATIONS,
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

    @task
    def validate_silver_inputs() -> dict[str, str]:
        """Validate all Silver input files exist.

        Returns:
            Dict mapping dataset names to their Silver file paths

        Raises:
            FileNotFoundError: If any Silver file is missing
        """
        logger.info("Validating Silver inputs existence")

        paths: dict[str, str] = {}

        for dataset_name in SILVER_DATASETS:
            resolver = PathResolver(dataset_name)
            silver_path = resolver.silver_current_path

            if not silver_path.exists():
                raise FileNotFoundError(
                    f"Silver file not found for '{dataset_name}': {silver_path}. "
                    f"Ensure the Silver pipeline has run successfully before "
                    f"triggering the Gold transformation."
                )

            paths[dataset_name] = str(silver_path)
            logger.info(
                f"Validated Silver exists: {dataset_name}",
                extra={
                    "path": str(silver_path),
                    "size_mb": silver_path.stat().st_size / 1024 / 1024,
                },
            )

        logger.info(
            "All Silver inputs validated",
            extra={"datasets": list(paths.keys())},
        )
        return paths

    @task(outlets=[ASSET_GOLD_INSTALLATIONS_METEO])
    def transform_and_save(silver_paths: dict[str, str]):
        """Run Gold transformation and save with backup rotation.

        Args:
            silver_paths: Dict of validated Silver paths from validate task

        Yields:
            Metadata for the Gold Asset
        """
        logger.info("Starting Gold transformation: installations_meteo")

        # Run transformation with validated paths
        df_gold = transform_gold_installations_meteo(
            odre_silver_path=Path(silver_paths["odre_installations"]),
            ign_silver_path=Path(silver_paths["ign_contours_iris"]),
            meteo_silver_path=Path(silver_paths["meteo_france_stations"]),
        )

        # Setup Gold layer paths
        resolver = PathResolver("installations_meteo")
        file_manager = FileManager(resolver)

        # Ensure directory exists
        resolver._gold_dir.mkdir(parents=True, exist_ok=True)

        # Rotate: current -> backup (preserves previous version)
        file_manager.rotate_gold()

        # Write new current
        df_gold.write_parquet(resolver.gold_current_path)

        # Compute statistics for logging and metadata
        n_total = len(df_gold)
        n_with_station = int(df_gold["station_meteo_id"].is_not_null().sum())
        n_without_station = n_total - n_with_station
        n_solaire = len(df_gold.filter(df_gold["type_energie"] == "solaire"))
        n_eolien = len(df_gold.filter(df_gold["type_energie"] == "eolien"))
        coverage_pct = round(n_with_station / n_total * 100, 1) if n_total > 0 else 0
        file_size_mb = round(resolver.gold_current_path.stat().st_size / 1024 / 1024, 2)

        logger.info(
            "Gold transformation completed",
            extra={
                "total_installations": n_total,
                "with_station": n_with_station,
                "without_station": n_without_station,
                "coverage_pct": coverage_pct,
                "solaire": n_solaire,
                "eolien": n_eolien,
                "file_size_mb": file_size_mb,
                "output_path": str(resolver.gold_current_path),
            },
        )

        # Emit Asset metadata for Airflow UI
        yield Metadata(
            asset=ASSET_GOLD_INSTALLATIONS_METEO,
            extra={
                "row_count": n_total,
                "with_station": n_with_station,
                "without_station": n_without_station,
                "coverage_pct": coverage_pct,
                "solaire_count": n_solaire,
                "eolien_count": n_eolien,
                "file_size_mb": file_size_mb,
            },
        )

    @task
    def load_to_postgres() -> int:
        """Load Gold data to PostgreSQL.

        Returns:
            Number of rows loaded
        """
        logger.info("Loading Gold installations_meteo to PostgreSQL")

        loader = PostgresLoader()

        # Ensure schema exists (idempotent)
        loader.initialize_schema()

        # Load Gold table
        rows = loader.load_table("gold.installations_meteo")

        logger.info(
            "PostgreSQL load completed",
            extra={"table": "gold.installations_meteo", "rows": rows},
        )
        return rows

    # Task dependencies: validate -> transform -> load_to_postgres
    silver_paths = validate_silver_inputs()
    # type: ignore below - XComArg is resolved to dict at runtime by Airflow
    transform_and_save(silver_paths) >> load_to_postgres()  # type: ignore[arg-type]


# =============================================================================
# Expose DAG to Airflow
# =============================================================================
dag_instance = gold_installations_meteo_dag()
