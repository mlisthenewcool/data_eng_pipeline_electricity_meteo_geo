"""DAG to load reference tables to PostgreSQL.

This DAG loads Silver data to PostgreSQL reference tables (ref.* schema).
Triggered when the corresponding Silver asset is updated.

For V1, only loads:
- ref.stations_meteo (from meteo_france_stations Silver)
"""

from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import dag, task

from de_projet_perso.airflow.assets import METEO_FRANCE_STATIONS_SILVER
from de_projet_perso.core.logger import logger
from de_projet_perso.database.loader import PostgresLoader

# =============================================================================
# Configuration
# =============================================================================

DAG_ID = "load_ref_stations_meteo"

DEFAULT_ARGS: dict[str, Any] = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=10),
    "depends_on_past": False,
}


# =============================================================================
# DAG Definition
# =============================================================================


@dag(
    dag_id=DAG_ID,
    description="Load weather stations reference table to PostgreSQL",
    schedule=METEO_FRANCE_STATIONS_SILVER,  # Triggered when Silver is updated
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["postgres", "reference", "meteo"],
    doc_md="""
## Reference Table Loader: stations_meteo

### Description
Loads the Météo France weather stations from Silver layer to PostgreSQL.
This reference table is used for joins in API queries and dashboards.

### Trigger
Runs when `meteo_france_stations_silver` Asset is updated.

### Target Table
- **Schema**: ref
- **Table**: stations_meteo
- **Strategy**: UPSERT (insert new, update changed)

### Source
- Silver: `data/silver/meteo_france_stations/current.parquet`
""",
)
def load_ref_stations_meteo_dag():
    """DAG to load ref.stations_meteo from Silver."""

    @task
    def load_stations_meteo() -> int:
        """Load weather stations to PostgreSQL.

        Returns:
            Number of rows loaded
        """
        logger.info("Loading ref.stations_meteo to PostgreSQL")

        loader = PostgresLoader()

        # Ensure schema exists (idempotent)
        loader.initialize_schema()

        # Load reference table
        rows = loader.load_table("ref.stations_meteo")

        logger.info(
            "PostgreSQL load completed",
            extra={"table": "ref.stations_meteo", "rows": rows},
        )
        return rows

    load_stations_meteo()


# =============================================================================
# Expose DAG to Airflow
# =============================================================================
dag_instance = load_ref_stations_meteo_dag()
