"""Todo."""

from datetime import timedelta
from typing import Any

from airflow.sdk import DAG, Asset, dag, task

from de_projet_perso.airflow.assets import ASSETS_SILVER
from de_projet_perso.airflow.dags.error_dag import create_error_dag
from de_projet_perso.core.data_catalog import DataCatalog
from de_projet_perso.core.exception_handler import log_exception_with_extra
from de_projet_perso.core.exceptions import InvalidCatalogError
from de_projet_perso.core.logger import logger
from de_projet_perso.core.path_resolver import PathResolver
from de_projet_perso.core.settings import settings
from de_projet_perso.database.loader import PostgresLoader

TASK_POSTGRES = "load_silver_into_postgres"
TASK_POSTGRES_TIMEOUT = timedelta(minutes=20)


def _create_dag(dataset_name: str, asset: Asset) -> DAG:
    """Todo."""

    @dag(
        dag_id=f"load_silver_into_postgres_{dataset_name}",
        tags=["database", "postgres"],
        schedule=[asset],
        description=f"Automated Postgres load for {dataset_name} triggered by Asset update.",
        doc_md=__doc__,
        # max_active_runs=1 # controlled globally via AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG
    )
    def _dag() -> None:
        @task(task_id=TASK_POSTGRES, execution_timeout=TASK_POSTGRES_TIMEOUT)
        def load_to_postgres_task(dataset_name: str) -> dict[str, Any]:
            """Load Silver data to PostgreSQL if SQL file exists.

            This task is convention-based: if sql/upsert/silver/{dataset_name}.sql
            exists, the data is loaded to PostgreSQL. Otherwise, it skips silently.

            Args:
                dataset_name: Dataset identifier

            Returns:
                Dict with 'skipped' flag and 'rows' count
            """
            loader = PostgresLoader()

            if not loader.has_silver_sql(dataset_name):
                logger.info(
                    "No PostgreSQL loader configured, skipping",
                    extra={"dataset": dataset_name},
                )
                return {"skipped": True, "rows": 0}

            # Initialize schema/tables if needed (idempotent)
            # loader.initialize_schema()
            loader.initialize_silver_tables()

            # Load data
            resolver = PathResolver(dataset_name)
            rows = loader.load(dataset_name, "silver", resolver.silver_current_path)

            logger.info(
                "Loaded Silver to PostgreSQL",
                extra={"dataset": dataset_name, "rows": rows},
            )

            return {"skipped": False, "rows": rows}

        load_to_postgres_task(dataset_name=dataset_name)

    return _dag()


def _generate_all_dags() -> dict[str, DAG]:
    """TODO."""
    try:
        catalog = DataCatalog.load(settings.data_catalog_file_path)
    except InvalidCatalogError as e:
        log_exception_with_extra(e)
        return {"data_catalog_error": create_error_dag("ERROR_LOADING_DATA_CATALOG", str(e))}

    except Exception as e:
        logger.exception(str(e))
        return {"data_catalog_error": create_error_dag("ERROR_LOADING_DATA_CATALOG", str(e))}

    pipelines: dict[str, DAG] = {}

    for name, dataset in catalog.datasets.items():
        try:
            asset = ASSETS_SILVER[dataset.name]

            dag_id = f"dag_{name}"
            dag_obj = _create_dag(name, asset)

            pipelines[name] = dag_obj
            logger.info(f"Created dataset load to postgres DAG: {dag_id}")

        except Exception as e:
            error_msg = f"{type(e).__name__}: {e}"
            logger.exception(f"Failed to create DAG for {name}", extra={"error": error_msg})
            # Create individual error DAG with unique ID
            pipelines[f"{name}_error"] = create_error_dag(
                dag_id=f"ERROR_LOADING_DAG_{name}", error_message=error_msg
            )

    return pipelines


# Note: expose DAGs to Airflow
_generate_all_dags()
