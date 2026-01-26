"""TODO docstring."""

from datetime import datetime, timedelta
from typing import Any, Union

from airflow.sdk import DAG, Asset, Metadata, XComArg, dag, task
from pydantic import TypeAdapter

from de_projet_perso.airflow.assets import get_silver_asset
from de_projet_perso.airflow.dags.error_dag import create_error_dag
from de_projet_perso.core.data_catalog import DataCatalog
from de_projet_perso.core.exception_handler import log_exception_with_extra
from de_projet_perso.core.exceptions import InvalidCatalogError
from de_projet_perso.core.logger import logger
from de_projet_perso.core.settings import settings
from de_projet_perso.pipeline.constants import PipelineAction
from de_projet_perso.pipeline.manager import PipelineManager
from de_projet_perso.pipeline.results import (
    BronzeResult,
    CheckMetadataResult,
    ExtractResult,
    IngestResult,
)
from de_projet_perso.pipeline.state import PipelineStateManager

# =============================================================================
# Production defaults - TODO, move to settings
# =============================================================================

DEFAULT_ARGS: dict[str, Any] = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(seconds=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=30),
    "depends_on_past": False,  # if True then DAGs will freeze if previous execution failed
    # Note: max_active_runs is controlled globally via AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG=1
    # This allows inter-DAG parallelism while preventing intra-DAG conflicts on files
}

TASK_CHECK = "check_remote_for_changes"
TASK_INGEST = "ingest_remote_to_landing"
TASK_EXTRACT = "extract_archive_to_landing"
TASK_BRONZE = "convert_landing_to_bronze"
TASK_SILVER = "clean_bronze_to_silver"
TASK_POSTGRES = "load_silver_to_postgres"

# Task-specific timeouts (override defaults)
TASK_CHECK_TIMEOUT = timedelta(minutes=1)
TASK_INGEST_TIMEOUT = timedelta(minutes=30)
TASK_EXTRACT_TIMEOUT = timedelta(minutes=5)
TASK_BRONZE_TIMEOUT = timedelta(minutes=3)
TASK_SILVER_TIMEOUT = timedelta(minutes=10)
TASK_POSTGRES_TIMEOUT = timedelta(minutes=10)

# Task-specific retries (override defaults)
# if needed ?


def _create_dag(manager: PipelineManager, asset: Asset) -> DAG:
    """Create a production-ready DAG for a dataset.

    Args:
        manager: todo
        asset: Target Asset representing the silver layer output

    Returns:
        Instantiated DAG object
    """

    @dag(
        dag_id=f"ingest_{manager.dataset.name}",
        schedule=manager.dataset.ingestion.frequency.airflow_schedule,
        start_date=datetime(2026, 1, 24),
        catchup=False,  # TODO[prod]: set to True
        default_args=DEFAULT_ARGS,
        tags=[manager.dataset.source.provider, "ingestion"],
        description=manager.dataset.description[:200] if manager.dataset.description else None,
        doc_md=__doc__,
        # max_active_runs=1 # controlled globally via AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG
    )
    def _dag() -> None:
        # ============================================================
        # 1. CHECK REMOTE (HTTP HEAD) - short-circuit if unchanged.
        # ============================================================
        @task.short_circuit(task_id=TASK_CHECK)
        def check_remote_task() -> XComArg | bool:
            """Check if remote data changed (HTTP HEAD) - short-circuit if unchanged."""
            metadata_res = manager.has_dataset_metadata_changed()
            return (
                metadata_res.model_dump(mode="json")
                if metadata_res.action != PipelineAction.SKIP
                else False
            )

        # ============================================================
        # 2. DOWNLOAD + SHA256 CHECK - short-circuit if identical.
        # ============================================================
        @task.short_circuit(task_id=TASK_INGEST, execution_timeout=TASK_INGEST_TIMEOUT)
        def ingest_task(ctx: XComArg, version: str) -> XComArg | bool:
            """Download and check if SHA256 changed - short-circuit if unchanged."""
            check_metadata_ctx = CheckMetadataResult.model_validate(ctx)

            ingest_res = manager.ingest(
                version=version, remote_metadata=check_metadata_ctx.remote_metadata
            )

            if not manager.has_hash_changed(ingest_res):
                return False

            return ingest_res.model_dump(mode="json")

        # ============================================================
        # 3. (Optional) EXTRACTION - only if remote data requires it
        # ============================================================
        @task.short_circuit(task_id=TASK_EXTRACT, execution_timeout=TASK_EXTRACT_TIMEOUT)
        def extract_task(ctx: XComArg) -> XComArg | bool:
            """Extract archive and check if SHA256 changed - short-circuit if unchanged."""
            ingest_ctx = IngestResult.model_validate(ctx)
            extract_res = manager.extract_archive(ingest_ctx)

            if not manager.has_hash_changed(extract_res):
                return False

            return extract_res.model_dump(mode="json")

        # ============================================================
        # 4. CONVERT TO BRONZE - parquet + column names normalisation
        # ============================================================
        @task(task_id=TASK_BRONZE, execution_timeout=TASK_BRONZE_TIMEOUT)
        def bronze_task(ctx: XComArg) -> XComArg:
            """Convert landing file to Parquet with normalized column names."""
            # todo
            bronze_adapter = TypeAdapter(Union[IngestResult, ExtractResult])
            ingest_or_extract_ctx = bronze_adapter.validate_python(ctx)

            bronze_res = manager.to_bronze(ingest_or_extract_ctx)
            return bronze_res.model_dump(mode="json")

        # ============================================================
        # 5. TRANSFORM TO SILVER - Business Rules + Metadata
        # ============================================================
        @task(task_id=TASK_SILVER, execution_timeout=TASK_SILVER_TIMEOUT, outlets=[asset])
        def silver_task(ctx: XComArg):  # -> Generator[Metadata]:
            """Apply business transformations and emit Asset metadata.

            This is the ONLY task that emits Metadata (Airflow 3.x best practice).

            Args:
                ctx: Pipeline context from previous tasks.

            Yields:
                Metadata for the silver Asset with enriched information
            """
            # todo: we don't really need that, atm bronze_dict has more keys than just BronzeResult
            #  and we only need those extra keys here for metadata
            bronze_ctx = BronzeResult.model_validate(ctx)

            # Transform to silver
            silver_res = manager.to_silver(bronze_ctx)

            # TODO: replace by parquet_file_size_mib

            # TODO: move that to manager ?
            PipelineStateManager.update_success(dataset_name=manager.dataset.name, data=silver_res)

            # Emit enriched metadata for Airflow UI
            # These metadata fields will be visible in the Assets tab
            yield Metadata(
                asset=asset,
                extra={
                    **silver_res.model_dump(
                        mode="json", exclude_none=True, exclude={"path", "extracted_file_path"}
                    )
                    | {
                        "state_file": PipelineStateManager.get_state_path(manager.dataset.name),
                    },
                },
            )

        # ============================================================
        # 6. LOAD SILVER TO POSTGRES (if SQL exists for this dataset)
        # ============================================================
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
            # Deferred imports for Airflow task isolation (parse-time vs runtime)
            from de_projet_perso.core.path_resolver import PathResolver  # noqa: PLC0415
            from de_projet_perso.database.loader import PostgresLoader  # noqa: PLC0415

            loader = PostgresLoader()

            if not loader.has_silver_sql(dataset_name):
                logger.info(
                    "No PostgreSQL loader configured, skipping",
                    extra={"dataset": dataset_name},
                )
                return {"skipped": True, "rows": 0}

            # Initialize schema/tables if needed (idempotent)
            loader.initialize_schema()
            loader.initialize_silver_tables()

            # Load data
            resolver = PathResolver(dataset_name)
            rows = loader.load_silver(dataset_name, resolver.silver_current_path)

            logger.info(
                "Loaded Silver to PostgreSQL",
                extra={"dataset": dataset_name, "rows": rows},
            )

            return {"skipped": False, "rows": rows}

        # ============================================================
        # DAG's WORKFLOW - check > ingest > (extract) > bronze > silver > postgres
        # ============================================================
        # 1. CHECK
        remote_metadata = check_remote_task()

        # generate version inside Airflow decorated function so template will be replaced
        run_version = manager.dataset.ingestion.frequency.get_airflow_version_template()

        # 2. INGEST
        landing_data = ingest_task(remote_metadata, run_version)

        # 3. (optional) EXTRACT
        if manager.dataset.source.format.is_archive:
            ready_to_convert = extract_task(landing_data)
        else:
            ready_to_convert = landing_data

        # 4. CONVERT TO BRONZE
        bronze_data = bronze_task(ready_to_convert)

        # 5. TRANSFORM TO SILVER
        silver_result = silver_task(bronze_data)

        # 6. LOAD SILVER TO POSTGRES
        postgres_result = load_to_postgres_task(manager.dataset.name)

        # Explicit dependency: postgres runs after silver
        silver_result >> postgres_result

    return _dag()


def _generate_all_dags() -> dict[str, DAG]:
    """Generate all DAGs from catalog with error handling.

    Returns:
        Dictionary mapping pipeline names to DAG objects.
        If catalog fails to load, returns a single error DAG.
    """
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
            asset = get_silver_asset(dataset_name=dataset.name)
            manager = PipelineManager(dataset=dataset)

            dag_id = f"dag_{name}"
            dag_obj = _create_dag(manager, asset)

            pipelines[name] = dag_obj
            logger.info(f"Created dataset DAG: {dag_id}")

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
