"""TODO docstring."""

from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Union

from airflow.sdk import DAG, Asset, Metadata, XComArg, dag, task
from pydantic import TypeAdapter

from de_projet_perso.airflow.assets import ASSETS_SILVER
from de_projet_perso.airflow.dags.error_dag import create_error_dag
from de_projet_perso.core.data_catalog import DataCatalog
from de_projet_perso.core.exception_handler import log_exception_with_extra
from de_projet_perso.core.exceptions import InvalidCatalogError
from de_projet_perso.core.logger import logger
from de_projet_perso.core.settings import settings
from de_projet_perso.pipeline.manager import RemoteDatasetPipeline
from de_projet_perso.pipeline.results import (
    BronzeStageResult,
    DownloadStageResult,
    ExtractionStageResult,
)

if TYPE_CHECKING:
    from airflow.sdk.execution_time.context import InletEventsAccessors

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


def _create_dag(manager: RemoteDatasetPipeline, asset: Asset) -> DAG:
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
        # 2. INGEST WITH SHORT-CIRCUITS IF CONTENT IN UNCHANGED
        #   HTTP HEAD - short-circuit with ETag / Last-modified
        #   DOWNLOAD
        #   COMPARE HASH (SHA256)
        # ============================================================
        @task.short_circuit(
            task_id=TASK_INGEST, execution_timeout=TASK_INGEST_TIMEOUT, inlets=[asset]
        )
        def ingest_task(
            version: str, inlet_events: "InletEventsAccessors | None" = None
        ) -> XComArg | bool:
            """IngestionPolicy strategy with various checks to short-circuit if data is unchanged.

            1. Retrieve metadata from remote
            2. Do we have metadata from previous runs ?
                Yes: Compare it to metadata from 1. - short-circuit if unchanged.
                No: continue to 3.
            3. Download content to landing layer
            4. Are hash (sha256) for both files identical ?
                Yes: Delete content from landing layer AND short-circuit the DAG
                No: continue to next task
            """
            previous_metadata = (
                inlet_events[asset][-1].extra
                if inlet_events and asset in inlet_events and len(inlet_events[asset]) > 0
                else None
            )

            logger.debug("TEST POUR AFFICHAGE")

            ingestion_result = manager.ingest(version=version, previous_metadata=previous_metadata)

            if isinstance(ingestion_result, bool):
                return False

            return ingestion_result.model_dump(mode="json")

        # ============================================================
        # 3. (Optional) EXTRACTION - only if remote data requires it
        # ============================================================
        @task.short_circuit(
            task_id=TASK_EXTRACT, execution_timeout=TASK_EXTRACT_TIMEOUT, inlets=[asset]
        )
        def extract_task(
            ctx: XComArg, inlet_events: "InletEventsAccessors | None" = None
        ) -> XComArg | bool:
            """Extract archive and check if SHA256 changed - short-circuit if unchanged."""
            previous_metadata = (
                inlet_events[asset][-1].extra
                if inlet_events and asset in inlet_events and len(inlet_events[asset]) > 0
                else None
            )

            extract_result = manager.extract_archive(DownloadStageResult.model_validate(ctx))

            # --- IF HASH IS IDENTICAL (extracted file), REMOVE LANDING FILES AND SKIP ---
            if manager.should_skip_extraction(
                extract_result=extract_result, previous_metadata=previous_metadata
            ):
                return False

            return extract_result.model_dump(mode="json")

        # ============================================================
        # 4. CONVERT TO BRONZE - parquet + column names normalisation
        # ============================================================
        @task(task_id=TASK_BRONZE, execution_timeout=TASK_BRONZE_TIMEOUT)
        def bronze_task(ctx: XComArg) -> XComArg:
            """Convert landing file to Parquet with normalized column names."""
            type_adapter = TypeAdapter(Union[DownloadStageResult, ExtractionStageResult])
            return manager.to_bronze(type_adapter.validate_python(ctx)).model_dump(mode="json")

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
            silver_result = manager.to_silver(BronzeStageResult.model_validate(ctx))

            # Emit enriched metadata for Airflow UI
            # These metadata fields will be visible in the Assets tab
            # and will be used to short-circuit refresh on ingest_task

            yield Metadata(
                asset=asset,
                # exclude={"path", "extracted_file_path"}
                extra=manager.create_metadata_emission(silver_result).model_dump(exclude_none=True),
            )

        # ============================================================
        # DAG's WORKFLOW : ingest > (extract) > bronze > silver
        # ============================================================

        # generate version inside Airflow decorated function so template will be replaced
        run_version = manager.dataset.ingestion.frequency.get_airflow_version_template()

        # 1. INGEST
        landing_ctx = ingest_task(run_version)

        # 2. (optional) EXTRACT
        if manager.dataset.source.format.is_archive:
            landing_ctx = extract_task(landing_ctx)

        # 3. CONVERT TO BRONZE
        bronze_ctx = bronze_task(landing_ctx)

        # 4. TRANSFORM TO SILVER
        _ = silver_task(bronze_ctx)

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

    # Only process remote datasets (derived datasets have their own Gold DAGs)
    for dataset in catalog.get_remote_datasets():
        try:
            asset = ASSETS_SILVER[dataset.name]
            manager = RemoteDatasetPipeline(dataset=dataset)

            dag_id = f"dag_{dataset.name}"
            dag_obj = _create_dag(manager, asset)

            pipelines[dataset.name] = dag_obj
            logger.info(f"Created dataset DAG: {dag_id}")

        except Exception as e:
            error_msg = f"{type(e).__name__}: {e}"
            logger.exception(f"Failed to create DAG for {dataset.name}", extra={"error": error_msg})
            # Create individual error DAG with unique ID
            pipelines[f"{dataset.name}_error"] = create_error_dag(
                dag_id=f"ERROR_LOADING_DAG_{dataset.name}", error_message=error_msg
            )

    return pipelines


# Note: expose DAGs to Airflow
_generate_all_dags()
