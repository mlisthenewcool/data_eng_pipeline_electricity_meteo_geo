"""Data pipeline factory for dataset ingestion.

This module generates Airflow DAGs dynamically from the data catalog.
Each dataset in the catalog gets its own DAG with standardized stages:
- Download from source
- Archive extraction (if needed)
- Landing layer storage
- Bronze layer conversion (Parquet + normalized columns)
- Silver layer transformation
- Silver layer transformation

Best Practices Applied (Airflow 3.x):
- Single Asset per pipeline (silver layer output)
- Metadata emitted only from final task
- Graceful error handling at parse time
- Explicit task dependencies with trigger rules
- Production defaults (retries, timeouts, SLAs)
"""

from __future__ import annotations

import traceback
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

from airflow.sdk import DAG, Asset, Metadata, dag, task

from de_projet_perso.airflow.adapters import AirflowTaskAdapter
from de_projet_perso.core.exceptions import InvalidCatalogError
from de_projet_perso.core.logger import logger
from de_projet_perso.core.settings import settings
from de_projet_perso.datacatalog import DataCatalog, Dataset
from de_projet_perso.pipeline.decision import PipelineDecisionEngine
from de_projet_perso.pipeline.downloader import PipelineDownloader
from de_projet_perso.pipeline.state import PipelineAction, PipelineStateManager
from de_projet_perso.pipeline.transformer import PipelineTransformer
from de_projet_perso.pipeline.validator import PipelineValidator

if TYPE_CHECKING:
    from collections.abc import Generator


# =============================================================================
# Production Defaults
# =============================================================================

DEFAULT_ARGS: dict[str, Any] = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=2),
    "depends_on_past": False,
}

# Task-specific timeouts (override defaults)
DOWNLOAD_TIMEOUT = timedelta(hours=1)
EXTRACT_TIMEOUT = timedelta(minutes=30)
TRANSFORM_TIMEOUT = timedelta(minutes=45)


# =============================================================================
# Action Decision Logic - Now delegated to PipelineDecisionEngine
# =============================================================================
# Functions moved to de_projet_perso.pipeline.decision module for better separation


# =============================================================================
# Error DAG Factory
# =============================================================================


def _create_error_dag(dag_id: str, error_message: str) -> DAG:
    """Create a placeholder DAG that surfaces the error in Airflow UI.

    This prevents Airflow scheduler from crashing while making the error
    visible to operators via the UI.

    Args:
        dag_id: Unique DAG identifier (includes dataset name for uniqueness)
        error_message: Error to display
    """

    @dag(
        dag_id=dag_id,
        description=f"FAILED: {error_message[:200]}",
        schedule=None,
        start_date=datetime(2025, 1, 1),
        catchup=False,
        tags=["error", "data-pipeline", "needs-attention"],
        default_args={"owner": "data-engineering"},
    )
    def error_dag() -> None:
        @task
        def report_error() -> None:
            """Surfaces the catalog/DAG creation error."""
            raise RuntimeError(f"Pipeline creation failed: {error_message}")

        report_error()

    return error_dag()


# =============================================================================
# Asset Factory
# =============================================================================


def _create_asset_for_dataset(name: str, dataset: Dataset) -> Asset:
    """Create an Asset representing the silver layer output for a dataset.

    Args:
        name: Dataset identifier
        dataset: Dataset configuration

    Returns:
        Asset configured for the silver layer output
    """
    return Asset(
        name=f"{name}_silver",
        uri=f"file:///{settings.data_dir_path / dataset.get_storage_path('silver')}",
        group="data-pipeline",
        extra={
            "provider": dataset.source.provider,
            "format": str(dataset.source.format),
            "version": dataset.ingestion.version,
            "description": dataset.description,
        },
    )


# =============================================================================
# Task Factory Functions
# =============================================================================


def create_pipeline_tasks(dataset_name: str, dataset: Dataset, asset: Asset):  # noqa: PLR0915
    """Create all tasks for a dataset pipeline.

    This factory approach keeps task definitions close to DAG creation,
    avoiding module-level decorated functions that can cause issues
    with dynamic DAG generation.

    Args:
        dataset_name: Dataset identifier
        dataset: Dataset configuration
        asset: Target Asset for the silver layer

    Returns:
        Dict of task functions keyed by task_id
    """

    @task.branch(task_id="decide_action")
    def decide_action_task() -> str:
        """Branch based on pipeline state.

        Returns task_id of next task to execute.
        """
        action = PipelineDecisionEngine.decide_action(dataset_name, dataset, settings.data_dir_path)

        if action == PipelineAction.SKIP:
            return "mark_skipped"

        return "log_state_summary"

    @task(task_id="log_state_summary")
    def log_state_summary_task() -> dict[str, Any]:
        """Log current pipeline state for Airflow UI visibility.

        This task provides structured logging of the pipeline state,
        making it easy to understand the current state directly from
        the Airflow UI without accessing the JSON files.

        The logged information includes:
        - Last successful/failed run timestamps
        - Current version
        - History count
        - Path to state file for manual inspection

        Returns:
            Dict with state summary information
        """
        state = PipelineStateManager.load(dataset_name)
        if state is None:
            summary = {
                "status": "no_state",
                "action": PipelineAction.FIRST_RUN.value,
                "dataset": dataset_name,
            }
            logger.info("No state found - first run", extra=summary)
            return summary

        summary = {
            "dataset": dataset_name,
            "last_success": (
                state.last_successful_run.timestamp.isoformat()
                if state.last_successful_run
                else None
            ),
            "last_failure": (
                state.last_failed_run.timestamp.isoformat() if state.last_failed_run else None
            ),
            "current_version": state.current_version,
            "history_count": len(state.history),
            "state_file": str(PipelineStateManager.get_state_path(dataset_name)),
        }

        logger.info("Pipeline state summary", extra=summary)
        return summary

    @task(task_id="mark_skipped")
    def mark_skipped_task() -> dict[str, str]:
        """Mark pipeline as skipped when data is current."""
        logger.info(f"Pipeline skipped for {dataset_name} - data is current")
        return {"status": "skipped", "dataset": dataset_name}

    @task(
        task_id="download_data",
        execution_timeout=DOWNLOAD_TIMEOUT,
        retries=3,  # More retries for network operations
    )
    def download_data_task() -> dict:
        """Download source file from URL with retry logic.

        Returns:
            Dict serialized for XCom (DownloadResult → dict)
        """
        dest_dir = settings.data_dir_path / "landing" / dataset_name

        result = PipelineDownloader.download(
            dataset_name=dataset_name,
            dataset=dataset,
            dest_dir=dest_dir,
        )

        return AirflowTaskAdapter.to_xcom(result)

    @task(
        task_id="extract_archive",
        execution_timeout=EXTRACT_TIMEOUT,
    )
    def extract_archive_task(download_data: dict) -> dict:
        """Extract archive if format requires it.

        Args:
            download_data: XCom dict from download task

        Returns:
            Dict serialized for XCom (ExtractionResult → dict)
        """
        # Deserialize typed result from XCom
        download_result = AirflowTaskAdapter.from_xcom_download(download_data)

        dest_dir = settings.data_dir_path / "landing" / dataset_name

        result = PipelineDownloader.extract_archive(
            archive_path=download_result.path,
            dataset=dataset,
            dest_dir=dest_dir,
            archive_sha256=download_result.sha256,  # Pass SHA256 explicitly
        )

        return AirflowTaskAdapter.to_xcom(result)

    @task(task_id="save_to_landing")
    def save_to_landing_task(extract_data: dict) -> dict:
        """Validate and record landing layer file.

        Args:
            extract_data: XCom dict from extraction task

        Returns:
            Dict serialized for XCom (LandingResult → dict)
        """
        # Deserialize typed result from XCom
        extract_result = AirflowTaskAdapter.from_xcom_extraction(extract_data)

        landing_result = PipelineDownloader.validate_landing(extract_result)
        return AirflowTaskAdapter.to_xcom(landing_result)

    @task(
        task_id="convert_to_bronze",
        execution_timeout=TRANSFORM_TIMEOUT,
    )
    def convert_to_bronze_task(landing_data: dict) -> dict:
        """Convert to Parquet with normalized column names.

        Args:
            landing_data: XCom dict from landing validation task

        Returns:
            Dict serialized for XCom (BronzeResult → dict)
        """
        # Deserialize typed result from XCom
        landing_result = AirflowTaskAdapter.from_xcom_landing(landing_data)

        bronze_dir = settings.data_dir_path / "bronze" / dataset_name

        bronze_result = PipelineTransformer.to_bronze(
            landing_result=landing_result,
            dataset_name=dataset_name,
            dataset=dataset,
            bronze_dir=bronze_dir,
        )

        return AirflowTaskAdapter.to_xcom(bronze_result)

    @task(task_id="validate_state_coherence")
    def validate_state_coherence_task() -> dict[str, Any]:
        """Verify state file matches reality on disk.

        This validation task ensures that the state recorded in JSON
        files is coherent with the actual files present on disk,
        helping detect data integrity issues early.

        Validations performed:
        - Expected file exists on disk
        - Path in state matches expected path
        - State file itself exists

        Returns:
            Dict with validation results including:
            - coherent: bool indicating if validation passed
            - issues: list of detected problems
            - expected_path: path that should exist
        """
        result = PipelineValidator.validate_state_coherence(
            dataset_name=dataset_name,
            dataset=dataset,
            data_dir=settings.data_dir_path,
        )
        return result.to_dict()

    @task(
        task_id="transform_to_silver",
        outlets=[asset],
        execution_timeout=TRANSFORM_TIMEOUT,
    )
    def transform_to_silver_task(
        bronze_data: dict, context=None
    ) -> Generator[Metadata, None, None]:
        """Apply business transformations and emit Asset metadata.

        This is the ONLY task that emits Metadata (Airflow 3.x best practice).

        Args:
            bronze_data: XCom dict from bronze transformation task
            context: Airflow task context

        Yields:
            Metadata for the silver Asset with enriched information
        """
        # Deserialize typed result from XCom
        bronze_result = AirflowTaskAdapter.from_xcom_bronze(bronze_data)

        silver_dir = settings.data_dir_path / "silver" / dataset_name

        # Track start time for duration calculation
        start_time = datetime.now()

        # Transform to silver
        silver_result = PipelineTransformer.to_silver(
            bronze_result=bronze_result,
            dataset_name=dataset_name,
            dataset=dataset,
            silver_dir=silver_dir,
        )

        # Update state with full traceability
        PipelineStateManager.update_success(
            dataset_name=dataset_name,
            version=dataset.ingestion.version,
            silver_path=silver_result.path,
            row_count=silver_result.row_count,
            columns=silver_result.columns,
            sha256=silver_result.sha256,  # Landing file SHA256
        )

        # Calculate run duration
        end_time = datetime.now()
        run_duration = (end_time - start_time).total_seconds()

        # Infer which action triggered this run
        action_taken = PipelineAction.FIRST_RUN
        if context:
            try:
                ti = context.get("ti")
                if ti:
                    state = PipelineStateManager.load(dataset_name)
                    action_taken = PipelineDecisionEngine.infer_action_from_state(
                        state, dataset, settings.data_dir_path
                    )
            except Exception:
                # Fallback to FIRST_RUN if context unavailable or state unreadable
                pass

        # Emit enriched metadata for Airflow UI
        # These metadata fields will be visible in the Assets tab
        yield Metadata(
            asset=asset,
            extra={
                # Dataset metrics
                "row_count": silver_result.row_count,
                "columns": list(silver_result.columns),
                "version": dataset.ingestion.version,
                # Integrity & traceability
                "file_sha256": silver_result.sha256,  # SHA256 of extracted/landing file
                "archive_sha256": silver_result.archive_sha256,  # SHA256 of original archive
                # Run metadata
                "completed_at": end_time.isoformat(),
                "status": "success",
                "action_taken": str(action_taken),
                "run_duration_seconds": round(run_duration, 2),
                # Paths for reference
                "silver_path": str(silver_result.path),
                "state_file": str(PipelineStateManager.get_state_path(dataset_name)),
            },
        )

    return {
        "decide_action": decide_action_task,
        "mark_skipped": mark_skipped_task,
        "log_state_summary": log_state_summary_task,
        "validate_state_coherence": validate_state_coherence_task,
        "download_data": download_data_task,
        "extract_archive": extract_archive_task,
        "save_to_landing": save_to_landing_task,
        "convert_to_bronze": convert_to_bronze_task,
        "transform_to_silver": transform_to_silver_task,
    }


# Business logic functions moved to pipeline modules:
# - _infer_action_from_state() -> PipelineDecisionEngine.infer_action_from_state()
# - _update_pipeline_state() -> PipelineStateManager.update_success()


# =============================================================================
# DAG Factory
# =============================================================================


def create_dataset_pipeline_dag(
    dataset_name: str,
    dataset: Dataset,
    asset: Asset,
) -> DAG:
    """Create a production-ready DAG for a dataset.

    Args:
        dataset_name: Unique identifier for the dataset
        dataset: Dataset configuration from catalog
        asset: Target Asset representing the silver layer output

    Returns:
        Instantiated DAG object
    """
    tasks = create_pipeline_tasks(dataset_name, dataset, asset)

    desc = dataset.description[:200] if dataset.description else f"Pipeline for {dataset_name}"

    @dag(
        dag_id=f"dataset_pipeline_{dataset_name}",
        description=desc,
        schedule=dataset.ingestion.frequency.airflow_schedule,
        start_date=datetime(2025, 1, 1),
        catchup=False,
        default_args=DEFAULT_ARGS,
        max_active_runs=1,  # Prevent concurrent runs
        tags=[
            dataset.source.provider,
            "data-pipeline",
            str(dataset.ingestion.frequency),
        ],
        doc_md=f"""
## Dataset Pipeline: {dataset_name}

**Provider:** {dataset.source.provider}
**Format:** {dataset.source.format}
**Frequency:** {dataset.ingestion.frequency}
**Version:** {dataset.ingestion.version}

### Pipeline Stages
1. **Decide Action** - Check state and determine if refresh needed
2. **Log State Summary** - Log current state for visibility
3. **Validate State Coherence** - Verify state matches disk reality
4. **Download** - Fetch from source URL
5. **Extract** - Extract from archive if needed
6. **Landing** - Store raw file
7. **Bronze** - Convert to Parquet with normalized columns
8. **Silver** - Apply transformations and emit Asset metadata
        """,
    )
    def dataset_pipeline() -> None:
        """Data pipeline: decide -> log -> validate -> download -> extract -> bronze -> silver."""
        # Branch decision
        branch = tasks["decide_action"]()

        # Skip path
        skip = tasks["mark_skipped"]()

        # Main pipeline path with XCom data flow
        log_state = tasks["log_state_summary"]()
        validate = tasks["validate_state_coherence"]()
        download = tasks["download_data"]()
        extract = tasks["extract_archive"](download)
        landing = tasks["save_to_landing"](extract)
        bronze = tasks["convert_to_bronze"](landing)
        _silver = tasks["transform_to_silver"](bronze)  # Final task with outlet

        # Define branching graph
        branch >> [skip, log_state]
        log_state >> validate >> download

    return dataset_pipeline()


# =============================================================================
# Module-Level DAG Generation
# =============================================================================


def _generate_all_pipelines() -> dict[str, DAG]:
    """Generate all DAGs from catalog with error handling.

    Returns:
        Dictionary mapping pipeline names to DAG objects.
        If catalog fails to load, returns a single error DAG.
    """
    try:
        catalog = DataCatalog.load(settings.data_catalog_file_path)
    except InvalidCatalogError as e:
        logger.exception(
            "Failed to load data catalog",
            extra={"path": str(settings.data_catalog_file_path), "errors": e.validation_errors},
        )
        return {"catalog_error": _create_error_dag("catalog_load_error", str(e))}
    except Exception as e:
        logger.exception("Unexpected error loading catalog")
        return {"catalog_error": _create_error_dag("catalog_load_error", str(e))}

    pipelines: dict[str, DAG] = {}

    for name, dataset in catalog.datasets.items():
        try:
            asset = _create_asset_for_dataset(name, dataset)
            dag_obj = create_dataset_pipeline_dag(name, dataset, asset)
            pipelines[name] = dag_obj
            logger.info(f"Created pipeline DAG: dataset_pipeline_{name}")
        except Exception as e:
            error_msg = f"{type(e).__name__}: {e}"
            logger.exception(
                f"Failed to create DAG for {name}",
                extra={"error": error_msg, "traceback": traceback.format_exc()},
            )
            # Create individual error DAG with unique ID
            pipelines[f"{name}_error"] = _create_error_dag(
                dag_id=f"dataset_pipeline_{name}_error",
                error_message=error_msg,
            )

    return pipelines


# =============================================================================
# The Single Global Variable (Required by Airflow)
# =============================================================================
_generate_all_pipelines()
