"""Data pipeline factory for dataset ingestion.

This module generates Airflow DAGs dynamically from the data catalog.
Each dataset in the catalog gets its own DAG with standardized stages:
- State validation and action decision (SKIP/FIRST_RUN/HEAL/RETRY/REFRESH)
- Conditional state cleanup for incoherent states
- Download from source
- Conditional archive extraction (only if needed)
- Bronze layer conversion (Parquet + normalized columns)
- Silver layer transformation with Asset metadata emission

Pipeline Features:
- Intelligent skip logic based on state and data freshness
- Automatic healing of incoherent states
- Conditional extraction with visual branching in Airflow UI
- Automatic cleanup of archive files after extraction
- Full traceability with SHA256 hashing at each step

Best Practices Applied (Airflow 3.x):
- Single Asset per pipeline (silver layer output)
- Metadata emitted only from final task
- Graceful error handling at parse time
- Explicit task dependencies with trigger rules
- Conditional branching for visual clarity
- Production defaults (retries, timeouts, SLAs)
"""

from __future__ import annotations

import traceback
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

from airflow.sdk import DAG, Asset, Metadata, dag, get_current_context, task

from de_projet_perso.airflow.adapters import AirflowTaskAdapter
from de_projet_perso.core.data_catalog import DataCatalog, Dataset
from de_projet_perso.core.exceptions import InvalidCatalogError
from de_projet_perso.core.logger import logger
from de_projet_perso.core.settings import settings
from de_projet_perso.pipeline.decision import PipelineDecisionEngine
from de_projet_perso.pipeline.downloader import PipelineDownloader
from de_projet_perso.pipeline.results import ExtractionResult
from de_projet_perso.pipeline.state import PipelineAction, PipelineStateManager
from de_projet_perso.pipeline.transformer import PipelineTransformer
from de_projet_perso.pipeline.validator import PipelineValidator

if TYPE_CHECKING:
    pass


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

        return "validate_state_coherence"

    @task(task_id="mark_skipped")
    def mark_skipped_task() -> dict[str, Any]:
        """Mark pipeline as skipped with detailed reason."""
        state = PipelineStateManager.load(dataset_name)

        # Calculer la raison détaillée
        reason_parts = ["SKIP"]

        if state and state.last_successful_run:
            last_success = state.last_successful_run.timestamp
            age = datetime.now() - last_success
            frequency_delta = PipelineDecisionEngine.frequency_to_timedelta(
                dataset.ingestion.frequency
            )

            reason_parts.append(f"last success: {last_success.isoformat()}")
            if frequency_delta:
                reason_parts.append(
                    f"data age: {age.total_seconds() / 3600:.1f}h < "
                    f"frequency: {frequency_delta.total_seconds() / 3600:.0f}h"
                )

        reason = " - ".join(reason_parts)

        logger.info(f"Pipeline skipped for {dataset_name}", extra={"reason": reason})

        return {
            "status": "skipped",
            "dataset": dataset_name,
            "action": PipelineAction.SKIP.value,
            "reason": reason,
        }

    @task(task_id="cleanup_incoherent_state")
    def cleanup_incoherent_state_task() -> dict[str, Any]:
        """Cleanup incoherent state and prepare for healing.

        This task is triggered when state validation detects incoherence.
        It removes the corrupted state file to allow a fresh start (HEAL action).

        Returns:
            Dict with cleanup status and action taken
        """
        state_path = PipelineStateManager.get_state_path(dataset_name)

        logger.warning(
            "Cleaning up incoherent state file",
            extra={"dataset": dataset_name, "state_file": str(state_path), "action": "HEAL"},
        )

        # Remove corrupted state file
        if state_path.exists():
            state_path.unlink()
            logger.info("Removed incoherent state file")

        return {
            "status": "cleaned",
            "dataset": dataset_name,
            "action": PipelineAction.HEAL.value,
            "state_file_removed": str(state_path),
        }

    @task(
        task_id="download_data",
        execution_timeout=DOWNLOAD_TIMEOUT,
        retries=3,  # More retries for network operations
        trigger_rule="none_failed_min_one_success",  # Can come from validate OR cleanup
    )
    def download_data_task() -> dict:
        """Download source file from URL with retry logic.

        Returns:
            Dict serialized for XCom (DownloadResult → dict)
        """
        result = PipelineDownloader.download(dataset_name=dataset_name, dataset=dataset)
        return AirflowTaskAdapter.to_xcom(result)

    @task.branch(task_id="check_extraction_needed")
    def check_extraction_needed_task() -> str:
        """Determine if archive extraction is needed.

        This task decides the extraction path based on dataset format.
        Decision is based on dataset configuration (not XCom data).

        Returns:
            Task ID: "extract_archive" if archive format, "skip_extraction" otherwise
        """
        if dataset.source.format.is_archive:
            logger.info(
                "Archive format detected, extraction needed",
                extra={"format": str(dataset.source.format)},
            )
            return "extract_archive"
        else:
            logger.info(
                "Non-archive format, skipping extraction",
                extra={"format": str(dataset.source.format)},
            )
            return "skip_extraction"

    @task(
        task_id="extract_archive",
        execution_timeout=EXTRACT_TIMEOUT,
    )
    def extract_archive_task(download_data: dict) -> dict:
        """Extract archive, cleanup .7z file, and validate landing.

        Args:
            download_data: XCom dict from download task

        Returns:
            Dict serialized for XCom (LandingResult → dict)
        """
        # Deserialize typed result from XCom
        download_result = AirflowTaskAdapter.from_xcom_download(download_data)

        archive_path = download_result.path

        # Extract archive
        extract_result = PipelineDownloader.extract_archive(
            archive_path=archive_path,
            dataset=dataset,
            archive_sha256=download_result.sha256,  # Pass SHA256 explicitly
        )

        # Cleanup archive after successful extraction
        try:
            if archive_path.exists() and archive_path.suffix == ".7z":
                archive_path.unlink()
                logger.info(
                    "Removed archive after successful extraction",
                    extra={"archive": str(archive_path)},
                )
        except Exception as e:
            logger.warning(
                "Failed to remove archive (non-critical)",
                extra={"archive": str(archive_path), "error": str(e)},
            )

        # Validate landing
        landing_result = PipelineDownloader.validate_landing(extract_result)

        return AirflowTaskAdapter.to_xcom(landing_result)

    @task(task_id="skip_extraction")
    def skip_extraction_task(download_data: dict) -> dict:
        """Skip extraction for non-archive formats and validate landing.

        Args:
            download_data: XCom dict from download task

        Returns:
            Dict serialized for XCom (LandingResult → dict)
        """
        download_result = AirflowTaskAdapter.from_xcom_download(download_data)

        logger.info(
            "No extraction needed, file already in final format",
            extra={"path": str(download_result.path), "format": str(dataset.source.format)},
        )

        # Create ExtractionResult from DownloadResult (no extraction)
        extract_result = ExtractionResult(
            path=download_result.path,
            size_mib=download_result.size_mib,
            extracted_sha256=download_result.sha256,
            archive_sha256=download_result.sha256,  # Same SHA256 (no archive)
        )

        # Validate landing
        landing_result = PipelineDownloader.validate_landing(extract_result)

        return AirflowTaskAdapter.to_xcom(landing_result)

    @task(task_id="merge_landing_results", trigger_rule="none_failed_min_one_success")
    def merge_landing_results_task() -> dict:
        """Merge landing results from whichever extraction path succeeded.

        This task handles XCom routing for branched extraction paths.
        Only one of extract_archive or skip_extraction will have executed.

        Uses get_current_context() to access TaskInstance for XCom pulling.

        Returns:
            Dict with landing result from the successful extraction path

        Raises:
            ValueError: If no landing data found from either extraction path
        """
        # Get Airflow context and TaskInstance
        context = get_current_context()
        ti = context["ti"]

        # Try extract_archive first
        landing_data = ti.xcom_pull(task_ids="extract_archive")
        if landing_data is not None:
            logger.info("Using landing data from extract_archive path")
            return landing_data

        # Fallback to skip_extraction
        landing_data = ti.xcom_pull(task_ids="skip_extraction")
        if landing_data is not None:
            logger.info("Using landing data from skip_extraction path")
            return landing_data

        # Should never happen with correct trigger_rule
        raise ValueError(
            "No landing data found from extraction tasks (extract_archive or skip_extraction)"
        )

    @task(
        task_id="convert_to_bronze",
        execution_timeout=TRANSFORM_TIMEOUT,
    )
    def convert_to_bronze_task(landing_data: dict) -> dict:
        """Convert to Parquet with normalized column names.

        Args:
            landing_data: XCom dict from merge_landing_results task

        Returns:
            Dict serialized for XCom (BronzeResult → dict)
        """
        # Deserialize typed result from XCom
        landing_result = AirflowTaskAdapter.from_xcom_landing(landing_data)

        bronze_result = PipelineTransformer.to_bronze(
            landing_result=landing_result, dataset_name=dataset_name, dataset=dataset
        )

        return AirflowTaskAdapter.to_xcom(bronze_result)

    @task.branch(task_id="validate_state_coherence")
    def validate_state_coherence_task() -> str:
        """Log state summary and validate coherence.

        This task combines state logging and validation:
        1. Logs current pipeline state for visibility in Airflow UI
        2. Validates that state matches reality on disk
        3. Branches to cleanup if incoherent, or continues to download

        Validations performed:
        - Expected file exists on disk
        - Path in state matches expected path
        - State file itself exists

        Returns:
            Task ID to execute next: "download_data" or "cleanup_incoherent_state"
        """
        # 1. Log state summary (replaces old log_state_summary_task)
        state = PipelineStateManager.load(dataset_name)

        if state is None:
            summary = {
                "status": "no_state",
                "action": PipelineAction.FIRST_RUN.value,
                "dataset": dataset_name,
            }
            logger.info("No state found - first run", extra=summary)
            # No validation possible, proceed directly to download
            return "download_data"

        # Log state summary
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

        # 2. Validate coherence
        result = PipelineValidator.validate_state_coherence(
            dataset_name=dataset_name,
            dataset=dataset,
            data_dir=settings.data_dir_path,
        )

        # 3. Branch based on validation result
        if result.coherent:
            logger.info("State is coherent, proceeding to download")
            return "download_data"
        else:
            logger.warning(
                "State is incoherent, will cleanup and heal", extra={"issues": result.issues}
            )
            return "cleanup_incoherent_state"

    @task(
        task_id="transform_to_silver",
        outlets=[asset],
        execution_timeout=TRANSFORM_TIMEOUT,
    )
    def transform_to_silver_task(bronze_data: dict):
        """Apply business transformations and emit Asset metadata.

        This is the ONLY task that emits Metadata (Airflow 3.x best practice).

        Uses get_current_context() to access TaskInstance for action inference.

        Args:
            bronze_data: XCom dict from bronze transformation task

        Yields:
            Metadata for the silver Asset with enriched information
        """
        # Deserialize typed result from XCom
        bronze_result = AirflowTaskAdapter.from_xcom_bronze(bronze_data)

        # Track start time for duration calculation
        start_time = datetime.now()

        # Transform to silver
        silver_result = PipelineTransformer.to_silver(
            bronze_result=bronze_result, dataset_name=dataset_name, dataset=dataset
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
        try:
            context = get_current_context()
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
        "validate_state_coherence": validate_state_coherence_task,
        "cleanup_incoherent_state": cleanup_incoherent_state_task,
        "download_data": download_data_task,
        "check_extraction_needed": check_extraction_needed_task,
        "extract_archive": extract_archive_task,
        "skip_extraction": skip_extraction_task,
        "merge_landing_results": merge_landing_results_task,
        "convert_to_bronze": convert_to_bronze_task,
        "transform_to_silver": transform_to_silver_task,
    }


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

**Decision & Validation:**
1. **Decide Action** - Determine pipeline action (SKIP/FIRST_RUN/HEAL/RETRY/REFRESH)
   - SKIP: Data is current, no action needed
   - FIRST_RUN: No state file, first execution
   - HEAL: Expected files missing on disk
   - RETRY: Previous run failed
   - REFRESH: Data is stale based on frequency
2. **Mark Skipped** - Log detailed skip reason (if SKIP)
3. **Validate State Coherence** - Log state + verify coherence with disk
4. **Cleanup Incoherent State** - Remove corrupted state file (if incoherent)

**Data Processing:**
5. **Download Data** - Fetch from source URL with retry logic
6. **Check Extraction Needed** - Branch based on file format (is_archive)
7. **Extract Archive** - Extract file + cleanup .7z archive (if archive format)
8. **Skip Extraction** - Direct landing validation (if non-archive format)
9. **Merge Landing Results** - Route XCom from successful extraction path
10. **Convert to Bronze** - Transform to Parquet with normalized columns
11. **Transform to Silver** - Apply business rules + emit Asset metadata

### Conditional Paths
- **Skip Path**: decide_action → mark_skipped (END)
- **Heal Path**: validate → cleanup → download → ...
- **Archive Path**: download → check → extract_archive → merge → bronze → silver
- **Non-Archive Path**: download → check → skip_extraction → merge → bronze → silver
        """,
    )
    def dataset_pipeline() -> None:
        """Data pipeline with conditional extraction and state validation.

        Flow:
        decide_action -> mark_skipped (END)
                      -> validate_state_coherence -> download_data
                                                  -> cleanup_incoherent_state -> download_data
        download_data -> check_extraction_needed -> extract_archive -> merge_landing_results
                                                 -> skip_extraction -> merge_landing_results
        merge_landing_results -> convert_to_bronze -> transform_to_silver (END)
        """
        # 1. Decide action (branch)
        branch = tasks["decide_action"]()

        # 2. Skip path (END)
        skip = tasks["mark_skipped"]()

        # 3. Validate state and cleanup if needed
        validate = tasks["validate_state_coherence"]()
        cleanup = tasks["cleanup_incoherent_state"]()
        download = tasks["download_data"]()

        # 4. Extraction branching
        check_extract = tasks["check_extraction_needed"]()
        extract = tasks["extract_archive"](download)
        skip_extract = tasks["skip_extraction"](download)

        # 5. Merge extraction results
        merge_landing = tasks["merge_landing_results"]()

        # 6. Transformation
        bronze = tasks["convert_to_bronze"](merge_landing)
        _silver = tasks["transform_to_silver"](bronze)  # Final task with outlet

        # 7. Define branching graph
        branch >> [skip, validate]
        validate >> [cleanup, download]
        cleanup >> download
        download >> check_extract
        check_extract >> [extract, skip_extract]
        [extract, skip_extract] >> merge_landing
        merge_landing >> bronze

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
