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

from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

from airflow.sdk import DAG, Asset, Metadata, dag, get_current_context, task

from de_projet_perso.airflow.adapters import AirflowTaskAdapter
from de_projet_perso.core.data_catalog import Dataset
from de_projet_perso.core.logger import logger
from de_projet_perso.pipeline.decision import PipelineDecisionEngine
from de_projet_perso.pipeline.downloader import PipelineDownloader
from de_projet_perso.pipeline.results import ExtractionResult
from de_projet_perso.pipeline.state import PipelineAction, PipelineStateManager
from de_projet_perso.pipeline.transformer import PipelineTransformer
from de_projet_perso.pipeline.validator import PipelineValidator

if TYPE_CHECKING:
    pass


# =============================================================================
# Production Defaults TODO
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
        uri=f"file:///{dataset.get_silver_path()}",
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


# TODO: typer proprement le retour
def create_common_tasks(  # noqa: PLR0915
    dataset_name: str, dataset: Dataset, asset: Asset
) -> dict[str, Any]:
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

    @task.short_circuit(task_id="check_should_run")
    def check_should_run() -> bool:
        """Short-circuit the entire pipeline if action is SKIP.

        This task evaluates the pipeline decision (SKIP/FIRST_RUN/HEAL/RETRY/REFRESH).
        If the action is SKIP, it logs detailed skip information and returns False,
        which causes Airflow to skip all downstream tasks.

        Returns:
            bool: True to continue pipeline, False to skip all downstream tasks
        """
        action = PipelineDecisionEngine.decide_action(dataset_name, dataset)

        if action == PipelineAction.SKIP:
            # Log detailed skip reason before short-circuiting
            state = PipelineStateManager.load(dataset_name)
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
            logger.info(
                f"Pipeline skipped for {dataset_name}",
                extra={
                    "reason": reason,
                    "action": PipelineAction.SKIP.value,
                    "dataset": dataset_name,
                },
            )
            return False  # Short-circuit: skip all downstream tasks

        # Continue with pipeline execution
        logger.info(
            f"Pipeline will execute for {dataset_name}",
            extra={"action": action.value, "dataset": dataset_name},
        )
        return True

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
        result = PipelineDownloader.download(dataset)
        return AirflowTaskAdapter.to_xcom(result)

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
        3. Branches to clean up if incoherent, or continues to download

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
            dataset_name=dataset_name, dataset=dataset
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
                action_taken = PipelineDecisionEngine.infer_action_from_state(state, dataset)
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

    @task(task_id="validate_landing")
    def validate_landing_task(download_data: dict) -> dict:
        """Validate landing for simple (non-archive) datasets.

        Converts DownloadResult to LandingResult for non-archive formats.

        Args:
            download_data: XCom dict from download task

        Returns:
            Dict serialized for XCom (LandingResult → dict)
        """
        download_result = AirflowTaskAdapter.from_xcom_download(download_data)

        # For non-archive files, create ExtractionResult (no actual extraction)
        # The download result already has the original filename from the server
        extract_result = ExtractionResult(
            path=download_result.path,
            size_mib=download_result.size_mib,
            extracted_sha256=download_result.sha256,
            archive_sha256=download_result.sha256,  # Same as extracted (no archive)
            original_filename=download_result.original_filename,  # Preserve from download
        )

        # Validate landing
        landing_result = PipelineDownloader.validate_landing(extract_result)

        return AirflowTaskAdapter.to_xcom(landing_result)

    return {
        "check_should_run": check_should_run,
        "validate_state_coherence": validate_state_coherence_task,
        "cleanup_incoherent_state": cleanup_incoherent_state_task,
        "download_data": download_data_task,
        "validate_landing": validate_landing_task,
        "convert_to_bronze": convert_to_bronze_task,
        "transform_to_silver": transform_to_silver_task,
    }


def create_extract_tasks(dataset: Dataset) -> dict[str, Any]:
    """Create archive extraction task for a given dataset.

    Args:
        dataset: Dataset configuration

    Returns:
        Dict with 'extract_archive' task function
    """

    @task(task_id="extract_archive", execution_timeout=EXTRACT_TIMEOUT)
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
        # TODO: should be done after a successful bronze task run
        # try:
        #     if archive_path.exists() and archive_path.suffix == ".7z":
        #         archive_path.unlink()
        #         logger.info(
        #             "Removed archive after successful extraction",
        #             extra={"archive": str(archive_path)},
        #         )
        # except Exception as e:
        #     logger.warning(
        #         "Failed to remove archive (non-critical)",
        #         extra={"archive": str(archive_path), "error": str(e)},
        #     )

        # Validate landing
        landing_result = PipelineDownloader.validate_landing(extract_result)

        return AirflowTaskAdapter.to_xcom(landing_result)

    return {"extract_archive": extract_archive_task}


# =============================================================================
# DAG Factory
# =============================================================================


def create_simple_dag(
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
    tasks = create_common_tasks(dataset_name, dataset, asset)

    desc = dataset.description[:200] if dataset.description else f"Pipeline for {dataset_name}"

    @dag(
        dag_id=f"dag_simple_{dataset_name}",  # TODO, move to settings ?
        description=desc,
        schedule=dataset.ingestion.frequency.airflow_schedule,
        start_date=datetime(2026, 1, 1),
        catchup=False,
        default_args=DEFAULT_ARGS,
        max_active_runs=1,  # Prevent concurrent runs
        tags=[
            dataset.source.provider,
            "simple-dag",
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
1. **Check Should Run** - Determine pipeline action (SKIP/FIRST_RUN/HEAL/RETRY/REFRESH)
   - SKIP: Data is current, short-circuits entire pipeline (all downstream tasks skipped)
   - FIRST_RUN: No state file, first execution
   - HEAL: Expected files missing on disk
   - RETRY: Previous run failed
   - REFRESH: Data is stale based on frequency
2. **Validate State Coherence** - Log state + verify coherence with disk
3. **Cleanup Incoherent State** - Remove corrupted state file (if incoherent)

**Data Processing:**
4. **Download Data** - Fetch from source URL with retry logic
5. **Check Extraction Needed** - Branch based on file format (is_archive)
6. **Extract Archive** - Extract file + cleanup .7z archive (if archive format)
7. **Skip Extraction** - Direct landing validation (if non-archive format)
8. **Merge Landing Results** - Route XCom from successful extraction path
9. **Convert to Bronze** - Transform to Parquet with normalized columns
10. **Transform to Silver** - Apply business rules + emit Asset metadata

### Conditional Paths
- **Skip Path**: check_should_run → (returns False) → all downstream skipped
- **Heal Path**: validate → cleanup → download → ...
- **Archive Path**: download → check → extract_archive → merge → bronze → silver
- **Non-Archive Path**: download → check → skip_extraction → merge → bronze → silver
""",
    )
    def dataset_pipeline() -> None:
        """Data pipeline with conditional extraction and state validation.

        Flow:
        check_should_run (short_circuit)
            -> validate_state_coherence -> download_data
                                        -> cleanup_incoherent_state -> download_data
        download_data -> check_extraction_needed
            -> extract_archive -> merge_landing_results
            -> skip_extraction -> merge_landing_results
        merge_landing_results -> convert_to_bronze -> transform_to_silver (END)

        Note: If check_should_run returns False (SKIP), all downstream tasks skipped.
        """
        # 1. Check if pipeline should run (short-circuit if SKIP)
        should_run = tasks["check_should_run"]()

        # 2. Validate state and cleanup if needed
        validate = tasks["validate_state_coherence"]()
        cleanup = tasks["cleanup_incoherent_state"]()
        download = tasks["download_data"]()

        # 3. Transformation
        bronze = tasks["convert_to_bronze"](download)
        _silver = tasks["transform_to_silver"](bronze)  # Final task with outlet

        # 4. Define task dependencies
        should_run >> validate
        validate >> [cleanup, download]
        cleanup >> download

    return dataset_pipeline()


def create_archive_dag(
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
    tasks = create_common_tasks(dataset_name, dataset, asset)
    tasks["extract_archive"] = create_extract_tasks(dataset)["extract_archive"]

    desc = dataset.description[:200] if dataset.description else f"Pipeline for {dataset_name}"

    @dag(
        dag_id=f"dag_archive_{dataset_name}",  # TODO, move to settings ?
        description=desc,
        schedule=dataset.ingestion.frequency.airflow_schedule,
        start_date=datetime(2026, 1, 1),
        catchup=False,
        default_args=DEFAULT_ARGS,
        max_active_runs=1,  # Prevent concurrent runs
        tags=[
            dataset.source.provider,
            "archive-dag",
            str(dataset.ingestion.frequency),
        ],
    )
    def dataset_pipeline() -> None:
        """Data pipeline with conditional extraction and state validation.

        Flow:
        check_should_run (short_circuit)
            -> validate_state_coherence -> download_data
                                        -> cleanup_incoherent_state -> download_data
        download_data -> check_extraction_needed
            -> extract_archive -> merge_landing_results
            -> skip_extraction -> merge_landing_results
        merge_landing_results -> convert_to_bronze -> transform_to_silver (END)

        Note: If check_should_run returns False (SKIP), all downstream tasks skipped.
        """
        # 1. Check if pipeline should run (short-circuit if SKIP)
        should_run = tasks["check_should_run"]()

        # 2. Validate state and cleanup if needed
        validate = tasks["validate_state_coherence"]()
        cleanup = tasks["cleanup_incoherent_state"]()

        # 3. Download
        download = tasks["download_data"]()

        # 4. Extraction extra tasks
        extract = tasks["extract_archive"](download)

        # 5. Transformation
        bronze = tasks["convert_to_bronze"](extract)
        _silver = tasks["transform_to_silver"](bronze)  # Final task with outlet

        # 6. Define task dependencies
        should_run >> validate
        validate >> [cleanup, download]
        cleanup >> download

    return dataset_pipeline()
