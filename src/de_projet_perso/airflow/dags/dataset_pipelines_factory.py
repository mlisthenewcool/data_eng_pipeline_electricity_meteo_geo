"""Data pipeline factory for dataset ingestion.

This module generates Airflow DAGs dynamically from the data catalog.
Each dataset in the catalog gets its own DAG with standardized stages:
- Smart remote check (HTTP HEAD - ETag/Last-Modified)
- Download + SHA256 comparison (detect false changes)
- Conditional archive extraction (only if needed)
- Bronze layer conversion (Parquet + normalized columns)
- Silver layer transformation with Asset metadata emission

Pipeline Features (Smart Skip Logic):
- Skip #1: HTTP HEAD before download (ETag/Last-Modified/Content-Length)
- Skip #2: SHA256 comparison after download (detect file re-upload with same content)
- Inline validation: Each task validates its inputs (fail-fast with clear errors)
- State enrichment: Track remote metadata (etag, last_modified) for audit trail
- Full traceability with SHA256 hashing at each step

Best Practices Applied (Airflow 3.x):
- Single Asset per pipeline (silver layer output)
- Metadata emitted only from final task
- Short-circuit tasks for efficient skipping
- Explicit task dependencies with trigger rules
- Production defaults (retries, timeouts, SLAs)
- Airflow native retry mechanism (retries=N)
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import DAG, Asset, Metadata, dag, task

from de_projet_perso.core.data_catalog import Dataset
from de_projet_perso.pipeline.manager import PipelineManager
from de_projet_perso.pipeline.results import (
    BronzeResult,
    CheckMetadataResult,
    DownloadResult,
    ExtractionResult,
)
from de_projet_perso.pipeline.state import PipelineStateManager
from de_projet_perso.pipeline.transformer import PipelineTransformer

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

    @task.short_circuit(task_id="check_remote_changed")
    def check_remote_changed() -> dict[str, Any] | bool:
        """TODO."""
        ctx = PipelineManager.has_dataset_metadata_changed(dataset, dataset_name)
        return False if not ctx else ctx.to_serializable()

    @task.short_circuit(
        task_id="download_and_check_hash",
        execution_timeout=DOWNLOAD_TIMEOUT,  # TODO: mettre dans settings
        retries=3,  # TODO: mettre dans settings
    )
    def download_then_check_hash_task(metadata_dict: dict) -> dict | bool:
        """TODO."""
        _metadata = CheckMetadataResult.from_dict(metadata_dict)
        download = PipelineManager.download(dataset)
        has_changed = PipelineManager.has_hash_changed(dataset_name, download)

        if not has_changed:
            return False

        return metadata_dict | download.to_serializable()

    @task(
        task_id="transform_to_silver",
        outlets=[asset],
        execution_timeout=TRANSFORM_TIMEOUT,
    )
    def transform_to_silver_task(bronze_dict: dict):
        """Apply business transformations and emit Asset metadata.

        This is the ONLY task that emits Metadata (Airflow 3.x best practice).

        Args:
            bronze_dict: PipelineContext from previous task (contains bronze result)

        Yields:
            Metadata for the silver Asset with enriched information
        """
        bronze = BronzeResult.from_dict(bronze_dict)

        # Transform to silver
        silver = PipelineTransformer.to_silver(
            bronze_result=bronze, dataset_name=dataset_name, dataset=dataset
        )

        # TODO: qu'est-ce qui est vraiment important ?
        #  retirer les doublons par rapport au PipelineContext
        # Note: trick the analyzer
        sha256 = bronze_dict.get("extracted_file_sha256") or bronze_dict.get("sha256")
        if not sha256:
            sha256 = "unknown"

        file_size_mib = bronze_dict.get("size_mib", -1)

        PipelineStateManager.update_success(
            dataset_name=dataset_name,
            version=dataset.ingestion.version,
            etag=bronze_dict.get("etag"),
            last_modified=bronze_dict.get("last_modified"),
            content_length=bronze_dict.get("content_length"),
            sha256=sha256,
            file_size_mib=file_size_mib,
            row_count=silver.row_count,
            columns=silver.columns,
        )

        # version=version,
        # etag=etag,
        # last_modified=last_modified,
        # content_length=content_length,
        # sha256=sha256,
        # file_size_mib=file_size_mib,
        # row_count=row_count,
        # columns=columns,

        # Emit enriched metadata for Airflow UI
        # These metadata fields will be visible in the Assets tab
        yield Metadata(
            asset=asset,
            extra={
                "version": dataset.ingestion.version,
                # metadata: integrity & traceability
                "etag": bronze_dict.get("etag"),
                "last_modified": bronze_dict.get("last_modified"),
                "content_length": bronze_dict.get("content_length"),
                "sha256": sha256,
                "file_size_mib": file_size_mib,
                # Dataset metrics
                "row_count": silver.row_count,
                "columns": silver.columns,
                # extra
                "state_file": str(PipelineStateManager.get_state_path(dataset_name)),
            },
        )

    return {
        "check_remote_changed": check_remote_changed,
        "download_then_check_hash": download_then_check_hash_task,
        "transform_to_silver": transform_to_silver_task,
    }


def create_simple_tasks(dataset: Dataset, dataset_name: str) -> dict[str, Any]:
    """Create archive extraction tasks for a given dataset.

    Args:
        dataset: Dataset configuration
        dataset_name: ...

    Returns:
        Dict with 'extract_archive' task function
    """

    @task(
        task_id="convert_to_bronze",
        execution_timeout=TRANSFORM_TIMEOUT,
    )
    def convert_to_bronze_task(download_dict: dict) -> dict:
        """Convert landing file to Parquet with normalized column names.

        Inline validation: Verifies that the extracted/landing file exists
        before attempting transformation. This replaces the old validate_state_coherence
        task with a simpler fail-fast approach.

        Args:
            download_dict: PipelineContext from previous task (contains extraction result)

        Returns:
            Updated PipelineContext with bronze result

        Raises:
            ValueError: If extraction result is missing from context
            FileNotFoundError: If landing file doesn't exist on disk
        """
        download = DownloadResult.from_dict(download_dict)

        if dataset.source.format.is_archive:
            raise Exception  # TODO custom

        # TODO: changer en PipelineLogicalError par exemple
        if not download.path.exists():
            raise FileNotFoundError(
                f"Landing file not found: {download.path}. "
                f"Dataset: {dataset_name}. "
                "This may indicate missing files (HEAL scenario). "
                "To fix: Manually delete the state file and re-run the pipeline, "
                f"or delete the landing file: {download.path}"
            )

        return (
            download_dict
            | PipelineTransformer.to_bronze(
                landing_path=download.path, dataset_name=dataset_name, dataset=dataset
            ).to_serializable()
        )

    return {"convert_to_bronze": convert_to_bronze_task}


def create_archive_tasks(dataset: Dataset, dataset_name: str) -> dict[str, Any]:
    """Create archive extraction tasks for a given dataset.

    Args:
        dataset: Dataset configuration
        dataset_name: ...

    Returns:
        Dict with 'extract_archive' task function
    """

    @task.short_circuit(
        task_id="extract_archive_then_check_hash", execution_timeout=EXTRACT_TIMEOUT
    )
    def extract_archive_then_check_hash_task(download_dict: dict) -> dict | bool:
        """Extract archive and update context.

        Args:
            download_dict: PipelineContext from download task (contains download result)

        Returns:
            Updated PipelineContext with extraction result
        """
        download = DownloadResult.from_dict(download_dict)

        extract = PipelineManager.extract_archive(archive_path=download.path, dataset=dataset)

        has_changed = PipelineManager.has_hash_changed(dataset_name, extract)

        if not has_changed:
            return False

        return download_dict | extract.to_serializable()

    @task(
        task_id="convert_to_bronze",
        execution_timeout=TRANSFORM_TIMEOUT,
    )
    def convert_to_bronze_task(extraction_dict: dict) -> dict:
        """Convert landing file to Parquet with normalized column names.

        Inline validation: Verifies that the extracted/landing file exists
        before attempting transformation. This replaces the old validate_state_coherence
        task with a simpler fail-fast approach.

        Args:
            extraction_dict: PipelineContext from previous task (contains extraction result)

        Returns:
            Updated PipelineContext with bronze result

        Raises:
            ValueError: If extraction result is missing from context
            FileNotFoundError: If landing file doesn't exist on disk
        """
        extraction = ExtractionResult.from_dict(extraction_dict)

        if not dataset.source.format.is_archive:
            raise Exception  # TODO custom

        # TODO: changer en PipelineLogicalError par exemple
        if not extraction.extracted_file_path.exists():
            raise FileNotFoundError(
                f"Landing file not found: {extraction.extracted_file_path}. "
                f"Dataset: {dataset_name}. "
                "This may indicate missing files (HEAL scenario). "
                "To fix: Manually delete the state file and re-run the pipeline, "
                f"or delete the landing file: {extraction.extracted_file_path}"
            )

        return (
            extraction_dict
            | PipelineTransformer.to_bronze(
                landing_path=extraction.extracted_file_path,
                dataset_name=dataset_name,
                dataset=dataset,
            ).to_serializable()
        )

    return {
        "extract_archive_then_check_hash": extract_archive_then_check_hash_task,
        "convert_to_bronze": convert_to_bronze_task,
    }


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
    tasks = create_common_tasks(dataset_name, dataset, asset) | create_simple_tasks(
        dataset, dataset_name
    )

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

### Smart Skip Logic (2-Stage)

**Stage 1: Remote Check (HTTP HEAD)**
- Checks ETag, Last-Modified, Content-Length headers
- Skips download if remote file unchanged
- Zero bandwidth cost when data is current

**Stage 2: SHA256 Comparison**
- Downloads file and compares hash with previous run
- Detects "false changes" (file re-uploaded but identical content)
- Cleans up duplicate file and skips processing

### Pipeline Stages

1. **Check Remote Changed** - HTTP HEAD request to compare remote metadata
   - Action: FIRST_RUN (no state) / REFRESH (remote changed) / SKIP (unchanged)
2. **Download and Check Hash** - Download + SHA256 comparison
   - Skips if SHA256 identical to previous run (false change)
3. **Convert to Bronze** - Transform to Parquet with normalized columns
   - Inline validation: Verifies file exists before processing
4. **Transform to Silver** - Apply business rules + emit Asset metadata
   - Enriches state with remote metadata (etag, last_modified)

### Skip Scenarios
- **Remote Unchanged**: `check_remote_changed` → SKIP (all downstream skipped)
- **False Change**: `download_and_check_hash` → SKIP (file identical, cleanup + skip)
- **True Change**: Full pipeline executes (remote changed AND SHA256 different)

### Error Handling
- **Missing Files (HEAL)**: Inline validation in `convert_to_bronze` provides clear error
  - Manual fix: Delete state file + re-run OR delete missing file
- **Network Errors**: Airflow native retry mechanism (retries=3)
""",
    )
    def dataset_pipeline() -> None:
        """Data pipeline for non-archive datasets with smart skip logic.

        Flow (with 2-stage skip):
        1. check_remote_changed (HTTP HEAD) → SKIP or create context
        2. download_and_check_hash (SHA256) → SKIP or update context
        3. convert_to_bronze (inline validation) → update context
        4. transform_to_silver (state enrichment) → emit Asset metadata

        Short-circuit behavior:
        - check_remote_changed returns False → all downstream skipped (remote unchanged)
        - download_and_check_hash returns False → all downstream skipped (SHA256 identical)

        Context propagation:
        - Context created in check_remote_changed
        - Updated in download_and_check_hash (adds download + remote metadata)
        - For non-archive: download_and_check_hash populates both download AND extraction
        - Passed through bronze and silver tasks
        """
        # 1. Check remote (HTTP HEAD) - short-circuit if unchanged
        context = tasks["check_remote_changed"]()

        # 2. Download + SHA256 check - short-circuit if identical
        download = tasks["download_then_check_hash"](context)

        # 3. Transformations (bronze → silver)
        bronze = tasks["convert_to_bronze"](download)
        _silver = tasks["transform_to_silver"](bronze)  # Final task with Asset outlet

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
    # merge common & specific-dag tasks
    tasks = create_common_tasks(dataset_name, dataset, asset) | create_archive_tasks(
        dataset, dataset_name
    )

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
        """Data pipeline for archive datasets with smart skip logic.

        Flow (with 2-stage skip + extraction):
        1. check_remote_changed (HTTP HEAD) → SKIP or create context
        2. download_and_check_hash (SHA256) → SKIP or update context
        3. extract_archive (7z extraction) → update context
        4. convert_to_bronze (inline validation) → update context
        5. transform_to_silver (state enrichment) → emit Asset metadata

        Short-circuit behavior:
        - check_remote_changed returns False → all downstream skipped (remote unchanged)
        - download_and_check_hash returns False → all downstream skipped (SHA256 identical)

        Context propagation:
        - Context created in check_remote_changed
        - Updated in download_and_check_hash (adds download + remote metadata)
        - For archives: download_and_check_hash populates download only
        - extract_archive populates extraction
        - Passed through bronze and silver tasks
        """
        # 1. Check remote (HTTP HEAD) - short-circuit if unchanged
        context = tasks["check_remote_changed"]()

        # 2. Download + SHA256 check - short-circuit if identical
        download = tasks["download_then_check_hash"](context)

        # 3. Extract archive (7z → landing file)
        extract = tasks["extract_archive_then_check_hash"](download)

        # 4. Transformations (bronze → silver)
        bronze = tasks["convert_to_bronze"](extract)
        _silver = tasks["transform_to_silver"](bronze)  # Final task with Asset outlet

    return dataset_pipeline()
