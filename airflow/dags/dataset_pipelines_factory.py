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
from pathlib import Path
from typing import TYPE_CHECKING, Any

from airflow.sdk import DAG, Asset, Metadata, dag, task

from de_projet_perso.core.catalog import DataCatalog, Dataset
from de_projet_perso.core.exceptions import InvalidCatalogError
from de_projet_perso.core.logger import logger
from de_projet_perso.core.pipeline_state import PipelineAction, StateManager
from de_projet_perso.core.settings import DATA_CATALOG_PATH, DATA_DIR

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
# Action Decision Logic
# =============================================================================


def decide_pipeline_action(dataset_name: str, dataset: Dataset) -> PipelineAction:
    """Determine which action to take based on pipeline state.

    Priority order:
    1. FORCE - First run (no state file)
    2. HEAL - File missing from disk
    3. RETRY - Previous run failed
    4. REFRESH - Data is stale based on frequency
    5. SKIP - Everything is up to date

    Args:
        dataset_name: Dataset identifier
        dataset: Dataset configuration

    Returns:
        PipelineAction enum value
    """
    state = StateManager.load(dataset_name)

    # 1. FORCE - No state means first run
    if state is None:
        logger.info(
            "No state found, forcing execution",
            extra={"dataset": dataset_name, "action": PipelineAction.FORCE},
        )
        return PipelineAction.FORCE

    # 2. HEAL - Check if expected files exist
    expected_path = DATA_DIR / dataset.get_storage_path()
    if not expected_path.exists():
        logger.info(
            "Expected file missing, healing",
            extra={
                "dataset": dataset_name,
                "path": str(expected_path),
                "action": PipelineAction.HEAL,
            },
        )
        return PipelineAction.HEAL

    # 3. RETRY - Last run failed
    if state.last_failed_run is not None:
        if state.last_successful_run is None or (
            state.last_failed_run.timestamp > state.last_successful_run.timestamp
        ):
            logger.info(
                "Last run failed, retrying",
                extra={
                    "dataset": dataset_name,
                    "failed_at": state.last_failed_run.timestamp.isoformat(),
                    "action": PipelineAction.RETRY,
                },
            )
            return PipelineAction.RETRY

    # 4. REFRESH - Check if data is stale based on frequency
    if state.last_successful_run is not None:
        frequency_delta = _frequency_to_timedelta(dataset.ingestion.frequency)
        if frequency_delta is not None:
            age = datetime.now() - state.last_successful_run.timestamp
            if age > frequency_delta:
                logger.info(
                    "Data is stale, refreshing",
                    extra={
                        "dataset": dataset_name,
                        "age_hours": round(age.total_seconds() / 3600, 1),
                        "action": PipelineAction.REFRESH,
                    },
                )
                return PipelineAction.REFRESH

    # 5. SKIP - All good
    logger.info(
        "Data is up to date",
        extra={"dataset": dataset_name, "action": PipelineAction.SKIP},
    )
    return PipelineAction.SKIP


def _frequency_to_timedelta(frequency: str) -> timedelta | None:
    """Convert ingestion frequency to timedelta for staleness check."""
    mapping = {
        "hourly": timedelta(hours=1),
        "daily": timedelta(days=1),
        "weekly": timedelta(weeks=1),
        "monthly": timedelta(days=30),
        "yearly": timedelta(days=365),
    }
    return mapping.get(str(frequency))


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
        uri=f"file:///{DATA_DIR / dataset.get_storage_path()}",
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
        action = decide_pipeline_action(dataset_name, dataset)

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
        state = StateManager.load(dataset_name)
        if state is None:
            summary = {
                "status": "no_state",
                "action": "FORCE",
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
            "state_file": str(StateManager.get_state_path(dataset_name)),
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
    def download_data_task() -> dict[str, Any]:
        """Download source file from URL with retry logic."""
        # Local imports to avoid loading heavy deps at DAG parse time
        import asyncio  # noqa: PLC0415

        import aiohttp  # noqa: PLC0415

        from de_projet_perso.core.downloader import download_to_file  # noqa: PLC0415
        from de_projet_perso.core.enums import ExistingFileAction  # noqa: PLC0415

        logger.info(f"Downloading {dataset_name}", extra={"url": str(dataset.source.url)})

        # Determine destination path
        dest_dir = DATA_DIR / "landing" / dataset_name
        dest_dir.mkdir(parents=True, exist_ok=True)

        # Extract filename from URL or use dataset name
        url_path = Path(str(dataset.source.url).split("?")[0])
        filename = url_path.name or f"{dataset_name}.{dataset.source.format}"
        dest_path = dest_dir / filename

        async def _download() -> dict[str, Any]:
            async with aiohttp.ClientSession() as session:
                result = await download_to_file(
                    session=session,
                    url=str(dataset.source.url),
                    dest_path=dest_path,
                    if_exists=ExistingFileAction.OVERWRITE,
                )
                if result is None:
                    return {"path": str(dest_path), "sha256": "cached", "size_mib": 0}
                return {
                    "path": str(result.path),
                    "sha256": result.sha256,
                    "size_mib": result.size_mib,
                }

        return asyncio.run(_download())

    @task(
        task_id="extract_archive",
        execution_timeout=EXTRACT_TIMEOUT,
    )
    def extract_archive_task(download_result: dict[str, Any]) -> dict[str, Any]:
        """Extract archive if format requires it."""
        import asyncio  # noqa: PLC0415

        from de_projet_perso.core.downloader import extract_7z_async  # noqa: PLC0415
        from de_projet_perso.core.enums import ExistingFileAction  # noqa: PLC0415

        # Skip if not an archive format
        if not dataset.source.format.is_archive:
            logger.info(f"No extraction needed for {dataset_name}")
            return download_result

        inner_file = dataset.source.inner_file
        if inner_file is None:
            raise ValueError(f"inner_file required for archive format: {dataset.source.format}")

        archive_path = Path(download_result["path"])
        extract_dir = DATA_DIR / "landing" / dataset_name
        dest_path = extract_dir / inner_file

        logger.info(
            f"Extracting archive for {dataset_name}",
            extra={"archive": archive_path.name, "target": inner_file},
        )

        async def _extract() -> None:
            await extract_7z_async(
                archive_path=archive_path,
                target_filename=inner_file,
                dest_path=dest_path,
                if_exists=ExistingFileAction.OVERWRITE,
                validate_sqlite=dest_path.suffix == ".gpkg",
            )

        asyncio.run(_extract())

        return {
            "path": str(dest_path),
            "sha256": download_result.get("sha256", ""),
            "size_mib": dest_path.stat().st_size / (1024**2) if dest_path.exists() else 0,
        }

    @task(task_id="save_to_landing")
    def save_to_landing_task(extract_result: dict[str, Any]) -> dict[str, Any]:
        """Validate and record landing layer file."""
        path = Path(extract_result["path"])

        if not path.exists():
            raise FileNotFoundError(f"Expected file not found: {path}")

        logger.info(
            f"File saved to landing for {dataset_name}",
            extra={"path": str(path), "size_mib": extract_result.get("size_mib", 0)},
        )

        return {
            "path": str(path),
            "sha256": extract_result.get("sha256", ""),
            "size_mib": extract_result.get("size_mib", 0),
            "layer": "landing",
        }

    @task(
        task_id="convert_to_bronze",
        execution_timeout=TRANSFORM_TIMEOUT,
    )
    def convert_to_bronze_task(landing_result: dict[str, Any]) -> dict[str, Any]:
        """Convert to Parquet with normalized column names."""
        import polars as pl  # noqa: PLC0415

        source_path = Path(landing_result["path"])
        bronze_dir = DATA_DIR / "bronze" / dataset_name
        bronze_dir.mkdir(parents=True, exist_ok=True)
        bronze_path = bronze_dir / f"{dataset.ingestion.version}.parquet"

        logger.info(
            f"Converting to bronze for {dataset_name}",
            extra={"source": source_path.name, "dest": bronze_path.name},
        )

        # Read based on format
        if source_path.suffix == ".gpkg":
            # GeoPackage needs special handling with DuckDB
            import duckdb  # noqa: PLC0415

            conn = duckdb.connect()
            conn.execute("INSTALL spatial; LOAD spatial;")
            df = conn.execute(f"SELECT * FROM st_read('{source_path}')").pl()
        elif source_path.suffix == ".parquet":
            df = pl.read_parquet(source_path)
        elif source_path.suffix == ".json":
            df = pl.read_json(source_path)
        else:
            raise ValueError(f"Unsupported format: {source_path.suffix}")

        # Normalize column names to snake_case
        df = df.rename(lambda col: col.lower().replace(" ", "_").replace("-", "_"))

        # Write to Parquet
        df.write_parquet(bronze_path)

        columns = df.columns
        row_count = len(df)

        logger.info(
            f"Bronze conversion complete for {dataset_name}",
            extra={"rows": row_count, "columns": len(columns)},
        )

        return {
            "path": str(bronze_path),
            "row_count": row_count,
            "columns": columns,
            "sha256": landing_result.get("sha256", ""),
            "layer": "bronze",
        }

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
        state = StateManager.load(dataset_name)
        expected_path = DATA_DIR / dataset.get_storage_path()

        validation_result = {
            "dataset": dataset_name,
            "expected_path": str(expected_path),
            "expected_exists": expected_path.exists(),
            "state_file_exists": state is not None,
            "coherent": True,
            "issues": [],
        }

        # Check if expected file exists
        if not expected_path.exists():
            validation_result["issues"].append(f"Expected file missing: {expected_path}")
            validation_result["coherent"] = False

        # Check state coherence
        if state and state.last_successful_run:
            silver_stage = state.last_successful_run.stages.get("silver")
            if silver_stage and silver_stage.path:
                recorded_path = Path(silver_stage.path)
                if recorded_path != expected_path:
                    validation_result["issues"].append(
                        f"Path mismatch - recorded: {recorded_path}, expected: {expected_path}"
                    )
                    validation_result["coherent"] = False
                    logger.warning(
                        "State file path mismatch",
                        extra={
                            "recorded": str(recorded_path),
                            "expected": str(expected_path),
                        },
                    )

        if validation_result["coherent"]:
            logger.info("State validation passed", extra=validation_result)
        else:
            logger.error("State validation failed", extra=validation_result)

        return validation_result

    @task(
        task_id="transform_to_silver",
        outlets=[asset],
        execution_timeout=TRANSFORM_TIMEOUT,
    )
    def transform_to_silver_task(
        bronze_result: dict[str, Any], context=None
    ) -> Generator[Metadata, None, None]:
        """Apply business transformations and emit Asset metadata.

        This is the ONLY task that emits Metadata (Airflow 3.x best practice).
        """
        import polars as pl  # noqa: PLC0415

        bronze_path = Path(bronze_result["path"])
        silver_dir = DATA_DIR / "silver" / dataset_name
        silver_dir.mkdir(parents=True, exist_ok=True)
        silver_path = silver_dir / f"{dataset.ingestion.version}.parquet"

        logger.info(
            f"Transforming to silver for {dataset_name}",
            extra={"source": bronze_path.name},
        )

        # Track start time for duration calculation
        start_time = datetime.now()
        df = pl.read_parquet(bronze_path)

        # Dataset-specific transformations would go here
        # For now, just copy to silver layer
        df.write_parquet(silver_path)

        # Update state
        _update_pipeline_state(
            dataset_name=dataset_name,
            version=dataset.ingestion.version,
            silver_path=silver_path,
            row_count=len(df),
            columns=df.columns,
            sha256=bronze_result.get("sha256", ""),
        )

        logger.info(
            f"Silver transformation complete for {dataset_name}",
            extra={"path": str(silver_path), "rows": len(df)},
        )

        # Calculate run duration
        end_time = datetime.now()
        run_duration = (end_time - start_time).total_seconds()

        # Infer which action triggered this run
        # Note: We reconstruct the action from state rather than using XCom
        # because decide_action returns the next task_id, not the action itself
        action_taken = "UNKNOWN"
        if context:
            try:
                ti = context.get("ti")
                if ti:
                    state = StateManager.load(dataset_name)
                    action_taken = _infer_action_from_state(state, dataset)
            except Exception:
                # Fallback to UNKNOWN if context unavailable or state unreadable
                pass

        # Emit enriched metadata for Airflow UI
        # These metadata fields will be visible in the Assets tab
        yield Metadata(
            asset=asset,
            extra={
                # Original metadata (kept for compatibility)
                "row_count": len(df),
                "columns": list(df.columns),  # Convert to list for JSON serialization
                "sha256": bronze_result.get("sha256", ""),
                "version": dataset.ingestion.version,
                "completed_at": end_time.isoformat(),
                "status": "success",
                # New enriched metadata
                "action_taken": action_taken,
                "state_file": str(StateManager.get_state_path(dataset_name)),
                "run_duration_seconds": round(run_duration, 2),
                "silver_path": str(silver_path),
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


def _infer_action_from_state(state, dataset: Dataset) -> str:
    """Infer which action was taken based on current state.

    This helper reconstructs the action taken by applying the same
    logic as decide_pipeline_action(), useful for metadata enrichment.

    Note: This function duplicates the decision logic to provide
    the action in metadata without requiring XCom communication
    between decide_action and transform_to_silver tasks.

    Args:
        state: Current pipeline state (PipelineState or None)
        dataset: Dataset configuration

    Returns:
        Action string (FORCE/HEAL/RETRY/REFRESH/SKIP)
    """
    if state is None:
        return "FORCE"

    expected_path = DATA_DIR / dataset.get_storage_path()
    if not expected_path.exists():
        return "HEAL"

    if state.last_failed_run is not None:
        if state.last_successful_run is None or (
            state.last_failed_run.timestamp > state.last_successful_run.timestamp
        ):
            return "RETRY"

    if state.last_successful_run is not None:
        frequency_delta = _frequency_to_timedelta(dataset.ingestion.frequency)
        if frequency_delta is not None:
            age = datetime.now() - state.last_successful_run.timestamp
            if age > frequency_delta:
                return "REFRESH"

    return "SKIP"


def _update_pipeline_state(  # noqa: PLR0913
    dataset_name: str,
    version: str,
    silver_path: Path,
    row_count: int,
    columns: list[str],
    sha256: str,
) -> None:
    """Update pipeline state after successful run."""
    from de_projet_perso.core.pipeline_state import RunRecord, StageStatus  # noqa: PLC0415

    state = StateManager.load(dataset_name)
    if state is None:
        state = StateManager.create_new(dataset_name, version)

    state.last_successful_run = RunRecord(
        timestamp=datetime.now(),
        run_id=f"{dataset_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        version=version,
        duration_seconds=0,  # Would need to track actual duration
        stages={
            "silver": StageStatus(
                status="success",
                timestamp=datetime.now(),
                path=str(silver_path),
                row_count=row_count,
                columns=columns,
                sha256=sha256,
            )
        },
    )

    StateManager.save(state)


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
        catalog = DataCatalog.load(DATA_CATALOG_PATH)
    except InvalidCatalogError as e:
        logger.exception(
            "Failed to load data catalog",
            extra={"path": str(DATA_CATALOG_PATH), "errors": e.validation_errors},
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

PIPELINES = _generate_all_pipelines()
