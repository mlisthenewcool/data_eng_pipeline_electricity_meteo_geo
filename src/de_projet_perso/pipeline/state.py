"""State management for pipeline executions.

Architecture Decision: Hybrid State Management
-----------------------------------------------
This module provides custom JSON-based state management that coexists with
Airflow's native metadata system. Both approaches are used intentionally:

**State JSON files** (_state/*.json):
  - Purpose: Drive pipeline decision logic (FORCE/HEAL/RETRY/REFRESH/SKIP)
  - Benefits: Portable, testable without Airflow, simple file-based storage
  - Used by: decide_pipeline_action() at DAG runtime
  - Retention: Short-term (weeks), can be safely deleted for fresh start
  - Location: data/_state/{dataset_name}.json

**Airflow Metadata** (Assets + XCom):
  - Purpose: Monitoring, UI visualization, audit trail, lineage tracking
  - Benefits: Native Airflow integration, queryable via API, automatic retention
  - Used by: Final transform_to_silver task emits enriched metadata
  - Retention: Managed by Airflow configuration
  - Includes: action_taken, state_file, run_duration_seconds, silver_path

This hybrid approach prioritizes simplicity and portability while maintaining
full observability in the Airflow UI. For small-scale deployments (< 50 datasets),
this provides the best balance of maintainability vs features.

When to reconsider this approach:
  - Scaling to 50+ datasets
  - Moving to distributed environment (Kubernetes, Celery)
  - Team preference for full Airflow integration
  - Need for long-term historical analysis (> 6 months)
"""

from datetime import datetime
from pathlib import Path

from pydantic import Field

from de_projet_perso.core.models import StrictModel
from de_projet_perso.core.settings import settings
from de_projet_perso.pipeline.results import SilverStageResult


class SuccessfulRunRecord(SilverStageResult):
    """Record of a successful pipeline run.

    Extends SilverStageResult with timestamp to track when the run completed.
    Used by DatasetPipelineState to store the most recent successful execution.
    """

    timestamp: datetime = Field(default_factory=datetime.now)

    # version: str
    # # from metadata check
    # etag: str | None
    # last_modified: datetime | None
    # content_length: int | None
    #
    # # from download or extraction
    # sha256: str
    # file_size_mib: float
    #
    # # from silver
    # row_count: int
    # columns: list[str]


class FailedRunRecord(StrictModel):
    """Record of a failed pipeline run.

    Captures error information to help with debugging and retry logic.
    Currently defined but not yet used in pipeline logic (future enhancement).

    Attributes:
        timestamp: When the failure occurred
        version: Dataset version that failed
        stage_failed: Pipeline stage where error occurred (ingest/bronze/silver)
        error: Error message
        traceback: Full exception traceback if available
    """

    timestamp: datetime = Field(default_factory=datetime.now)
    version: str

    stage_failed: str
    error: str
    traceback: str | None = None


class DatasetPipelineState(StrictModel):
    """Complete state of a dataset pipeline.

    Stores the current state and execution history for a single dataset.
    This state is persisted to data/_state/{dataset_name}.json and drives
    pipeline decision logic (FORCE/HEAL/RETRY/REFRESH/SKIP).

    Attributes:
        dataset_name: Dataset identifier
        current_version: Most recent version processed
        last_successful_run: Details of the last successful execution (if any)

    Note:
        Future enhancements may include last_failed_run and full history tracking.
    """

    dataset_name: str
    current_version: str
    last_successful_run: SuccessfulRunRecord | None = None
    # last_failed_run: FailedRunRecord | None = None # TODO: implement logic
    # history: list[dict] = [] # TODO: implement logic


class PipelineStateManager:
    """Gestion de la persistance de l'état."""

    @classmethod
    def get_state_path(cls, dataset_name: str) -> Path:
        """Retourne le chemin du fichier state."""
        return settings.data_state_dir_path / f"{dataset_name}.json"

    @classmethod
    def load(cls, dataset_name: str) -> DatasetPipelineState | None:
        """Charge l'état depuis le fichier JSON."""
        path = cls.get_state_path(dataset_name)
        if not path.exists():
            return None
        return DatasetPipelineState.model_validate_json(path.read_text())

    @classmethod
    def create_new(cls, dataset_name: str, version: str) -> DatasetPipelineState:
        """Crée un nouvel état."""
        return DatasetPipelineState(dataset_name=dataset_name, current_version=version)

    @classmethod
    def save(cls, state: DatasetPipelineState):
        """Sauvegarde l'état dans le fichier JSON."""
        path = cls.get_state_path(state.dataset_name)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(state.model_dump_json(indent=2, exclude_none=True))

    @classmethod
    def update_success(cls, dataset_name: str, data: SilverStageResult) -> None:
        """Update pipeline state after successful run.

        Args:
            dataset_name: todo
            data: todo
        """
        state = cls.load(dataset_name)

        if not state:
            state = cls.create_new(dataset_name, data.version)

        state.last_successful_run = SuccessfulRunRecord.model_validate(data.model_dump())

        cls.save(state)
