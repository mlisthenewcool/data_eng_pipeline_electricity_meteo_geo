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

from dataclasses import field
from datetime import datetime
from enum import StrEnum
from pathlib import Path

from de_projet_perso.core.models import StrictModel
from de_projet_perso.core.settings import settings


class PipelineAction(StrEnum):
    """Actions possibles pour un pipeline.

    Simplified version - relies on Airflow native features and smart skip logic:
    - FIRST_RUN: Initial execution (no state file exists)
    - REFRESH: Remote file changed OR scheduled refresh (data stale)
    - SKIP: Remote unchanged AND SHA256 unchanged (smart skip)

    Removed actions (handled differently):
    - HEAL: Manual trigger or monitoring DAG (inline validation in tasks)
    - RETRY: Handled by Airflow native retry mechanism (retries=N)
    """

    FIRST_RUN = "first_run"  # Première exécution (pas de state)
    REFRESH = "refresh"  # Données périmées ou fichier distant changé
    SKIP = "skip"  # Tout est OK (remote + SHA256 inchangés)


# class Stage(StrEnum):
#     """Étapes du pipeline."""
#
#     DOWNLOAD = "download"
#     EXTRACT = "extract"
#     LANDING = "landing"
#     BRONZE = "bronze"
#     SILVER = "silver"


class RunRecord(StrictModel):
    """Enregistrement d'une exécution réussie."""

    timestamp: datetime = field(default_factory=datetime.now)
    version: str

    # from metadata check
    etag: str | None
    last_modified: datetime | None
    content_length: int | None

    # from download or extraction
    sha256: str
    file_size_mib: float

    # from silver
    row_count: int
    columns: list[str]


class FailedRunRecord(StrictModel):
    """Enregistrement d'une exécution échouée."""

    timestamp: datetime = field(default_factory=datetime.now)
    version: str

    stage_failed: str
    error: str
    traceback: str | None = None


class PipelineState(StrictModel):
    """État complet d'un pipeline."""

    dataset_name: str
    current_version: str
    last_successful_run: RunRecord | None = None
    last_failed_run: FailedRunRecord | None = None
    # history: list[dict] = [] # TODO: implémenter logique


class PipelineStateManager:
    """Gestion de la persistance de l'état."""

    @classmethod
    def get_state_path(cls, dataset_name: str) -> Path:
        """Retourne le chemin du fichier state."""
        return settings.data_state_dir_path / f"{dataset_name}.json"

    @classmethod
    def load(cls, dataset_name: str) -> PipelineState | None:
        """Charge l'état depuis le fichier JSON."""
        path = cls.get_state_path(dataset_name)
        if not path.exists():
            return None
        return PipelineState.model_validate_json(path.read_text())

    @classmethod
    def create_new(cls, dataset_name: str, version: str) -> PipelineState:
        """Crée un nouvel état."""
        return PipelineState(dataset_name=dataset_name, current_version=version)

    @classmethod
    def save(cls, state: PipelineState):
        """Sauvegarde l'état dans le fichier JSON."""
        path = cls.get_state_path(state.dataset_name)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(state.model_dump_json(indent=2))

    @classmethod
    def update_success(  # noqa: PLR0913
        cls,
        dataset_name: str,
        version: str,
        etag: str | None,
        last_modified: datetime | None,
        content_length: int | None,
        sha256: str,
        file_size_mib: float,
        row_count: int,
        columns: list[str],
    ) -> None:
        """Update pipeline state after successful run.

        Args:
            dataset_name: Dataset identifier
            version: Dataset version
            etag: ...
            last_modified: ...
            content_length: ...
            sha256: SHA256 hash of source file
            file_size_mib: ...
            row_count: Number of rows in dataset
            columns: List of column names
        """
        state = cls.load(dataset_name)

        if state is None:
            state = cls.create_new(dataset_name, version)

        state.last_successful_run = RunRecord(
            version=version,
            etag=etag,
            last_modified=last_modified,
            content_length=content_length,
            sha256=sha256,
            file_size_mib=file_size_mib,
            row_count=row_count,
            columns=columns,
        )

        cls.save(state)
