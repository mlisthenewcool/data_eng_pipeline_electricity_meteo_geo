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
from enum import StrEnum
from pathlib import Path

from pydantic import BaseModel

from de_projet_perso.core.settings import DATA_DIR


class PipelineAction(StrEnum):
    """Actions possibles pour un pipeline."""

    FORCE = "force"  # Première exécution (pas de state)
    HEAL = "heal"  # Fichier manquant sur disque
    RETRY = "retry"  # Dernière exécution a échoué
    REFRESH = "refresh"  # Données périmées (fréquence + vérification source)
    SKIP = "skip"  # Tout est OK


class Stage(StrEnum):
    """Étapes du pipeline."""

    DOWNLOAD = "download"
    EXTRACT = "extract"
    LANDING = "landing"
    BRONZE = "bronze"
    SILVER = "silver"


class StageStatus(BaseModel):
    """Statut d'une étape du pipeline."""

    status: str  # success, failed, pending
    duration_seconds: float | None = None
    timestamp: datetime | None = None
    path: str | None = None
    error: str | None = None

    # Métadonnées spécifiques
    sha256: str | None = None
    file_size_bytes: int | None = None
    row_count: int | None = None
    columns: list[str] | None = None


class RunRecord(BaseModel):
    """Enregistrement d'une exécution réussie."""

    timestamp: datetime
    run_id: str
    version: str
    duration_seconds: float
    stages: dict[str, StageStatus]


class FailedRunRecord(BaseModel):
    """Enregistrement d'une exécution échouée."""

    timestamp: datetime
    run_id: str
    stage_failed: str
    error: str
    traceback: str | None = None


class PipelineState(BaseModel):
    """État complet d'un pipeline."""

    dataset_name: str
    current_version: str
    last_successful_run: RunRecord | None = None
    last_failed_run: FailedRunRecord | None = None
    history: list[dict] = []


class StateManager:
    """Gestion de la persistance de l'état."""

    @staticmethod
    def get_state_path(dataset_name: str) -> Path:
        """Retourne le chemin du fichier state."""
        return DATA_DIR / "_state" / f"{dataset_name}.json"

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
