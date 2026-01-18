"""Pipeline state validation logic.

This module validates that pipeline state files are coherent with reality on disk.
"""

from dataclasses import dataclass, field
from pathlib import Path

from de_projet_perso.core.data_catalog import Dataset
from de_projet_perso.core.logger import logger
from de_projet_perso.pipeline.state import PipelineStateManager


@dataclass
class ValidationResult:
    """Result from state validation."""

    dataset: str
    expected_path: Path
    expected_exists: bool
    state_file_exists: bool
    coherent: bool
    issues: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "dataset": self.dataset,
            "expected_path": str(self.expected_path),
            "expected_exists": self.expected_exists,
            "state_file_exists": self.state_file_exists,
            "coherent": self.coherent,
            "issues": self.issues,
        }


class PipelineValidator:
    """Validation logic for pipeline state coherence."""

    @staticmethod
    def validate_state_coherence(dataset_name: str, dataset: Dataset) -> ValidationResult:
        """Verify state file matches reality on disk.

        This validation ensures that the state recorded in JSON
        files is coherent with the actual files present on disk,
        helping detect data integrity issues early.

        Validations performed:
        - Expected file exists on disk
        - Path in state matches expected path
        - State file itself exists

        Args:
            dataset_name: Dataset identifier
            dataset: Dataset configuration
            data_dir: Root data directory

        Returns:
            ValidationResult with validation status and issues
        """
        state = PipelineStateManager.load(dataset_name)
        expected_path = dataset.get_silver_path()

        result = ValidationResult(
            dataset=dataset_name,
            expected_path=expected_path,
            expected_exists=expected_path.exists(),
            state_file_exists=state is not None,
            coherent=True,
            issues=[],
        )

        # Check if expected file exists
        if not expected_path.exists():
            result.issues.append(f"Expected file missing: {expected_path}")
            result.coherent = False

        # Check state coherence
        if state and state.last_successful_run:
            silver_stage = state.last_successful_run.stages.get("silver")
            if silver_stage and silver_stage.path:
                recorded_path = Path(silver_stage.path)
                if recorded_path != expected_path:
                    result.issues.append(
                        f"Path mismatch - recorded: {recorded_path}, expected: {expected_path}"
                    )
                    result.coherent = False
                    logger.warning(
                        "State file path mismatch",
                        extra={
                            "recorded": str(recorded_path),
                            "expected": str(expected_path),
                        },
                    )

        if result.coherent:
            logger.info("State validation passed", extra=result.to_dict())
        else:
            logger.error("State validation failed", extra=result.to_dict())

        return result
