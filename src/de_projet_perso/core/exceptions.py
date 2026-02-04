"""Custom exceptions with automatic attribute extraction for structured logging."""

from pathlib import Path
from typing import Any


class BaseProjectException(Exception):
    """Base exception with automatic attribute extraction for structured logging via to_dict()."""

    def to_dict(self) -> dict[str, Any]:
        """Extract public attributes as dict for logger extra={}."""
        result: dict[str, Any] = {}

        for key, value in self.__dict__.items():
            # Skip private attributes
            if key.startswith("_"):
                continue

            # Convert Path to string for better logging
            if isinstance(value, Path):
                result[key] = str(value)
            # Skip complex non-serializable objects (basic safety check)
            elif isinstance(value, (str, int, float, bool, type(None))):
                result[key] = value
            elif isinstance(value, (list, dict, tuple)):
                # Assume collections contain simple types (or override to_dict if not)
                result[key] = value
            else:
                # For other types, use class name as placeholder
                result[key] = f"<{type(value).__name__}>"

        return result


class DownloadError(BaseProjectException):
    """Base exception for download-related failures."""


class RetryExhaustedError(DownloadError):
    """Raised when all retry attempts have been exhausted."""

    def __init__(self, url: str, attempts: int, last_error: Exception) -> None:
        self.url = url
        self.attempts = attempts
        self.last_error = last_error
        error_summary = self._format_last_error(last_error)
        super().__init__(f"{error_summary} after {attempts} attempts for URL {url}")

    @staticmethod
    def _format_last_error(error: Exception) -> str:
        # aiohttp.ClientResponseError
        if hasattr(error, "status") and hasattr(error, "message"):
            return f"HTTP {error.status} {error.message}"

        # asyncio.TimeoutError
        if isinstance(error, TimeoutError):
            return "Connection timeout"

        # Fallback: use the error class name and message
        error_str = str(error)
        error_name = type(error).__name__
        return f"{error_name}: {error_str}" if error_str else error_name


class ExtractionError(BaseProjectException):
    """Base exception for archive extraction failures."""


class ArchiveNotFoundError(ExtractionError):
    """Raised when the archive file does not exist."""

    def __init__(self, path: Path) -> None:
        self.path = path
        super().__init__(f"Archive not found: {path}")


class FileNotFoundInArchiveError(ExtractionError):
    """Raised when the target file is not found within the archive."""

    def __init__(self, target_filename: str, archive_path: Path) -> None:
        self.target_filename = target_filename
        self.archive_path = archive_path
        super().__init__(f"File {target_filename} not found in archive: {archive_path.name}")


class FileIntegrityError(BaseProjectException):
    """Raised when file validation (hash, size, etc.) fails."""

    def __init__(self, path: Path, reason: str) -> None:
        self.path = path
        self.reason = reason
        super().__init__(f"File integrity check failed for {path.name}: {reason}")


class DataCatalogError(BaseProjectException):
    """Base exception for data catalog related failures."""


class InvalidCatalogError(DataCatalogError):
    """Raised when the data catalog YAML could not be parsed or validated."""

    def __init__(
        self, path: Path, reason: str, validation_errors: dict[str, str] | None = None
    ) -> None:
        self.path = path
        self.reason = reason
        self.validation_errors = validation_errors
        super().__init__(f"Data catalog could not be validated for {path}: {reason}")


class DatasetNotFoundError(DataCatalogError):
    """Raised when a requested dataset is missing from the catalog."""

    def __init__(self, name: str, available_datasets: list[str]) -> None:
        self.name = name
        self.available_datasets = available_datasets
        super().__init__("Dataset does not exist in data catalog")


class PlatformError(BaseProjectException):
    """Base exception for platform related failures."""


class AirflowContextError(PlatformError):
    """Raised when code is executed in the wrong Airflow context."""

    def __init__(
        self, operation: str, expected_context: str, actual_context: str, suggestion: str | None
    ) -> None:
        self.operation = operation
        self.expected_context = expected_context
        self.actual_context = actual_context
        self.suggestion = suggestion
        super().__init__("Code is executed on the wrong context")


class PipelineError(BaseProjectException):
    """Base exception for pipeline-related failures."""


class SilverDependencyNotFoundError(PipelineError):
    """Raised when Silver datasets required for a Gold transformation are missing."""

    def __init__(self, gold_dataset: str, missing_dependencies: list[tuple[str, Path]]) -> None:
        self.gold_dataset = gold_dataset
        self.missing_dependencies = missing_dependencies

        missing_names = [name for name, _ in missing_dependencies]
        missing_details = "\n".join(f"  - {name}: {path}" for name, path in missing_dependencies)

        super().__init__(
            f"Gold dataset '{gold_dataset}' cannot be built: "
            f"missing Silver dependencies: {missing_names}\n{missing_details}"
        )
