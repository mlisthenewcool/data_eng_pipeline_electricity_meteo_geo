"""Custom exceptions.

This module defines domain-specific exceptions that provide clear error messages
and structured error information for debugging and error handling.

All exceptions inherit from BaseProjectException to support automatic attribute
extraction for structured logging via to_dict().
"""

from pathlib import Path
from typing import Any


class BaseProjectException(Exception):
    """Base exception for all custom exceptions in this project.

    This base class provides automatic attribute extraction via to_dict()
    for structured logging. All exception attributes (except private ones
    starting with '_') are automatically included in logs.

    Subclasses should:
    1. Call super().__init__(message) with a clear error message
    2. Set instance attributes for contextual information
    3. Optionally override to_dict() to customize logged fields

    Attributes are automatically extracted for logging, no need to override
    to_dict() in most cases.
    """

    def to_dict(self) -> dict[str, Any]:
        """Extract exception attributes as dict for structured logging.

        Returns a dictionary of all public attributes (not starting with '_'),
        converting Path objects to strings for better logging.

        Returns:
            Dictionary of exception attributes suitable for logger extra={}.

        Note:
            Override this method if you need custom behavior (e.g., skip large objects,
            format complex types, add computed fields).
        """
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
    """Raised when all retry attempts have been exhausted.

    Attributes:
        url: The URL that failed to download.
        attempts: Total number of attempts made.
        last_error: The final exception that caused the last attempt to fail.
    """

    def __init__(self, url: str, attempts: int, last_error: Exception) -> None:
        """Initializes the error with failure details and a formatted message.

        Args:
            url: The URL that failed to download.
            attempts: Total number of attempts made.
            last_error: The last exception that occurred.
        """
        self.url = url
        self.attempts = attempts
        self.last_error = last_error
        error_summary = self._format_last_error(last_error)
        super().__init__(f"{error_summary} after {attempts} attempts for URL {url}")

    @staticmethod
    def _format_last_error(error: Exception) -> str:
        """Formats the last error into a concise string.

        Args:
            error: The exception to format.

        Returns:
            A concise error description, avoiding redundant URL info.
        """
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
    """Raised when the archive file does not exist.

    Attributes:
        path: Path to the missing archive file.
    """

    def __init__(self, path: Path) -> None:
        """Initializes the error with the missing archive path.

        Args:
            path: Path to the missing archive.
        """
        self.path = path
        super().__init__(f"Archive not found: {path}")


class FileNotFoundInArchiveError(ExtractionError):
    """Raised when the target file is not found within the archive.

    Attributes:
        target_filename: Name of the file that was expected in the archive.
        archive_path: Path to the archive that was searched.
    """

    def __init__(self, target_filename: str, archive_path: Path) -> None:
        """Initializes the error with the missing filename and archive location.

        Args:
            target_filename: Name of the file that was not found.
            archive_path: Path to the archive that was searched.
        """
        self.target_filename = target_filename
        self.archive_path = archive_path
        super().__init__(f"File {target_filename} not found in archive: {archive_path.name}")


class FileIntegrityError(BaseProjectException):
    """Raised when file validation (hash, size, etc.) fails.

    Attributes:
        path: Path to the file that failed validation.
        reason: Description of why validation failed.
    """

    def __init__(self, path: Path, reason: str) -> None:
        """Initializes the error with the file path and failure reason.

        Args:
            path: Path to the file that failed validation.
            reason: Description of why validation failed.
        """
        self.path = path
        self.reason = reason
        super().__init__(f"File integrity check failed for {path.name}: {reason}")


class DataCatalogError(BaseProjectException):
    """Base exception for data catalog related failures."""


class InvalidCatalogError(DataCatalogError):
    """Raised when the data catalog YAML could not be parsed or validated.

    Attributes:
        path: Path to the catalog file (None if loaded from string).
        reason: Specific details about the validation or parsing failure.
        validation_errors: Optional, Pydantic validation errors.
    """

    def __init__(
        self, path: Path, reason: str, validation_errors: dict[str, str] | None = None
    ) -> None:
        """Initializes the error with the catalog path and failure reason.

        Args:
            path: Path to the data catalog file.
            reason: Description of why validation failed.
            validation_errors: Optional, Pydantic validation errors.
        """
        self.path = path
        self.reason = reason
        self.validation_errors = validation_errors
        super().__init__(f"Data catalog could not be validated for {path}: {reason}")


class DatasetNotFoundError(DataCatalogError):
    """Raised when a requested dataset is missing from the catalog.

    Attributes:
        name: The identifier of the missing dataset.
        available_datasets: List of valid dataset names found in the catalog.
    """

    def __init__(self, name: str, available_datasets: list[str]) -> None:
        """Initializes the error with the dataset name and a list of valid options.

        Args:
            name: Name of the dataset requested.
            available_datasets: List of dataset identifiers available in the catalog.
        """
        self.name = name
        self.available_datasets = available_datasets
        super().__init__("Dataset does not exist in data catalog")


class PlatformError(BaseProjectException):
    """Base exception for platform related failures."""


class AirflowContextError(PlatformError):
    """Raised when code is executed in the wrong Airflow context.

    This exception is raised when:
    - Airflow-only code is executed outside Airflow (e.g., in scripts)
    - Non-Airflow code is executed inside Airflow (e.g., runtime generation)

    Attributes:
        operation: Name of the operation that failed
        expected_context: Where the code should be executed ("airflow" or "non-airflow")
        actual_context: Where the code is actually running
        suggestion: Alternative method or approach to use

    Example:
        Check Airflow context before executing:

            from de_projet_perso.core.settings import settings

            if not settings.is_running_on_airflow:
                raise AirflowContextError(
                    "get_airflow_version_template() requires Airflow context. "
                    "Use format_datetime_as_version() instead"
                )
    """

    def __init__(
        self, operation: str, expected_context: str, actual_context: str, suggestion: str | None
    ) -> None:
        """Initializes the error with context information.

        Args:
            operation: Name of the operation (e.g., "get_airflow_version_template")
            expected_context: Where code should run ("airflow" or "non-airflow")
            actual_context: Where code is running ("airflow" or "non-airflow")
            suggestion: Optional alternative method to use
        """
        self.operation = operation
        self.expected_context = expected_context
        self.actual_context = actual_context
        self.suggestion = suggestion
        super().__init__("Code is executed on the wrong context")


class PipelineError(BaseProjectException):
    """Base exception for pipeline-related failures."""


class SilverDependencyNotFoundError(PipelineError):
    """Raised when a Gold transformation's Silver dependencies are missing.

    This error indicates that one or more Silver datasets required for a
    Gold transformation have not been produced yet.

    Attributes:
        gold_dataset: Name of the Gold dataset being processed
        missing_dependencies: List of (dataset_name, expected_path) tuples
    """

    def __init__(self, gold_dataset: str, missing_dependencies: list[tuple[str, Path]]) -> None:
        """Initialize with Gold dataset and missing dependency info.

        Args:
            gold_dataset: Name of the Gold dataset
            missing_dependencies: List of (name, path) for missing Silver files
        """
        self.gold_dataset = gold_dataset
        self.missing_dependencies = missing_dependencies

        missing_names = [name for name, _ in missing_dependencies]
        missing_details = "\n".join(f"  - {name}: {path}" for name, path in missing_dependencies)

        super().__init__(
            f"Gold dataset '{gold_dataset}' cannot be built: "
            f"missing Silver dependencies: {missing_names}\n{missing_details}"
        )
