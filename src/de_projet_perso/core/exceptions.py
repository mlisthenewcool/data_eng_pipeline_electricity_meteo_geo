"""Custom exceptions.

This module defines domain-specific exceptions that provide clear error messages
and structured error information for debugging and error handling.
"""

from pathlib import Path


class DownloadError(Exception):
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


class ExtractionError(Exception):
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


class FileShouldNotExist(Exception):
    """Raised when a file exists but the operation requires it to be absent.

    Attributes:
        path: Path to the file that already exists.
    """

    def __init__(self, path: Path) -> None:
        """Initializes the error with the conflicting file path.

        Args:
            path: Path to the file that already exists.
        """
        self.path = path
        super().__init__(f"File {self.path} already exists but shouldn't.")


class FileIntegrityError(Exception):
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


class DataCatalogError(Exception):
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
        super().__init__(
            f"Dataset {name} does not exist in data catalog. "
            f"Available datasets: {available_datasets}"
        )
