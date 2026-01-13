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
        last_error: The last exception that occurred.
    """

    def __init__(self, url: str, attempts: int, last_error: Exception) -> None:
        """Initializes RetryExhaustedError with failure details.

        Args:
            url: The URL that failed to download.
            attempts: Total number of attempts made.
            last_error: The last exception that occurred.
        """
        self.url = url
        self.attempts = attempts
        self.last_error = last_error
        error_summary = self._format_last_error(last_error)
        super().__init__(f"{error_summary}, after {attempts} attempts for URL {url}")

    @staticmethod
    def _format_last_error(error: Exception) -> str:
        """Format the last error without redundant URL information.

        Args:
            error: The exception to format.

        Returns:
            A concise error description.
        """
        # Handle aiohttp.ClientResponseError (has status, message attributes)
        if hasattr(error, "status") and hasattr(error, "message"):
            return f"HTTP {error.status} {error.message}"

        # Handle asyncio.TimeoutError
        if isinstance(error, TimeoutError):
            return "Connection timeout"

        # Fallback: use the error class name and message
        error_str = str(error)
        error_name = type(error).__name__
        if error_str:
            return f"{error_name}: {error_str}"
        return error_name


class ExtractionError(Exception):
    """Base exception for archive extraction failures."""


class ArchiveNotFoundError(ExtractionError):
    """Raised when the archive file does not exist.

    Attributes:
        archive_path: Path to the missing archive.
    """

    def __init__(self, archive_path: Path) -> None:
        """Initializes ArchiveNotFoundError.

        Args:
            archive_path: Path to the missing archive.
        """
        self.archive_path = archive_path
        super().__init__(f"Archive not found: {archive_path}")


class FileNotFoundInArchiveError(ExtractionError):
    """Raised when the target file is not found within the archive.

    Attributes:
        target_filename: Name of the file that was not found.
        archive_path: Path to the archive that was searched.
    """

    def __init__(self, target_filename: str, archive_path: Path) -> None:
        """Initializes FileNotFoundInArchiveError.

        Args:
            target_filename: Name of the file that was not found.
            archive_path: Path to the archive that was searched.
        """
        self.target_filename = target_filename
        self.archive_path = archive_path
        super().__init__(f"File {target_filename} not found in archive: {archive_path.name}")


class FileShouldNotExist(Exception):
    """Raised when the file exists but should NOT.

    Attributes:
        file_path: Path to the already existing file.
    """

    def __init__(self, file_path: Path) -> None:
        """Initializes FileExistsButShouldNot.

        Args:
            file_path: Path to the already existing file.
        """
        self.file_path = file_path
        super().__init__(f"File {file_path} already exists but should NOT.")


class FileIntegrityError(Exception):
    """Raised when file validation fails.

    Attributes:
        file_path: Path to the file that failed validation.
        reason: Description of why validation failed.
    """

    def __init__(self, file_path: Path, reason: str) -> None:
        """Initializes FileIntegrityError.

        Args:
            file_path: Path to the file that failed validation.
            reason: Description of why validation failed.
        """
        self.file_path = file_path
        self.reason = reason
        super().__init__(f"File integrity check failed for {file_path.name}: {reason}")


# class DataNotFoundError(Exception):
#     """Raised when required data file is not found.
#
#     Attributes:
#         file_path: Path to the missing file.
#         hint: Suggestion for how to obtain the file.
#     """
#
#     def __init__(self, file_path: Path, hint: str = "") -> None:
#         """Initializes DataNotFoundError.
#
#         Args:
#             file_path: Path to the missing file.
#             hint: Suggestion for how to obtain the file.
#         """
#         self.file_path = file_path
#         self.hint = hint
#         message = f"Data file not found: {file_path}"
#         if hint:
#             message += f". {hint}"
#         super().__init__(message)
#
#
# class InvalidCatalogStateError(Exception):
#     """Raised when the catalog state file cannot be parsed or validated.
#
#     Attributes:
#         path: Path to the state file that failed to load.
#         reason: Description of why parsing/validation failed.
#     """
#
#     def __init__(self, path: Path, reason: str) -> None:
#         """Initialize InvalidCatalogStateError.
#
#         Args:
#             path: Path to the state file that failed to load.
#             reason: Description of why parsing/validation failed.
#         """
#         self.path = path
#         self.reason = reason
#         super().__init__(f"Invalid catalog state at {path}: {reason}")
#
#
# class InvalidCatalogError(Exception):
#     """todo."""
#
#     def __init__(self, path: Path, reason: str) -> None:
#         """todo: that."""
#         self.path = path
#         self.reason = reason
#         super().__init__(f"Invalid data catalog at {path}: {reason}")
