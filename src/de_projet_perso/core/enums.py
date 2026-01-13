"""Enumerations for application configuration.

This module defines enums used throughout the application for type-safe
configuration values, avoiding magic strings.
"""

from enum import StrEnum


class ExistingFileAction(StrEnum):
    """Action to take when a destination file already exists.

    Used by download and extraction functions to control how existing files
    are handled.

    Attributes:
        OVERWRITE: Replace the existing file with the new one.
        SKIP: Keep the existing file and skip the operation.
        ERROR: Throw an error if a file already exists.
    """

    OVERWRITE = "overwrite"
    SKIP = "skip"
    ERROR = "error"
