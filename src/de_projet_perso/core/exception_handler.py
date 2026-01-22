"""Exception handling utilities for uniform logging.

This module provides the ``log_exception_with_extra`` helper to ensure consistent
error reporting across the project, specifically by extracting structured metadata
from custom project exceptions inherited from BaseProjectException.
"""

from de_projet_perso.core.exceptions import BaseProjectException
from de_projet_perso.core.logger import logger


def log_exception_with_extra(
    exception: Exception,
    include_traceback: bool = False,
) -> None:
    """Log an exception with its structured metadata as extra fields.

    This helper bridges custom exceptions and the structured logger. If the
    exception is a subclass of ``BaseProjectException``, it automatically
    invokes its ``to_dict()`` method to enrich the log record with context.

    Args:
        exception: The exception instance to log. While any Exception is accepted,
                   structured logging is only fully supported for ``BaseProjectException``.
        include_traceback: If True, includes the full exception traceback in the log.
                           Use this for unexpected errors where stack trace is valuable.
                           Default: False (only logs message + extra).

    Returns:
        None

    Note:
        If ``to_dict()`` fails during extraction, the function falls back to
        logging the error message with a warning in the extra fields to prevent
        losing the original log event.
    """
    log_method = getattr(logger, "exception" if include_traceback else "error", logger.error)

    if isinstance(exception, BaseProjectException):
        try:
            extra = exception.to_dict()
        except Exception as e:
            # Fallback: prevents the logger itself from crashing
            extra = {"to_dict_error": f"Failed to extract exception attributes: {type(e).__name__}"}

    # Support non-BaseProjectException with to_dict() method
    elif hasattr(exception, "to_dict") and callable(getattr(exception, "to_dict", None)):
        try:
            to_dict_method = getattr(exception, "to_dict")
            result = to_dict_method()
            if isinstance(result, dict):
                extra = result
            else:
                extra = None
        except Exception as e:
            # Fallback: prevents the logger itself from crashing
            extra = {"to_dict_error": f"Failed to extract exception attributes: {type(e).__name__}"}
    else:
        extra = {"warning": "Use `log_exception_with_extra` with custom exceptions"}

    log_method(str(exception), extra=extra)
