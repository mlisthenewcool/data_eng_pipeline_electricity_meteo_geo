"""Exception handling utilities for uniform structured logging."""

from de_projet_perso.core.exceptions import BaseProjectException
from de_projet_perso.core.logger import logger


def log_exception_with_extra(
    exception: Exception,
    include_traceback: bool = False,
) -> None:
    """Log an exception with structured metadata from to_dict() if available.

    Falls back gracefully if to_dict() fails or exception is not a BaseProjectException.
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
