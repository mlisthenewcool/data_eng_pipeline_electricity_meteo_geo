"""Logging configuration using Loguru with Airflow compatibility.

This module provides a pre-configured logger that automatically adapts its output
based on the execution environment:

- **Terminal**: Colored output with timestamps and structured extra fields
- **Airflow**: Plain text routed to Airflow's task logger for UI integration

The logger supports the standard library's ``extra={}`` pattern for structured
logging, making it easy to add context to log messages.

Example:
    Basic usage with structured context::

        from de_projet_perso.core.logger import logger

        logger.info("Download started", extra={"url": "https://example.com", "size": 1024})
        logger.error("Failed to connect", extra={"attempt": 3, "max_retries": 5})

    Exception logging::

        try:
            risky_operation()
        except Exception:
            logger.exception("Operation failed", extra={"context": "pipeline"})

Attributes:
    logger: Pre-configured singleton logger instance ready for use.
"""

import logging
import re
import sys
from typing import TYPE_CHECKING, Any, Optional, Pattern, TypeGuard

from loguru import logger as _loguru_logger

from de_projet_perso.core.settings import settings

if TYPE_CHECKING:
    from loguru import Logger, Message, Record


# convenient, shorter reference
_ON_AIRFLOW = settings.is_running_on_airflow


def _should_use_colors() -> bool:
    """Determine if ANSI escape codes should be used for coloring.

    Colors are disabled in Airflow environments to prevent UI clutter and
    automatically suppressed if the output stream (stderr) is not a TTY
    (e.g., when redirecting to a file).

    Returns:
        True if the environment supports and expects colored output, False otherwise.
    """
    if _ON_AIRFLOW:
        return False
    return sys.stderr.isatty()


_USE_COLORS = _should_use_colors()

# ANSI escape codes for terminal coloring (both \x1b and \033 are equivalent)
if _USE_COLORS:
    _FAINT = "\033[2m"
    _LIGHT_PURPLE = "\033[1;35m"
    _LIGHT_WHITE = "\033[1;37m"
    _RESET = "\033[0m"
else:
    _FAINT = ""
    _LIGHT_PURPLE = ""
    _LIGHT_WHITE = ""
    _RESET = ""

_ANSI_PATTERN: Pattern[str] = re.compile(r"\033\[[0-9;]*m")

# Indentation constants
_INDENT = 22 if _ON_AIRFLOW else 20
_INDENT_INCREASE_PER_LEVEL = 4

_SEPARATOR = " => "

# Log formats: Airflow format is simplified since it already shows time & context
_FORMAT_TERMINAL = (
    "<dim>{time:HH:mm:ss}</dim> | <level>{level: <8}</level> | <level>{message}</level>{extra_str}"
)
_FORMAT_AIRFLOW = "{message}{extra_str}"


def _is_mutable_record(record: "Record") -> TypeGuard[dict[str, Any]]:
    """Type guard ensuring record is a mutable dict (always true for Loguru records).

    Loguru always passes dict instances to patcher functions, but this guard
    helps type checkers understand that record supports item assignment.

    Args:
        record: Loguru record object (always a dict in practice).

    Returns:
        True if record supports dict operations (always True for valid records).
    """
    return isinstance(record, dict)  # or hasattr(record, "__setitem__")


def _strip_ansi(text: str) -> str:
    """Remove ANSI escape sequences from text.

    Prevents log injection attacks where user input could manipulate terminal colors
    or cursor position.

    Args:
        text: Input string potentially containing ANSI escape codes.

    Returns:
        Sanitized string with all ANSI sequences removed.
    """
    return _ANSI_PATTERN.sub(repl="", string=text)


def _safe_str(value: Any) -> str:
    """Safely convert any value to a sanitized string.

    Handles conversion errors gracefully and strips ANSI codes to prevent
    log injection.

    Args:
        value: Any value to convert to string.

    Returns:
        Sanitized string representation, or ``"<REPR_ERROR>"`` if conversion fails.
    """
    try:
        return _strip_ansi(text=str(value))
    except Exception:
        return "<REPR_ERROR>"


def _compute_prefix(level: int) -> str:
    """Compute the prefix string for structured log output at a given nesting level.

    Args:
        level: Current nesting depth (0 for top-level extra fields).

    Returns:
        Formatted prefix string with appropriate indentation and tree character.
    """
    current_indent = " " * (_INDENT + (level * _INDENT_INCREASE_PER_LEVEL))
    if _ON_AIRFLOW:
        return f"\n{current_indent}└─ "
    else:
        return f"\n{current_indent}{_FAINT}└─ {_RESET}"


def _format_value_recursive(value: Any, level: int) -> str:
    r"""Format a value recursively for structured log output.

    Handles nested dictionaries by recursively formatting them with proper indentation.
    Non-dict values are converted to sanitized strings. Empty dicts are represented
    as "{}".

    Args:
        value: The value to format (dict, list, primitive, or any object).
        level: Current nesting depth for indentation (0 = top-level).

    Returns:
        Formatted string with proper indentation, colors (if enabled), and
        tree-style structure for nested dicts.

    Example:
        >>> _format_value_recursive({"a": {"b": 1}}, level=0, is_airflow=True)
        ... 'a => \\n'
        ... '   └─ b => 1'
    """
    # Terminal case: non-dict values are converted to safe strings
    if not isinstance(value, dict):
        return _safe_str(value)

    # Empty dict representation
    if not value:
        return "{?}"

    line_prefix = _compute_prefix(level=level)

    parts = []
    for k, v in value.items():
        k_str = _safe_str(k)
        # Apply color to keys only in terminal mode
        if not _ON_AIRFLOW:
            k_str = f"{_LIGHT_PURPLE}{k_str}{_RESET}"
        if isinstance(v, dict) and v:
            # Nested dict: recursively format with increased indentation
            v_formatted = _format_value_recursive(v, level + 1)
            parts.append(f"{k_str}{v_formatted}")
        else:
            # Simple value: format as key => value
            v_str = _safe_str(v)
            if not _ON_AIRFLOW:
                v_str = f"{_LIGHT_WHITE}{v_str}{_RESET}"
            parts.append(f"{k_str}{_SEPARATOR}{v_str}")
    result = line_prefix.join(parts)
    # Add leading prefix for nested levels
    if level > 0:
        return line_prefix + result
    return result


def _format_extra(record: "Record") -> None:
    """Format extra fields and inject them into the log record.

    This is a Loguru patcher function that adds an ``extra_str`` key to the record.
    The format differs based on environment:

    - Terminal: Colored key-value pairs on separate lines with tree-style indent
    - Airflow: Plain text key-value pairs for clean UI display

    Nested dictionaries in extra fields are recursively formatted with increased
    indentation to maintain visual hierarchy.

    Args:
        record: Loguru record dict to modify in-place.
    """
    extra = record.get("extra", {})

    if not _is_mutable_record(record):
        return

    if not extra:
        record["extra_str"] = ""
        return

    prefix = _compute_prefix(level=0)
    formatted_extra = _format_value_recursive(extra, level=0)

    record["extra_str"] = f"{prefix}{formatted_extra}"


def _airflow_sink(message: "Message") -> None:
    """Route log messages to Airflow's task logger.

    Extracts the log level from the Loguru message and forwards it to the
    configured Airflow logger, preserving the correct severity level.

    Args:
        message: Loguru message object containing the formatted log and metadata.
    """
    record = message.record
    level_name = record["level"].name
    level_no = logging.getLevelName(level=level_name)
    logging.getLogger(name=settings.airflow_logger_name).log(level=level_no, msg=message.strip())


class LoguruLogger:
    """Loguru wrapper providing extra={} support and automatic Airflow detection.

    This is a singleton class that configures Loguru once on first instantiation.
    It bridges the standard library's ``extra={}`` logging pattern with Loguru's
    ``bind()`` mechanism.

    The logger automatically detects if it's running inside Airflow and adjusts:

    - Output sink (stderr vs Airflow task logger)
    - Format (colored vs plain text)
    - Traceback verbosity

    Attributes:
        _instance: Singleton instance storage.
        _logger: Configured Loguru logger with patched formatter.

    Example:
        >>> logger = LoguruLogger(level="INFO")
        >>> logger.info("Hello", extra={"user": "alice"})
        12:34:56 | INFO     | Hello
                            └─ user => alice
    """

    _instance: Optional["LoguruLogger"] = None
    _logger: "Logger"

    def __new__(cls, level: str) -> "LoguruLogger":
        """Return the singleton instance, creating it on first call.

        Args:
            level: Minimum log level (DEBUG, INFO, WARNING, ERROR, CRITICAL).

        Returns:
            The singleton LoguruLogger instance.
        """
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._configure(level)
        return cls._instance

    def _configure(self, level: str) -> None:
        """Configure Loguru sinks and formatters.

        Called once during singleton creation. Sets up the appropriate sink
        and format based on the detected environment.

        Args:
            level: Minimum log level to capture.
        """
        _loguru_logger.remove()
        patched = _loguru_logger.patch(_format_extra)

        if _ON_AIRFLOW:
            patched.add(
                _airflow_sink,
                level=level,
                format=_FORMAT_AIRFLOW,
                colorize=False,
                backtrace=False,
                diagnose=True,
            )
        else:
            patched.add(
                sys.stderr,
                level=level,
                format=_FORMAT_TERMINAL,
                colorize=_USE_COLORS,
                backtrace=True,
                diagnose=True,
            )

        self._logger = patched

    def _log(self, level: str, message: str, **kwargs: Any) -> None:
        """Internal method that handles extra={} conversion to Loguru's bind().

        Args:
            level: Log level name (debug, info, warning, error, critical).
            message: The log message.
            **kwargs: Optional ``extra`` dict and ``exc_info`` bool.
        """
        extra: dict[str, Any] = kwargs.pop("extra", {}) or {}
        exc_info: bool = kwargs.pop("exc_info", False)

        bound = self._logger.bind(**extra)
        # depth=2 is required so Loguru correctly identifies the caller's
        # filename and line number, skipping the _log() and info/debug() wrappers.
        getattr(bound.opt(depth=2, exception=exc_info), level)(message)

    def debug(self, message: str, **kwargs: Any) -> None:
        """Log a debug message.

        Args:
            message: The log message.
            **kwargs: Optional ``extra`` dict for structured context.
        """
        self._log("debug", message, **kwargs)

    def info(self, message: str, **kwargs: Any) -> None:
        """Log an info message.

        Args:
            message: The log message.
            **kwargs: Optional ``extra`` dict for structured context.
        """
        self._log("info", message, **kwargs)

    def warning(self, message: str, **kwargs: Any) -> None:
        """Log a warning message.

        Args:
            message: The log message.
            **kwargs: Optional ``extra`` dict for structured context.
        """
        self._log("warning", message, **kwargs)

    def error(self, message: str, **kwargs: Any) -> None:
        """Log an error message.

        Args:
            message: The log message.
            **kwargs: Optional ``extra`` dict for structured context.
        """
        self._log("error", message, **kwargs)

    def critical(self, message: str, **kwargs: Any) -> None:
        """Log a critical message.

        Args:
            message: The log message.
            **kwargs: Optional ``extra`` dict for structured context.
        """
        self._log("critical", message, **kwargs)

    def exception(self, message: str, **kwargs: Any) -> None:
        """Log an error message with exception traceback.

        Should be called from within an exception handler. If called without
        an active exception, logs a warning instead of a traceback.

        Args:
            message: The log message.
            **kwargs: Optional keyword arguments. Supports ``extra`` dict for structured context.

        Example:
            >>> try:
            ...     _ = 1 / 0
            ... except ZeroDivisionError:
            ...     logger.exception("Failed", extra={"input": "extra information"})
        """
        if sys.exc_info()[0] is None:
            extra: dict[str, Any] = kwargs.setdefault("extra", {})
            extra["warning"] = "You called logger.exception() with no active exception"
            kwargs["exc_info"] = False
        else:
            kwargs["exc_info"] = True
        self._log("error", message, **kwargs)

    @classmethod
    def _reset(cls) -> None:
        """Reset singleton state for testing purposes.

        Warning:
            This method is intended for testing only. Do not use in production.
        """
        cls._instance = None


logger = LoguruLogger(level=settings.logging_level)


def __test_logs_visually() -> None:  # pragma: no cover
    """Visual test for logger output across all levels and edge cases.

    Run with: ``PYTHONPATH=src uv run python -m de_projet_perso.core.logger``
    """
    extras = {"status": "working", "user_id": 42}

    logger.debug(message="Debug message", extra=extras)
    logger.info(message="Info message", extra=extras)
    logger.warning(message="Warning message", extra=extras)
    logger.error(message="Error message", extra=extras)
    logger.critical(message="Critical message", extra=extras)

    logger.info(message="Message without extras")

    # ANSI injection test - codes should be stripped
    logger.info(message="ANSI test", extra={"\x1b[1;31mred as key": "\033[1;31mred as value"})

    logger.exception(message="No active exception")

    try:
        _ = 1 / 0
    except ZeroDivisionError:
        logger.exception(message="Division error", extra={"context": "test"})

    logger.info(message="After exception")


if __name__ == "__main__":
    __test_logs_visually()
