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
import os
import re
import sys
from functools import cache
from typing import TYPE_CHECKING, Any, Pattern, Self, TypeGuard

from loguru import logger as _loguru_logger

if TYPE_CHECKING:
    from loguru import Message, Record


# TODO: Move to settings. LOG_LEVEL should be defined via Docker for consistency.
# TODO: Consider moving AIRFLOW_LOGGER_NAME to settings as well.
_AIRFLOW_LOGGER_NAME = "MY_LOGGER"
_LOG_LEVEL = "INFO"


@cache
def _is_airflow_context() -> bool:
    """Detect if running inside an Airflow environment.

    Checks for the presence of ``airflow.cfg`` in the Airflow home directory.
    Result is cached for performance since environment doesn't change at runtime.

    Returns:
        True if running inside Airflow, False otherwise.
    """
    airflow_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
    return os.path.exists(os.path.join(airflow_home, "airflow.cfg"))


def _should_use_colors() -> bool:
    """Determine if ANSI escape codes should be used for coloring.

    Colors are disabled in Airflow environments to prevent UI clutter and
    automatically suppressed if the output stream (stderr) is not a TTY
    (e.g., when redirecting to a file).

    Returns:
        True if the environment supports and expects colored output, False otherwise.
    """
    if _is_airflow_context():
        return False
    return sys.stderr.isatty()


_USE_COLORS = _should_use_colors()

# ANSI escape codes for terminal coloring (both \x1b and \033 are equivalent)
_FAINT = "\033[2m" if _USE_COLORS else ""
_LIGHT_PURPLE = "\033[1;35m" if _USE_COLORS else ""
_LIGHT_WHITE = "\033[1;37m" if _USE_COLORS else ""
_RESET = "\033[0m" if _USE_COLORS else ""

_ANSI_PATTERN: Pattern[str] = re.compile(r"\033\[[0-9;]*m")

# Log formats: Airflow format is simplified since it already shows time & context
_FORMAT_TERMINAL = (
    "<dim>{time:HH:mm:ss}</dim> | <level>{level: <8}</level> | <level>{message}</level>{extra_str}"
)
_FORMAT_AIRFLOW = "{message}{extra_str}"


def _is_mutable_record(record: "Record") -> TypeGuard[dict[str, Any]]:
    """Check if the object is a mutable dictionary (TypeGuard)."""
    return isinstance(record, dict) or hasattr(record, "__setitem__")


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


def _format_extra(record: "Record") -> None:
    """Format extra fields and inject them into the log record.

    This is a Loguru patcher function that adds an ``extra_str`` key to the record.
    The format differs based on environment:

    - Terminal: Colored key-value pairs on separate lines with tree-style indent
    - Airflow: Plain text key-value pairs for clean UI display

    Args:
        record: Loguru record dict to modify in-place.
    """
    extra: dict[str, Any] = record.get("extra", {})

    if not _is_mutable_record(record):
        return

    if not extra:
        record["extra_str"] = ""
        return

    # indent sizes are based on both Airflow UI prefix & standard terminal format
    if _is_airflow_context():
        indent = " " * 22
        prefix = f"\n{indent}└─ "
        parts: list[str] = [f"{_safe_str(k)} => {_safe_str(v)}" for k, v in extra.items()]
    else:
        indent = " " * 20
        prefix = f"\n{indent}{_FAINT}└─ {_RESET}"
        parts: list[str] = [
            f"{_LIGHT_PURPLE}{_safe_str(k)}{_RESET} => {_LIGHT_WHITE}{_safe_str(v)}{_RESET}"
            for k, v in extra.items()
        ]

    record["extra_str"] = prefix + prefix.join(parts)


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
    logging.getLogger(name=_AIRFLOW_LOGGER_NAME).log(level=level_no, msg=message.strip())

    # # 2. Capture des extras pour XCom (Nouveauté)
    # # On ne pousse en XCom que si on est dans Airflow et que c'est une INFO/ERROR avec extras
    # extra = record.get("extra", {})
    # if _is_airflow_context() and extra:
    #     try:
    #         from airflow.operators.python import get_current_context
    #         context = get_current_context()
    #         ti = context["ti"]
    #
    #         # On pousse chaque clé de l'extra dans XCom avec un préfixe
    #         for key, value in extra.items():
    #             ti.xcom_push(key=f"log_metric_{key}", value=value)
    #     except (ImportError, RuntimeError):
    #         # On n'est pas dans un contexte de tâche Airflow (ex: parsing)
    #         # ou Airflow n'est pas installé. On ignore silencieusement.
    #         pass


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

    _instance: Self | None = None

    def __new__(cls, level: str) -> Self:
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

        if _is_airflow_context():
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
        extra: dict[str, Any] = kwargs.pop("extra", {})
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
            **kwargs: Optional ``extra`` dict for structured context.

        Example:
            >>> try:
            ...     _ = 1 / 0
            ... except ZeroDivisionError:
            ...     logger.exception("Failed", extra={"input": "extra information"})
        """
        if sys.exc_info()[0] is None:
            extra: dict[str, Any] = kwargs.setdefault("extra", {})
            extra["warning"] = "You called logger.exception() with no active exception."
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


logger = LoguruLogger(level=_LOG_LEVEL)


def _test_logs_visually() -> None:  # pragma: no cover
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
    _test_logs_visually()
