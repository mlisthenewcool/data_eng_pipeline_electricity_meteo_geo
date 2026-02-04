"""Loguru-based logger with extra={} support and automatic Airflow detection.

Adapts output to the execution environment:
- Terminal: colored output with timestamps and structured extra fields
- Airflow: plain text routed to Airflow's task logger

Usage::

    from de_projet_perso.core.logger import logger
    logger.info("Download started", extra={"url": url, "size": 1024})
"""

import logging
import re
import sys
from typing import TYPE_CHECKING, Any, Callable, Optional, Pattern, TypeGuard

from loguru import logger as _loguru_logger

from de_projet_perso.core.settings import settings

if TYPE_CHECKING:
    from loguru import Logger, Message, Record


# convenient, shorter reference
_ON_AIRFLOW = settings.is_running_on_airflow


def _should_use_colors() -> bool:
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

_SEPARATOR = " → "

# Log formats: Airflow format is simplified since it already shows time & context
_FORMAT_TERMINAL = (
    "<dim>{time:HH:mm:ss}</dim> | <level>{level: <8}</level> | <level>{message}</level>{extra_str}"
)
_FORMAT_AIRFLOW = "{message}{extra_str}"


def _is_mutable_record(record: "Record") -> TypeGuard[dict[str, Any]]:
    """Type guard for Loguru records (always dicts, but helps type checkers)."""
    return isinstance(record, dict)  # or hasattr(record, "__setitem__")


def _strip_ansi(text: str) -> str:
    return _ANSI_PATTERN.sub(repl="", string=text)


def _safe_str(value: Any) -> str:
    try:
        return _strip_ansi(text=str(value))
    except Exception:  # noqa
        return "<REPR_ERROR>"


def _compute_prefix(level: int) -> str:
    current_indent = " " * (_INDENT + (level * _INDENT_INCREASE_PER_LEVEL))
    if _ON_AIRFLOW:
        return f"\n{current_indent}└─ "
    else:
        return f"\n{current_indent}{_FAINT}└─ {_RESET}"


def _format_value_recursive(value: Any, level: int) -> str:
    """Format a value recursively with tree-style indentation for nested dicts."""
    # Terminal case: non-dict values are converted to safe strings
    if not isinstance(value, dict):
        return _safe_str(value)

    # Empty dict representation
    if not value:
        return "{}"

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
    """Loguru patcher: format extra fields and inject as extra_str into the record."""
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
    """Route log messages to Airflow's task logger with correct severity level."""
    record = message.record
    level_name = record["level"].name
    level_no = logging.getLevelName(level=level_name)
    logging.getLogger(name=settings.airflow_logger_name).log(level=level_no, msg=message.strip())


class LoguruLogger:
    """Singleton Loguru wrapper bridging stdlib extra={} pattern with Loguru's bind()."""

    _instance: Optional["LoguruLogger"] = None
    _logger: "Logger"

    def __new__(cls, level: str) -> "LoguruLogger":
        """Return singleton instance, creating on first call."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._configure(level)
        return cls._instance

    def _configure(self, level: str) -> None:
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

    def _log(
        self, level: str, message: str, extra: dict[str, Any] | None = None, exc_info: bool = False
    ) -> None:
        log_extra = extra or {}

        bound = self._logger.bind(**log_extra)
        # depth=2 is required so Loguru correctly identifies the caller's
        # filename and line number, skipping the _log() and info/debug() wrappers.
        getattr(bound.opt(depth=2, exception=exc_info), level)(message)

    def debug(self, message: str, /, extra: dict[str, Any] | None = None) -> None:
        """Log a debug message."""
        self._log("debug", message, extra)

    def info(self, message: str, /, extra: dict[str, Any] | None = None) -> None:
        """Log an info message."""
        self._log("info", message, extra)

    def warning(self, message: str, /, extra: dict[str, Any] | None = None) -> None:
        """Log a warning message."""
        self._log("warning", message, extra)

    def error(self, message: str, /, extra: dict[str, Any] | None = None) -> None:
        """Log an error message."""
        self._log("error", message, extra)

    def critical(self, message: str, /, extra: dict[str, Any] | None = None) -> None:
        """Log a critical message."""
        self._log("critical", message, extra)

    def exception(self, message: str, /, extra: dict[str, Any] | None = None) -> None:
        """Log error with traceback. Must be called inside an exception handler."""
        if sys.exc_info()[0] is None:
            extra = extra or {}
            extra["warning"] = "You called logger.exception() with no active exception."
            exc_info = False
        else:
            exc_info = True
        self._log("error", message, extra, exc_info)

    @classmethod
    def _reset(cls) -> None:
        cls._instance = None


class TqdmToLoguru:
    """File-like proxy redirecting tqdm progress output to Loguru."""

    def __init__(self, logger_func: Callable):
        self.logger_func = logger_func
        self.buf = ""

    def write(self, buf: str) -> None:
        """Forward cleaned tqdm buffer to the logger."""
        self.buf = buf.strip("\r\n\t ")
        if self.buf:
            self.logger_func(self.buf)


logger = LoguruLogger(level=settings.logging_level)


if __name__ == "__main__":
    from pathlib import Path

    extras = {"status": "working", "user_id": 42}

    logger.debug("Debug message", extra=extras)
    logger.info("Info message", extra=extras)
    logger.warning("Warning message", extra=extras)
    logger.error("Error message", extra=extras)
    logger.critical("Critical message", extra=extras)

    logger.info("Message without extras")

    # Thanks to `_safe_str(...)`, objects with __str__ method implemented use it automatically
    path = Path(__file__).name
    logger.info(f"Message with path {path}", extra={"path": path})

    # ANSI injection test - codes should be stripped
    logger.info("ANSI test", extra={"\x1b[1;31mred as key": "\033[1;31mred as value"})

    logger.exception("No active exception")

    try:
        _ = 1 / 0
    except ZeroDivisionError:
        logger.exception("Division error", extra={"context": "test"})

    logger.info("After exception")
