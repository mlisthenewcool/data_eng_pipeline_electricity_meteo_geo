"""PostgreSQL connection management using Docker secrets.

This module provides a connection manager that reads credentials
from Docker secrets (mounted at /run/secrets/) when running in Docker,
which is the standard secure way to handle credentials in containers.
"""

from contextlib import contextmanager
from pathlib import Path
from typing import Generator

import psycopg

from de_projet_perso.core.logger import logger
from de_projet_perso.core.settings import settings

# Docker secrets paths
_file_extension = "" if settings.is_running_on_airflow else ".txt"
SECRET_USERNAME = settings.secrets_dir_path / f"postgres_root_username{_file_extension}"
SECRET_PASSWORD = settings.secrets_dir_path / f"postgres_root_password{_file_extension}"

# Database configuration

if settings.is_running_on_airflow:
    DATABASE_HOST = "postgres_service"  # Docker service name
else:
    DATABASE_HOST = "127.0.0.1"

DATABASE_PORT = 5432
DATABASE_NAME = "projet_energie"


class DatabaseConnectionError(Exception):
    """Raised when database connection fails."""


class DatabaseConnection:
    """Manages PostgreSQL connections using Docker secrets.

    This class reads credentials from Docker secrets mounted at /run/secrets/
    and provides a context manager for safe cursor handling.

    Example:
        db = DatabaseConnection()
        with db.cursor() as cur:
            cur.execute("SELECT * FROM ref.stations_meteo")
            rows = cur.fetchall()
    """

    def __init__(
        self,
        host: str = DATABASE_HOST,
        port: int = DATABASE_PORT,
        database: str = DATABASE_NAME,
    ) -> None:
        """Initialize database connection manager.

        Args:
            host: PostgreSQL host (default: postgres_service for Docker)
            port: PostgreSQL port (default: 5432)
            database: Database name (default: projet_energie)
        """
        self.host = host
        self.port = port
        self.database = database
        self._connection: psycopg.Connection | None = None

    def _read_secret(self, secret_path: Path) -> str:
        """Read a secret from Docker secrets directory.

        Args:
            secret_path: Path to the secret file

        Returns:
            Secret value as string

        Raises:
            DatabaseConnectionError: If secret file not found
        """
        if not secret_path.exists():
            raise DatabaseConnectionError(
                f"Secret file not found: {secret_path}. "
                "Ensure you are running inside Docker with secrets mounted."
            )
        return secret_path.read_text().strip()

    def _get_connection_string(self) -> str:
        """Build PostgreSQL connection string from secrets.

        Returns:
            Connection string in format: postgresql://user:pass@host:port/db
        """
        username = self._read_secret(SECRET_USERNAME)
        password = self._read_secret(SECRET_PASSWORD)

        return f"postgresql://{username}:{password}@{self.host}:{self.port}/{self.database}"

    def connect(self) -> psycopg.Connection:
        """Establish connection to PostgreSQL.

        Returns:
            Active psycopg connection

        Raises:
            DatabaseConnectionError: If connection fails
        """
        if self._connection is not None and not self._connection.closed:
            return self._connection

        try:
            conn_string = self._get_connection_string()
            self._connection = psycopg.connect(conn_string)
            logger.debug(
                "Connected to PostgreSQL",
                extra={"host": self.host, "database": self.database},
            )
            return self._connection
        except psycopg.Error as e:
            raise DatabaseConnectionError(f"Failed to connect to PostgreSQL: {e}") from e

    def close(self) -> None:
        """Close the database connection."""
        if self._connection is not None and not self._connection.closed:
            self._connection.close()
            logger.debug("Closed PostgreSQL connection")
            self._connection = None

    @contextmanager
    def cursor(self) -> Generator[psycopg.Cursor, None, None]:
        """Context manager for database cursor with automatic commit/rollback.

        Yields:
            Database cursor

        Example:
            with db.cursor() as cur:
                cur.execute("SELECT 1")
        """
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                yield cur
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    def __enter__(self) -> "DatabaseConnection":
        """Enter context manager."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit context manager and close connection."""
        self.close()
