"""PostgreSQL data loader using SQL files.

This module provides the PostgresLoader class that loads Parquet data
to PostgreSQL using UPSERT queries defined in separate SQL files.
"""

from datetime import date
from importlib.resources import files
from pathlib import Path
from typing import Any

import polars as pl

from de_projet_perso.core.logger import logger
from de_projet_perso.database.connection import DatabaseConnection
from de_projet_perso.database.tables import TABLES, TableConfig, get_table_config


class PostgresLoader:
    """Loads Parquet data to PostgreSQL using SQL files.

    This class reads UPSERT queries from SQL files and executes them
    with data from Parquet files. SQL files are stored in the sql/upsert/
    directory and named after the target table (e.g., ref_stations_meteo.sql).

    Example:
        loader = PostgresLoader()

        # Load a specific table
        rows = loader.load_table("ref.stations_meteo")

        # Load from a custom Parquet path
        rows = loader.load_table("gold.installations_meteo", parquet_path=my_path)

        # Initialize database schema
        loader.initialize_schema()
    """

    # Path to SQL files within the package
    SQL_PACKAGE = "de_projet_perso.database.sql"

    def __init__(self, connection: DatabaseConnection | None = None) -> None:
        """Initialize the loader.

        Args:
            connection: Database connection manager. If None, creates a new one.
        """
        self._connection = connection or DatabaseConnection()

    def _load_sql(self, category: str, name: str) -> str:
        """Load SQL query from file.

        Args:
            category: SQL category subdirectory ('schema' or 'upsert')
            name: SQL file name without extension

        Returns:
            SQL query string

        Raises:
            FileNotFoundError: If SQL file not found
        """
        sql_files = files(self.SQL_PACKAGE).joinpath(category)
        sql_path = sql_files.joinpath(f"{name}.sql")

        try:
            return sql_path.read_text()
        except FileNotFoundError:
            raise FileNotFoundError(
                f"SQL file not found: {category}/{name}.sql. Expected at: {sql_path}"
            ) from None

    def initialize_schema(self) -> None:
        """Initialize database schema (create schemas and tables).

        Executes all SQL files in the schema/ directory in order.
        Safe to run multiple times (uses IF NOT EXISTS).
        """
        logger.info("Initializing database schema")

        schema_files = ["01_create_schemas", "02_create_tables"]

        with self._connection.cursor() as cur:
            for sql_file in schema_files:
                logger.debug(f"Executing schema file: {sql_file}.sql")
                sql_content = self._load_sql("schema", sql_file)
                # Use bytes to satisfy psycopg's Query type (accepts bytes)
                cur.execute(sql_content.encode("utf-8"))

        logger.info("Database schema initialized successfully")

    def _prepare_row(self, row: dict[str, Any], config: TableConfig) -> dict[str, Any]:
        """Prepare a row for insertion, applying column mappings and conversions.

        Args:
            row: Raw row data from Parquet
            config: Table configuration

        Returns:
            Prepared row with proper types for PostgreSQL
        """
        prepared = {}

        for col, value in row.items():
            # Skip excluded columns
            if col in config.exclude_columns:
                continue

            # Apply column mapping if defined
            target_col = config.column_mapping.get(col, col)

            # Convert types as needed
            if isinstance(value, date):
                # psycopg handles date objects natively
                prepared[target_col] = value
            elif isinstance(value, list):
                # Convert list to PostgreSQL array format
                prepared[target_col] = value
            elif value is None:
                prepared[target_col] = None
            else:
                prepared[target_col] = value

        return prepared

    def load_table(
        self,
        table_name: str,
        parquet_path: Path | None = None,
    ) -> int:
        """Load data from Parquet to PostgreSQL using UPSERT.

        Args:
            table_name: Fully qualified table name (e.g., 'ref.stations_meteo')
            parquet_path: Path to Parquet file. If None, uses default from config.

        Returns:
            Number of rows processed

        Raises:
            KeyError: If table configuration not found
            FileNotFoundError: If Parquet file or SQL file not found
        """
        config = get_table_config(table_name)

        # Get source path
        source_path = parquet_path or config.get_source_path()
        if not source_path.exists():
            raise FileNotFoundError(f"Parquet file not found: {source_path}")

        logger.info(
            f"Loading data to {table_name}",
            extra={
                "source": str(source_path),
                "table": table_name,
            },
        )

        # Read Parquet file
        df = pl.read_parquet(source_path)
        row_count = len(df)

        logger.debug(f"Read {row_count:,} rows from Parquet")

        # Load UPSERT SQL
        upsert_sql = self._load_sql("upsert", config.sql_file_name)

        # Convert to list of dicts and prepare rows
        rows = df.to_dicts()
        prepared_rows = [self._prepare_row(row, config) for row in rows]

        # Execute UPSERT in batches
        batch_size = 1000
        rows_affected = 0

        with self._connection.cursor() as cur:
            for i in range(0, len(prepared_rows), batch_size):
                batch = prepared_rows[i : i + batch_size]
                # Use bytes to satisfy psycopg's Query type (accepts bytes)
                cur.executemany(upsert_sql.encode("utf-8"), batch)
                rows_affected += len(batch)

                if (i + batch_size) % 10000 == 0:
                    logger.debug(f"Processed {i + batch_size:,} rows")

        logger.info(
            f"Loaded {rows_affected:,} rows to {table_name}",
            extra={
                "table": table_name,
                "rows_affected": rows_affected,
            },
        )

        return rows_affected

    def load_all_tables(self) -> dict[str, int]:
        """Load all configured tables.

        Returns:
            Dictionary mapping table names to rows processed
        """
        results = {}

        # Load ref tables first (for foreign key constraints)
        ref_tables = [t for t in TABLES if t.startswith("ref.")]
        gold_tables = [t for t in TABLES if t.startswith("gold.")]

        for table_name in ref_tables + gold_tables:
            try:
                rows = self.load_table(table_name)
                results[table_name] = rows
            except Exception as e:
                logger.error(f"Failed to load {table_name}: {e}")
                raise

        return results
