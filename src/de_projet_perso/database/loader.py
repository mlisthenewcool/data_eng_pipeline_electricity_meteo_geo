"""PostgreSQL data loader using SQL files.

This module provides the PostgresLoader class that loads Parquet data
to PostgreSQL using UPSERT queries defined in separate SQL files.

Convention-based loading:
- Silver datasets: sql/upsert/silver/{dataset_name}.sql
- Gold datasets: sql/upsert/gold/{dataset_name}.sql (future)

If a SQL file exists for a dataset, it will be loaded to PostgreSQL.
If not, the dataset is skipped (no error).

Performance:
    Uses PostgreSQL COPY protocol with staging tables, which is 10-100x
    faster than row-by-row INSERT. The strategy is:
    1. Create temp staging table (same structure as target)
    2. COPY data to staging via psycopg's write_row()
    3. UPSERT from staging to target table
    4. Staging auto-dropped on commit

Alternative strategies (not implemented):
    For very large fact tables (>1M rows) where full refresh is acceptable,
    a TRUNCATE + COPY strategy could be even faster (no conflict checking,
    no staging table needed). This would skip directly to target after TRUNCATE.
"""

import re
from datetime import date
from importlib.resources import files
from pathlib import Path
from typing import Any

import polars as pl
from psycopg import sql

from de_projet_perso.core.logger import logger
from de_projet_perso.core.path_resolver import PathResolver
from de_projet_perso.database.connection import DatabaseConnection
from de_projet_perso.database.tables import TABLES, TableConfig, get_table_config


class PostgresLoader:
    """Loads Parquet data to PostgreSQL using SQL files.

    This class reads UPSERT queries from SQL files and executes them
    with data from Parquet files.

    Convention-based approach (recommended):
        - SQL files are named after datasets (e.g., sql/upsert/silver/meteo_france_stations.sql)
        - If SQL exists, dataset is loaded; otherwise skipped silently

    Example:
        loader = PostgresLoader()

        # Convention-based Silver loading (recommended)
        if loader.has_silver_sql("meteo_france_stations"):
            rows = loader.load_silver("meteo_france_stations")

        # Initialize database schema (schemas + tables)
        loader.initialize_schema()

        # Legacy table-based loading (for gold layer)
        rows = loader.load_table("gold.installations_meteo")
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
            category: SQL category path (e.g., 'schema', 'upsert/silver', 'tables/silver')
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

    def _sql_exists(self, category: str, name: str) -> bool:
        """Check if a SQL file exists.

        Args:
            category: SQL category path (e.g., 'upsert/silver')
            name: SQL file name without extension

        Returns:
            True if SQL file exists
        """
        try:
            self._load_sql(category, name)
            return True
        except FileNotFoundError:
            return False

    def initialize_schema(self) -> None:
        """Initialize database schema (create schemas only).

        Executes the schema creation SQL file.
        Safe to run multiple times (uses IF NOT EXISTS).
        """
        logger.info("Initializing database schemas")

        with self._connection.cursor() as cur:
            sql_content = self._load_sql("schema", "01_create_schemas")
            cur.execute(sql_content.encode("utf-8"))

        logger.info("Database schemas initialized successfully")

    def initialize_silver_tables(self) -> None:
        """Initialize all Silver layer tables.

        Executes all SQL files in tables/silver/ directory.
        Safe to run multiple times (uses IF NOT EXISTS).
        """
        logger.info("Initializing Silver tables")

        # List of Silver datasets that have table definitions
        silver_datasets = [
            "meteo_france_stations",
            "ign_contours_iris",
            "odre_installations",
            "odre_eco2mix_tr",
            "odre_eco2mix_cons_def",
        ]

        with self._connection.cursor() as cur:
            for dataset in silver_datasets:
                if self._sql_exists("tables/silver", dataset):
                    logger.debug(f"Creating table for: {dataset}")
                    sql_content = self._load_sql("tables/silver", dataset)
                    cur.execute(sql_content.encode("utf-8"))

        logger.info("Silver tables initialized successfully")

    def initialize_all(self) -> None:
        """Initialize complete database (schemas + all tables).

        Convenience method that calls initialize_schema() and initialize_silver_tables().
        Safe to run multiple times (uses IF NOT EXISTS).
        """
        self.initialize_schema()
        self.initialize_silver_tables()

    # =========================================================================
    # Convention-based Silver loading (recommended)
    # =========================================================================

    def has_silver_sql(self, dataset_name: str) -> bool:
        """Check if UPSERT SQL exists for a Silver dataset.

        Args:
            dataset_name: Dataset identifier (e.g., "meteo_france_stations")

        Returns:
            True if sql/upsert/silver/{dataset_name}.sql exists
        """
        return self._sql_exists("upsert/silver", dataset_name)

    def load_silver(
        self,
        dataset_name: str,
        parquet_path: Path | None = None,
    ) -> int:
        """Load Silver parquet to PostgreSQL using COPY + staging table.

        Uses PostgreSQL COPY protocol which is 10-100x faster than row-by-row INSERT.

        Strategy:
            1. Create temp staging table (same structure as target)
            2. COPY parquet data to staging via psycopg write_row()
            3. UPSERT from staging to target table
            4. Staging auto-dropped on commit

        SQL file requirements:
            The SQL file must contain metadata comments:
            - @target_table: silver.dim_stations_meteo
            And use SELECT FROM staging_{dataset_name} in the UPSERT query.

        Args:
            dataset_name: Dataset identifier (e.g., "meteo_france_stations")
            parquet_path: Path to parquet file. If None, uses silver/current.parquet.

        Returns:
            Number of rows affected by UPSERT

        Raises:
            FileNotFoundError: If SQL or parquet file not found
            ValueError: If @target_table metadata is missing from SQL
        """
        # Get source path
        if parquet_path is None:
            resolver = PathResolver(dataset_name=dataset_name)
            parquet_path = resolver.silver_current_path

        if not parquet_path.exists():
            raise FileNotFoundError(f"Parquet file not found: {parquet_path}")

        # Load SQL and extract metadata
        sql_content = self._load_sql("upsert/silver", dataset_name)
        target_table = self._extract_sql_metadata(sql_content, "target_table")

        if not target_table:
            raise ValueError(
                f"Missing @target_table metadata in SQL for {dataset_name}. "
                f"Add '-- @target_table: silver.table_name' to the SQL file."
            )

        # Read Parquet
        df = pl.read_parquet(parquet_path)
        row_count = len(df)

        # Columns to COPY (exclude updated_at, will be set by UPSERT)
        copy_columns = [c for c in df.columns if c != "updated_at"]

        logger.info(
            "Loading Silver to PostgreSQL via COPY",
            extra={"dataset": dataset_name, "rows": row_count, "target": target_table},
        )

        staging_table = f"staging_{dataset_name}"

        conn = self._connection.connect()
        with conn.cursor() as cur:
            # 1. Create staging table (auto-drops on commit)
            # Parse target table as schema.table for proper quoting
            target_parts = target_table.split(".")
            schema_table_parts = 2  # schema.table format has 2 parts
            if len(target_parts) == schema_table_parts:
                target_schema, target_name = target_parts
                target_identifier = sql.Identifier(target_schema, target_name)
            else:
                target_identifier = sql.Identifier(target_table)

            create_staging_sql = sql.SQL(
                "CREATE TEMP TABLE {} (LIKE {} INCLUDING DEFAULTS) ON COMMIT DROP"
            ).format(sql.Identifier(staging_table), target_identifier)
            cur.execute(create_staging_sql)
            logger.debug(f"Created staging table {staging_table}")

            # 2. COPY data to staging using write_row()
            copy_sql = sql.SQL("COPY {} ({}) FROM STDIN").format(
                sql.Identifier(staging_table),
                sql.SQL(", ").join(sql.Identifier(c) for c in copy_columns),
            )
            with cur.copy(copy_sql) as copy:
                for row in df.select(copy_columns).iter_rows():
                    copy.write_row(row)

            logger.debug(f"Copied {row_count:,} rows to {staging_table}")

            # 3. UPSERT from staging to target
            upsert_query = self._extract_upsert_query(sql_content)
            cur.execute(upsert_query.encode("utf-8"))
            rows_affected = cur.rowcount

            logger.debug(f"Upserted {rows_affected:,} rows to {target_table}")

        conn.commit()  # Staging table dropped here

        logger.info(
            f"Loaded {rows_affected:,} rows to PostgreSQL",
            extra={"dataset": dataset_name, "rows_affected": rows_affected},
        )

        return rows_affected

    def _extract_sql_metadata(self, sql_content: str, key: str) -> str | None:
        """Extract metadata value from SQL file comments.

        Parses lines like: -- @target_table: silver.dim_stations_meteo

        Args:
            sql_content: Full SQL file content
            key: Metadata key to extract (e.g., "target_table")

        Returns:
            Metadata value or None if not found
        """
        pattern = rf"--\s*@{key}:\s*(.+)"
        match = re.search(pattern, sql_content)
        return match.group(1).strip() if match else None

    def _extract_upsert_query(self, sql_content: str) -> str:
        """Extract SQL query, removing metadata comment lines.

        Args:
            sql_content: Full SQL file content with metadata comments

        Returns:
            Clean SQL query without @metadata lines
        """
        lines = []
        for line in sql_content.split("\n"):
            # Skip metadata lines (-- @key: value)
            if not line.strip().startswith("-- @"):
                lines.append(line)
        return "\n".join(lines)

    # =========================================================================
    # Legacy table-based loading (for Gold layer compatibility)
    # =========================================================================

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
