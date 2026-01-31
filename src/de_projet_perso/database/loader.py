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
from pathlib import Path

import polars as pl
from psycopg import sql

from de_projet_perso.core.logger import logger
from de_projet_perso.core.path_resolver import PathResolver
from de_projet_perso.core.settings import settings
from de_projet_perso.database.connection import DatabaseConnection


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
    SQL_PACKAGE = settings.root_dir / "postgres"  # todo

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
        sql_path = self.SQL_PACKAGE / category / f"{name}.sql"

        if not sql_path.exists():
            raise FileNotFoundError(f"SQL file not found. Expected at: {sql_path}")

        return sql_path.read_text(encoding="utf-8")

    def _sql_exists(self, category: str, name: str) -> bool:
        """Check if a SQL file exists.

        Args:
            category: SQL category path (e.g., 'upsert/silver')
            name: SQL file name without extension

        Returns:
            True if SQL file exists
        """
        sql_path = self.SQL_PACKAGE / category / f"{name}.sql"
        return sql_path.exists()

    # def initialize_schema(self) -> None:
    #     """Initialize database schema (create schemas only).
    #
    #     Executes the schema creation SQL file.
    #     Safe to run multiple times (uses IF NOT EXISTS).
    #     """
    #     logger.info("Initializing database schemas")
    #
    #     with self._connection.cursor() as cur:
    #         sql_content = self._load_sql("schema", "01_create_schemas")
    #         cur.execute(sql_content.encode("utf-8"))
    #
    #     logger.info("Database schemas initialized successfully")

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
        # self.initialize_schema()
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

    def load(
        self,
        dataset_name: str,
        layer: str,
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
            layer: todo.
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
