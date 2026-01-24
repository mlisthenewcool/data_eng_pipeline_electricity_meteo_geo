"""Database module for PostgreSQL operations.

This module provides utilities for loading data from Parquet files
to PostgreSQL using UPSERT operations.

Usage:
    from de_projet_perso.database import PostgresLoader

    loader = PostgresLoader()
    rows_affected = loader.load_table("gold.installations_meteo", parquet_path)
"""

from de_projet_perso.database.loader import PostgresLoader

__all__ = ["PostgresLoader"]
