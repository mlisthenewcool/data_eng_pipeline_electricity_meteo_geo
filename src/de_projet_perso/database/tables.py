"""Table configuration for PostgreSQL loading.

This module defines the mapping between source datasets (Silver/Gold Parquet)
and target PostgreSQL tables, including column mappings and primary keys.
"""

from dataclasses import dataclass, field
from pathlib import Path

from de_projet_perso.core.path_resolver import PathResolver


@dataclass(frozen=True)
class TableConfig:
    """Configuration for loading a Parquet file to PostgreSQL.

    Attributes:
        schema: PostgreSQL schema name (e.g., 'ref', 'gold')
        table: PostgreSQL table name
        primary_key: List of primary key columns
        source_layer: Source layer ('silver' or 'gold')
        source_dataset: Source dataset name for PathResolver
        exclude_columns: Columns to exclude from loading
        column_mapping: Mapping from Parquet column names to SQL column names
    """

    schema: str
    table: str
    primary_key: list[str]
    source_layer: str
    source_dataset: str
    exclude_columns: list[str] = field(default_factory=list)
    column_mapping: dict[str, str] = field(default_factory=dict)

    @property
    def full_table_name(self) -> str:
        """Return fully qualified table name (schema.table)."""
        return f"{self.schema}.{self.table}"

    @property
    def sql_file_name(self) -> str:
        """Return SQL file name for UPSERT (e.g., 'ref_stations_meteo')."""
        return f"{self.schema}_{self.table}"

    def get_source_path(self) -> Path:
        """Get the source Parquet file path.

        Returns:
            Path to the current Parquet file (Silver or Gold layer)
        """
        resolver = PathResolver(dataset_name=self.source_dataset)
        if self.source_layer == "silver":
            return resolver.silver_current_path
        elif self.source_layer == "gold":
            return resolver.gold_current_path
        else:
            raise ValueError(f"Unknown source layer: {self.source_layer}")


# =============================================================================
# Table Configurations (V1 - Minimal)
# =============================================================================

TABLES: dict[str, TableConfig] = {
    # Reference table: Weather stations from Météo France
    "ref.stations_meteo": TableConfig(
        schema="ref",
        table="stations_meteo",
        primary_key=["id"],
        source_layer="silver",
        source_dataset="meteo_france_stations",
    ),
    # Gold table: Installations linked to weather stations
    "gold.installations_meteo": TableConfig(
        schema="gold",
        table="installations_meteo",
        primary_key=["id_peps"],
        source_layer="gold",
        source_dataset="installations_meteo",
    ),
}


def get_table_config(table_name: str) -> TableConfig:
    """Get table configuration by name.

    Args:
        table_name: Fully qualified table name (e.g., 'ref.stations_meteo')

    Returns:
        TableConfig for the specified table

    Raises:
        KeyError: If table configuration not found
    """
    if table_name not in TABLES:
        raise KeyError(
            f"Table configuration not found: {table_name}. Available tables: {list(TABLES.keys())}"
        )
    return TABLES[table_name]
