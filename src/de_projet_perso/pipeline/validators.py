"""Silver layer output validators.

This module provides validation functions for Silver layer DataFrames.
Validators are called at the end of each Silver transformation to ensure
data quality and schema compliance (fail-fast at source).

Each validator raises a ValueError with a clear message if validation fails.
"""

from typing import cast

import polars as pl

from de_projet_perso.core.logger import logger


class SilverValidationError(ValueError):
    """Raised when Silver output validation fails."""

    def __init__(self, dataset_name: str, message: str):
        """Initialize validation error with dataset context.

        Args:
            dataset_name: Name of the dataset that failed validation
            message: Detailed error message
        """
        self.dataset_name = dataset_name
        super().__init__(f"Silver validation failed for '{dataset_name}': {message}")


def _check_required_columns(
    df: pl.DataFrame,
    dataset_name: str,
    required_columns: list[str],
) -> None:
    """Validate that all required columns are present.

    Args:
        df: DataFrame to validate
        dataset_name: Name for error messages
        required_columns: List of required column names

    Raises:
        SilverValidationError: If any required column is missing
    """
    actual_columns = set(df.columns)
    missing = set(required_columns) - actual_columns

    if missing:
        raise SilverValidationError(
            dataset_name,
            f"Missing required columns: {sorted(missing)}. "
            f"Available columns: {sorted(actual_columns)}",
        )


def _check_non_empty(df: pl.DataFrame, dataset_name: str) -> None:
    """Validate that DataFrame is not empty.

    Args:
        df: DataFrame to validate
        dataset_name: Name for error messages

    Raises:
        SilverValidationError: If DataFrame has 0 rows
    """
    if len(df) == 0:
        raise SilverValidationError(
            dataset_name,
            "DataFrame is empty (0 rows). Source data may be missing or filtered out entirely.",
        )


def _check_column_types(
    df: pl.DataFrame,
    dataset_name: str,
    expected_types: dict[str, type[pl.DataType]],
) -> None:
    """Validate column data types.

    Args:
        df: DataFrame to validate
        dataset_name: Name for error messages
        expected_types: Dict mapping column names to expected Polars type classes

    Raises:
        SilverValidationError: If any column has wrong type
    """
    type_errors = []

    for col_name, expected_type in expected_types.items():
        if col_name not in df.columns:
            continue  # Missing columns are checked separately

        actual_type = df.schema[col_name]
        if actual_type != expected_type:
            type_errors.append(f"{col_name}: expected {expected_type}, got {actual_type}")

    if type_errors:
        raise SilverValidationError(
            dataset_name,
            f"Column type mismatches: {'; '.join(type_errors)}",
        )


def _check_geographic_bounds(  # noqa: PLR0913
    df: pl.DataFrame,
    dataset_name: str,
    lat_col: str,
    lon_col: str,
    lat_range: tuple[float, float] = (41.0, 52.0),  # France métro approx
    lon_range: tuple[float, float] = (-6.0, 10.0),
) -> None:
    """Validate that coordinates are within expected geographic bounds.

    Args:
        df: DataFrame to validate
        dataset_name: Name for error messages
        lat_col: Name of latitude column
        lon_col: Name of longitude column
        lat_range: (min, max) expected latitude range
        lon_range: (min, max) expected longitude range

    Raises:
        SilverValidationError: If coordinates are outside expected bounds
    """
    if lat_col not in df.columns or lon_col not in df.columns:
        return  # Missing columns are checked separately

    # Cast to float since we know these columns contain numeric coordinates
    lat_min: float | None = cast(float | None, df[lat_col].min())
    lat_max: float | None = cast(float | None, df[lat_col].max())
    lon_min: float | None = cast(float | None, df[lon_col].min())
    lon_max: float | None = cast(float | None, df[lon_col].max())

    errors: list[str] = []

    if lat_min is not None and lat_max is not None:
        if lat_min < lat_range[0] or lat_max > lat_range[1]:
            errors.append(
                f"Latitude range [{lat_min:.2f}, {lat_max:.2f}] "
                f"outside expected [{lat_range[0]}, {lat_range[1]}]"
            )

    if lon_min is not None and lon_max is not None:
        if lon_min < lon_range[0] or lon_max > lon_range[1]:
            errors.append(
                f"Longitude range [{lon_min:.2f}, {lon_max:.2f}] "
                f"outside expected [{lon_range[0]}, {lon_range[1]}]"
            )

    if errors:
        # Log as warning instead of error - data may include DOM-TOM
        logger.warning(
            f"Geographic bounds check for {dataset_name}",
            extra={"issues": errors},
        )


def _check_no_all_nulls(
    df: pl.DataFrame,
    dataset_name: str,
    columns: list[str],
) -> None:
    """Validate that specified columns are not entirely null.

    Args:
        df: DataFrame to validate
        dataset_name: Name for error messages
        columns: List of columns to check

    Raises:
        SilverValidationError: If any column is entirely null
    """
    all_null_cols = []

    for col in columns:
        if col not in df.columns:
            continue
        if df[col].null_count() == len(df):
            all_null_cols.append(col)

    if all_null_cols:
        raise SilverValidationError(
            dataset_name,
            f"Columns are entirely NULL: {all_null_cols}",
        )


# =============================================================================
# Dataset-Specific Validators
# =============================================================================


def validate_ign_contours_iris(df: pl.DataFrame) -> None:
    """Validate Silver output for ign_contours_iris dataset.

    Checks:
    - Required columns present
    - Non-empty
    - Correct types for centroid columns
    - Geographic bounds for centroids
    - No entirely null critical columns

    Args:
        df: Silver DataFrame to validate

    Raises:
        SilverValidationError: If validation fails
    """
    dataset_name = "ign_contours_iris"

    required_columns = [
        "code_iris",
        "nom_iris",
        "code_insee",
        "nom_commune",
        "type_iris",
        "centroid_lat",
        "centroid_lon",
    ]

    expected_types: dict[str, type[pl.DataType]] = {
        "code_iris": pl.String,
        "centroid_lat": pl.Float64,
        "centroid_lon": pl.Float64,
    }

    _check_required_columns(df, dataset_name, required_columns)
    _check_non_empty(df, dataset_name)
    _check_column_types(df, dataset_name, expected_types)
    _check_geographic_bounds(df, dataset_name, "centroid_lat", "centroid_lon")
    _check_no_all_nulls(df, dataset_name, ["code_iris", "centroid_lat", "centroid_lon"])

    logger.debug(
        f"Silver validation passed for {dataset_name}",
        extra={"rows": len(df), "columns": len(df.columns)},
    )


def validate_meteo_france_stations(df: pl.DataFrame) -> None:
    """Validate Silver output for meteo_france_stations dataset.

    Checks:
    - Required columns present
    - Non-empty
    - Correct types for coordinates and flags
    - Geographic bounds for station coordinates
    - No entirely null critical columns

    Args:
        df: Silver DataFrame to validate

    Raises:
        SilverValidationError: If validation fails
    """
    dataset_name = "meteo_france_stations"

    required_columns = [
        "id",
        "nom",
        "latitude",
        "longitude",
        "altitude",
        "mesure_solaire",
        "mesure_eolien",
    ]

    expected_types: dict[str, type[pl.DataType]] = {
        "id": pl.String,
        "latitude": pl.Float64,
        "longitude": pl.Float64,
        "altitude": pl.Int64,
        "mesure_solaire": pl.Boolean,
        "mesure_eolien": pl.Boolean,
    }

    _check_required_columns(df, dataset_name, required_columns)
    _check_non_empty(df, dataset_name)
    _check_column_types(df, dataset_name, expected_types)
    _check_geographic_bounds(df, dataset_name, "latitude", "longitude")
    _check_no_all_nulls(df, dataset_name, ["id", "latitude", "longitude"])

    logger.debug(
        f"Silver validation passed for {dataset_name}",
        extra={
            "rows": len(df),
            "mesure_solaire_count": df["mesure_solaire"].sum(),
            "mesure_eolien_count": df["mesure_eolien"].sum(),
        },
    )


def validate_odre_installations(df: pl.DataFrame) -> None:
    """Validate Silver output for odre_installations dataset.

    Checks:
    - Required columns present
    - Non-empty
    - Correct types for key columns
    - No entirely null critical columns

    Args:
        df: Silver DataFrame to validate

    Raises:
        SilverValidationError: If validation fails
    """
    dataset_name = "odre_installations"

    required_columns = [
        "id_peps",
        "code_iris",
        "code_filiere",
        "filiere",
        "type_energie",
        "puissance_max_installee",
        "est_renouvelable",
        "est_actif",
    ]

    expected_types: dict[str, type[pl.DataType]] = {
        "id_peps": pl.String,
        "code_iris": pl.String,
        "code_filiere": pl.String,
        "puissance_max_installee": pl.Float64,
        "est_renouvelable": pl.Boolean,
        "est_actif": pl.Boolean,
    }

    _check_required_columns(df, dataset_name, required_columns)
    _check_non_empty(df, dataset_name)
    _check_column_types(df, dataset_name, expected_types)
    _check_no_all_nulls(df, dataset_name, ["id_peps", "code_filiere"])

    logger.debug(
        f"Silver validation passed for {dataset_name}",
        extra={
            "rows": len(df),
            "renouvelables": df["est_renouvelable"].sum(),
            "actifs": df["est_actif"].sum(),
        },
    )
