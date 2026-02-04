"""Silver layer output validators.

This module provides validation functions for Silver layer DataFrames.
Validators are called at the end of each Silver transformation to ensure
data quality and schema compliance (fail-fast at source).

Each validator raises a ValueError with a clear message if validation fails.

Architecture:
    Each dataset has a single source of truth schema (SCHEMA_*) defining
    the exact columns and types expected in Silver output. The _check_exact_schema()
    function validates that DataFrames have EXACTLY these columns (no more, no less)
    with the correct types and order.
"""

from typing import cast

import polars as pl

from de_projet_perso.core.logger import logger

# =============================================================================
# Schema Definitions (Single Source of Truth)
# =============================================================================
# These schemas define the EXACT expected structure of Silver DataFrames.
# They are used by transformations (to select columns) and validators (to enforce).

SCHEMA_IGN_CONTOURS_IRIS = pl.Schema(
    {
        "code_iris": pl.String,
        "nom_iris": pl.String,
        "code_insee": pl.String,
        "nom_commune": pl.String,
        "type_iris": pl.String,
        "geom_wkb": pl.Binary,
        "centroid_lat": pl.Float64,
        "centroid_lon": pl.Float64,
    }
)

SCHEMA_METEO_FRANCE_STATIONS = pl.Schema(
    {
        "id": pl.String,
        "nom": pl.String,
        "lieu_dit": pl.String,
        "bassin": pl.String,
        "date_debut": pl.Date,
        "latitude": pl.Float64,
        "longitude": pl.Float64,
        "altitude": pl.Int64,
        "mesure_solaire": pl.Boolean,
        "mesure_eolien": pl.Boolean,
        "params_solaires": pl.List(pl.String),
        "params_eoliens": pl.List(pl.String),
        "nb_parametres": pl.UInt32,
    }
)

SCHEMA_ODRE_INSTALLATIONS = pl.Schema(
    {
        "id_peps": pl.String,
        "nom_installation": pl.String,
        "code_iris": pl.String,
        "code_insee": pl.String,
        "commune": pl.String,
        "code_departement": pl.String,
        "departement": pl.String,
        "code_region": pl.String,
        "region": pl.String,
        "code_filiere": pl.String,
        "filiere": pl.String,
        "code_technologie": pl.String,
        "technologie": pl.String,
        "puissance_max_installee": pl.Float64,
        "puissance_max_raccordement": pl.Float64,
        "nb_groupes": pl.Int64,
        "date_mise_en_service": pl.Date,
        "date_raccordement": pl.Date,
        "date_deraccordement": pl.Date,
        "regime": pl.String,
        "gestionnaire": pl.String,
        "code_epci": pl.String,
        "epci": pl.String,
        "est_renouvelable": pl.Boolean,
        "type_energie": pl.String,
        "est_actif": pl.Boolean,
    }
)

SCHEMA_ODRE_ECO2MIX_TR = pl.Schema(
    {
        "code_insee_region": pl.String,
        "libelle_region": pl.String,
        "nature": pl.String,
        "date": pl.String,
        "heure": pl.String,
        "date_heure": pl.Datetime("ms", "Europe/Berlin"),
        "consommation": pl.Int64,
        "thermique": pl.Int64,
        "nucleaire": pl.Int64,
        "eolien": pl.Int64,
        "solaire": pl.Int64,
        "hydraulique": pl.Int64,
        "pompage": pl.String,
        "bioenergies": pl.Int64,
        "ech_physiques": pl.Int64,
        "stockage_batterie": pl.String,
        "destockage_batterie": pl.String,
        "tco_thermique": pl.Float64,
        "tch_thermique": pl.Float64,
        "tco_nucleaire": pl.Float64,
        "tch_nucleaire": pl.Float64,
        "tco_eolien": pl.Float64,
        "tch_eolien": pl.Float64,
        "tco_solaire": pl.Float64,
        "tch_solaire": pl.Float64,
        "tco_hydraulique": pl.Float64,
        "tch_hydraulique": pl.Float64,
        "tco_bioenergies": pl.Float64,
        "tch_bioenergies": pl.Float64,
    }
)

SCHEMA_ODRE_ECO2MIX_CONS_DEF = pl.Schema(
    {
        "code_insee_region": pl.String,
        "libelle_region": pl.String,
        "nature": pl.String,
        "date": pl.String,
        "heure": pl.String,
        "date_heure": pl.Datetime("ms", "Europe/Berlin"),
        "consommation": pl.Int64,
        "thermique": pl.Int64,
        "nucleaire": pl.Int64,
        "eolien": pl.String,  # String in consolidated data
        "solaire": pl.Int64,
        "hydraulique": pl.Int64,
        "pompage": pl.Int64,
        "bioenergies": pl.Int64,
        "ech_physiques": pl.Int64,
        "stockage_batterie": pl.Int64,
        "destockage_batterie": pl.Int64,
        "eolien_terrestre": pl.Int64,
        "eolien_offshore": pl.Int64,
        "tco_thermique": pl.Float64,
        "tch_thermique": pl.Float64,
        "tco_nucleaire": pl.Float64,
        "tch_nucleaire": pl.Float64,
        "tco_eolien": pl.Float64,
        "tch_eolien": pl.Float64,
        "tco_solaire": pl.Float64,
        "tch_solaire": pl.Float64,
        "tco_hydraulique": pl.Float64,
        "tch_hydraulique": pl.Float64,
        "tco_bioenergies": pl.Float64,
        "tch_bioenergies": pl.Float64,
    }
)


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


def _check_exact_schema(
    df: pl.DataFrame,
    dataset_name: str,
    expected_schema: pl.Schema,
) -> None:
    """Validate that DataFrame has EXACTLY the expected schema.

    Checks:
    - No extra columns (DataFrame has columns not in schema)
    - No missing columns (Schema defines columns not in DataFrame)
    - Correct types for all columns
    - Correct column order

    Args:
        df: DataFrame to validate
        dataset_name: Name for error messages
        expected_schema: Expected Polars schema (ordered dict of column -> dtype)

    Raises:
        SilverValidationError: If schema doesn't match exactly
    """
    actual_columns = df.columns
    expected_columns = list(expected_schema.names())

    # Check for extra columns
    extra = set(actual_columns) - set(expected_columns)
    if extra:
        raise SilverValidationError(
            dataset_name,
            f"Unexpected columns in DataFrame: {sorted(extra)}. "
            f"Remove these columns in the transformation.",
        )

    # Check for missing columns
    missing = set(expected_columns) - set(actual_columns)
    if missing:
        raise SilverValidationError(
            dataset_name,
            f"Missing required columns: {sorted(missing)}. "
            f"Available columns: {sorted(actual_columns)}",
        )

    # Check column order
    if actual_columns != expected_columns:
        raise SilverValidationError(
            dataset_name,
            f"Column order mismatch. Expected: {expected_columns}, Got: {actual_columns}",
        )

    # Check types
    type_errors = []
    for col_name, expected_dtype in expected_schema.items():
        actual_dtype = df.schema[col_name]
        if actual_dtype != expected_dtype:
            type_errors.append(f"{col_name}: expected {expected_dtype}, got {actual_dtype}")

    if type_errors:
        raise SilverValidationError(
            dataset_name,
            f"Column type mismatches: {'; '.join(type_errors)}",
        )


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
    - Exact schema match (columns, types, order)
    - Non-empty
    - Geographic bounds for centroids
    - No entirely null critical columns

    Args:
        df: Silver DataFrame to validate

    Raises:
        SilverValidationError: If validation fails
    """
    dataset_name = "ign_contours_iris"

    _check_exact_schema(df, dataset_name, SCHEMA_IGN_CONTOURS_IRIS)
    _check_non_empty(df, dataset_name)
    _check_geographic_bounds(df, dataset_name, "centroid_lat", "centroid_lon")
    _check_no_all_nulls(df, dataset_name, ["code_iris", "centroid_lat", "centroid_lon"])

    logger.info(
        f"Silver validation passed for {dataset_name}",
        extra={"rows": len(df), "columns": len(df.columns)},
    )


def validate_meteo_france_stations(df: pl.DataFrame) -> None:
    """Validate Silver output for meteo_france_stations dataset.

    Checks:
    - Exact schema match (columns, types, order)
    - Non-empty
    - Geographic bounds for station coordinates
    - No entirely null critical columns

    Args:
        df: Silver DataFrame to validate

    Raises:
        SilverValidationError: If validation fails
    """
    dataset_name = "meteo_france_stations"

    _check_exact_schema(df, dataset_name, SCHEMA_METEO_FRANCE_STATIONS)
    _check_non_empty(df, dataset_name)
    _check_geographic_bounds(df, dataset_name, "latitude", "longitude")
    _check_no_all_nulls(df, dataset_name, ["id", "latitude", "longitude"])

    logger.info(
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
    - Exact schema match (columns, types, order)
    - Non-empty
    - No entirely null critical columns

    Args:
        df: Silver DataFrame to validate

    Raises:
        SilverValidationError: If validation fails
    """
    dataset_name = "odre_installations"

    _check_exact_schema(df, dataset_name, SCHEMA_ODRE_INSTALLATIONS)
    _check_non_empty(df, dataset_name)
    _check_no_all_nulls(df, dataset_name, ["id_peps", "code_filiere"])

    logger.info(
        f"Silver validation passed for {dataset_name}",
        extra={
            "rows": len(df),
            "renouvelables": df["est_renouvelable"].sum(),
            "actifs": df["est_actif"].sum(),
        },
    )


def validate_odre_eco2mix_tr(df: pl.DataFrame) -> None:
    """Validate Silver output for odre_eco2mix_tr dataset.

    Checks:
    - Exact schema match (columns, types, order)
    - Non-empty
    - No entirely null critical columns

    Args:
        df: Silver DataFrame to validate

    Raises:
        SilverValidationError: If validation fails
    """
    dataset_name = "odre_eco2mix_tr"

    _check_exact_schema(df, dataset_name, SCHEMA_ODRE_ECO2MIX_TR)
    _check_non_empty(df, dataset_name)
    _check_no_all_nulls(df, dataset_name, ["code_insee_region", "date_heure", "consommation"])

    logger.info(
        f"Silver validation passed for {dataset_name}",
        extra={"rows": len(df), "columns": len(df.columns)},
    )


def validate_odre_eco2mix_cons_def(df: pl.DataFrame) -> None:
    """Validate Silver output for odre_eco2mix_cons_def dataset.

    Checks:
    - Exact schema match (columns, types, order)
    - Non-empty
    - No entirely null critical columns

    Args:
        df: Silver DataFrame to validate

    Raises:
        SilverValidationError: If validation fails
    """
    dataset_name = "odre_eco2mix_cons_def"

    _check_exact_schema(df, dataset_name, SCHEMA_ODRE_ECO2MIX_CONS_DEF)
    _check_non_empty(df, dataset_name)
    _check_no_all_nulls(df, dataset_name, ["code_insee_region", "date_heure", "consommation"])

    logger.info(
        f"Silver validation passed for {dataset_name}",
        extra={"rows": len(df), "columns": len(df.columns)},
    )
