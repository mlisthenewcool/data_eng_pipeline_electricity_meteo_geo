"""Transformations for ODRE installations dataset."""

from pathlib import Path

import polars as pl

from de_projet_perso.core.logger import logger
from de_projet_perso.pipeline.transformations import register_bronze, register_silver
from de_projet_perso.pipeline.validators import validate_odre_installations

# Renewable energy filieres
FILIERES_RENOUVELABLES = ["SOLAI", "EOLIE", "HYDLQ", "BIOEN", "MARIN", "GEOTH"]

# Mapping from codeFiliere to simplified type
TYPE_ENERGIE_MAPPING = {
    "SOLAI": "solaire",
    "EOLIE": "eolien",
    "HYDLQ": "hydraulique",
    "BIOEN": "bioenergie",
    "MARIN": "marin",
    "GEOTH": "geothermie",
    "THERM": "thermique",
    "NUCLE": "nucleaire",
    "STOCK": "stockage",
    "AUTRE": "autre",
}


@register_bronze("odre_installations")
def transform_bronze_odre(landing_path: Path) -> pl.DataFrame:
    """Bronze transformation for ODRE installations.

    Simply reads parquet from landing.

    Args:
        landing_path: Path to the parquet file from landing layer

    Returns:
        Transformed DataFrame ready for bronze layer
    """
    logger.info("Reading parquet from landing", extra={"path": str(landing_path)})
    return pl.read_parquet(landing_path)


@register_silver("odre_installations")
def transform_silver_odre(latest_bronze_path: Path) -> pl.DataFrame:
    """Silver transformation for ODRE installations.

    Normalizes column names to snake_case, selects relevant columns,
    and adds business flags for energy type classification.

    Transformations applied:
    - Rename columns to snake_case
    - Select relevant columns (52 → ~22)
    - Add est_renouvelable flag
    - Add type_energie simplified classification
    - Add est_actif flag (not disconnected)

    Args:
        latest_bronze_path: Path to the latest bronze parquet file

    Returns:
        Normalized and enriched DataFrame
    """
    logger.info("Reading from bronze", extra={"path": str(latest_bronze_path)})
    df = pl.read_parquet(latest_bronze_path)

    logger.info("Applying transformations", extra={"n_rows": len(df), "n_cols": len(df.columns)})

    # Select and rename columns to snake_case
    df_renamed = df.select(
        pl.col("idPEPS").alias("id_peps"),
        pl.col("nomInstallation").alias("nom_installation"),
        pl.col("codeIRIS").alias("code_iris"),
        pl.col("codeINSEECommune").alias("code_insee"),
        pl.col("commune"),
        pl.col("codeDepartement").alias("code_departement"),
        pl.col("departement"),
        pl.col("codeRegion").alias("code_region"),
        pl.col("region"),
        pl.col("codeFiliere").alias("code_filiere"),
        pl.col("filiere"),
        pl.col("codeTechnologie").alias("code_technologie"),
        pl.col("technologie"),
        pl.col("puisMaxInstallee").alias("puissance_max_installee"),
        pl.col("puisMaxRac").alias("puissance_max_raccordement"),
        pl.col("nbGroupes").alias("nb_groupes"),
        pl.col("dateMiseEnService").alias("date_mise_en_service"),
        pl.col("dateRaccordement").alias("date_raccordement"),
        pl.col("dateDeraccordement").alias("date_deraccordement"),
        pl.col("regime"),
        pl.col("gestionnaire"),
        pl.col("codeEPCI").alias("code_epci"),
        pl.col("EPCI").alias("epci"),
    )

    # Add business flags
    df_with_flags = df_renamed.with_columns(
        # Is renewable energy
        pl.col("code_filiere").is_in(FILIERES_RENOUVELABLES).alias("est_renouvelable"),
        # Simplified energy type
        pl.col("code_filiere")
        .replace_strict(TYPE_ENERGIE_MAPPING, default="autre")
        .alias("type_energie"),
        # Is active (not disconnected)
        pl.col("date_deraccordement").is_null().alias("est_actif"),
    )

    # Validate output before returning
    validate_odre_installations(df_with_flags)

    # Log statistics
    stats = {
        "n_rows": len(df_with_flags),
        "n_renouvelables": df_with_flags["est_renouvelable"].sum(),
        "n_actifs": df_with_flags["est_actif"].sum(),
        "n_solaire": len(df_with_flags.filter(pl.col("type_energie") == "solaire")),
        "n_eolien": len(df_with_flags.filter(pl.col("type_energie") == "eolien")),
    }
    logger.info("Silver transformation completed", extra=stats)

    return df_with_flags
