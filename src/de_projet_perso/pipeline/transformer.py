"""Pipeline transformation logic.

This module handles bronze and silver layer transformations,
completely decoupled from Airflow orchestration.

Transformations:
- Bronze: Landing (ExtractionResult) → Parquet with normalized columns
- Silver: Bronze → Business logic applied (custom transformations)

SHA256 Propagation:
- Extracted file SHA256 is propagated through all layers for traceability
- Archive SHA256 is also propagated to track original source

Architecture changes:
- to_bronze() now accepts ExtractionResult or DownloadResult (via source_path)
- No separate landing layer result type needed
"""

from pathlib import Path

from de_projet_perso.core.data_catalog import Dataset
from de_projet_perso.core.logger import logger
from de_projet_perso.pipeline.results import BronzeResult, SilverResult
from de_projet_perso.pipeline.transformations import get_bronze_transform, get_silver_transform


class PipelineTransformer:
    """Transformation logic for bronze and silver layers."""

    @staticmethod
    def to_bronze(landing_path: Path, dataset_name: str, dataset: Dataset) -> BronzeResult:
        """Convert landing file to Parquet with normalized column names.

        Bronze layer transformations:
        1. Read raw file from landing (ExtractionResult contains path)
        2. Apply custom transformations (normalize columns, filter, etc.)
        3. Write to standardized bronze Parquet file (naming convention applied here)

        This is where the transition from original filenames (landing) to
        standardized naming (bronze/silver) happens.

        Args:
            landing_path: todo
            dataset_name: Dataset identifier
            dataset: Dataset configuration

        Returns:
            BronzeResult with bronze file info and propagated SHA256s

        Raises:
            NotImplementedError: If no bronze transformation is registered
        """
        # Generate standardized bronze path (naming convention applied here)
        bronze_path = dataset.get_bronze_path()
        bronze_path.parent.mkdir(parents=True, exist_ok=True)

        logger.info(
            f"Converting to bronze for {dataset_name}",
            extra={
                "source": str(landing_path),
                "dest": str(bronze_path),
            },
        )

        # Apply dataset-specific bronze transformation
        transforms = get_bronze_transform(dataset_name)
        if transforms is None:
            raise NotImplementedError(
                f"No bronze transformation registered for dataset: {dataset_name}"
            )

        logger.info(
            f"Applying bronze transformations for {dataset_name}",
            extra={"landing_path": str(landing_path)},
        )

        # Pass both dataset config and actual landing file path
        df = transforms(dataset, landing_path)
        df.write_parquet(bronze_path)

        columns = df.columns
        row_count = len(df)

        logger.info(
            f"Bronze conversion complete for {dataset_name}",
            extra={
                "rows": row_count,
                "columns": len(columns),
                "bronze_path": str(bronze_path),
            },
        )

        return BronzeResult(row_count=row_count, columns=columns)

    @staticmethod
    def to_silver(
        bronze_result: BronzeResult,
        dataset_name: str,
        dataset: Dataset,
    ) -> SilverResult:
        """Apply business transformations to create silver layer.

        Silver layer transformations:
        1. Read bronze Parquet file
        2. Apply custom business logic (if registered)
        3. Write to versioned silver Parquet file

        Args:
            bronze_result: Result from bronze transformation (contains file path and SHA256s)
            dataset_name: Dataset identifier
            dataset: Dataset configuration

        Returns:
            SilverResult with silver file info and propagated SHA256s
        """
        silver_path = dataset.get_silver_path()
        silver_path.parent.mkdir(parents=True, exist_ok=True)
        bronze_path = dataset.get_bronze_path()

        logger.info(
            f"Transforming to silver for {dataset_name}",
            extra={"source": bronze_path.name},
        )

        # Apply dataset-specific silver transformation
        transforms = get_silver_transform(dataset_name)
        if transforms is None:
            raise NotImplementedError(
                f"No silver transformation registered for dataset: {dataset_name}"
            )

        logger.info(
            f"Applying custom silver transformation for {dataset_name}",
            extra={"dataset": dataset_name},
        )
        df = transforms(dataset)
        df.write_parquet(silver_path)

        columns = df.columns
        row_count = len(df)

        logger.info(
            f"Silver transformation complete for {dataset_name}",
            extra={"rows": row_count, "columns": len(columns)},
        )

        return SilverResult(row_count=row_count, columns=df.columns)
