"""Base pipeline classes for dataset-specific transformations.

This module provides abstract base classes for implementing dataset-specific
pipeline logic. The actual pipeline orchestration is handled by Airflow DAGs
in airflow/dags/dataset_pipelines_factory.py.

For pipeline state management, see pipeline_state.py.
"""

from abc import ABC, abstractmethod
from pathlib import Path

import polars as pl

from de_projet_perso.core.catalog import Dataset
from de_projet_perso.core.settings import DATA_DIR


class BaseDatasetPipeline(ABC):
    """Abstract base class for dataset-specific pipeline transformations.

    Subclass this to implement dataset-specific logic for bronze-to-silver
    transformations. The download, extract, and landing stages are handled
    generically by the DAG factory.

    Example:
        class IGNContoursPipeline(BaseDatasetPipeline):
            def transform_to_silver(self, bronze_df: pl.DataFrame) -> pl.DataFrame:
                return bronze_df.with_columns(
                    pl.col("geometry").cast(pl.Utf8).alias("geometry_wkt")
                )
    """

    def __init__(self, dataset: Dataset) -> None:
        """Initialize with dataset configuration.

        Args:
            dataset: Dataset configuration from catalog
        """
        self.dataset = dataset
        self.landing_dir = DATA_DIR / "landing" / self._get_dataset_name()
        self.bronze_dir = DATA_DIR / "bronze" / self._get_dataset_name()
        self.silver_dir = DATA_DIR / "silver" / self._get_dataset_name()

    def _get_dataset_name(self) -> str:
        """Extract dataset name from storage path."""
        parts = self.dataset.storage.split("/")
        return parts[1] if len(parts) > 1 else parts[0]

    @abstractmethod
    def transform_to_silver(self, bronze_df: pl.DataFrame) -> pl.DataFrame:
        """Apply dataset-specific transformations.

        Override this method to implement business logic transformations
        from bronze to silver layer.

        Args:
            bronze_df: DataFrame from bronze layer (Parquet with snake_case columns)

        Returns:
            Transformed DataFrame ready for silver layer
        """

    def get_bronze_path(self) -> Path:
        """Get path to bronze layer file for current version."""
        return self.bronze_dir / f"{self.dataset.ingestion.version}.parquet"

    def get_silver_path(self) -> Path:
        """Get path to silver layer file for current version."""
        return self.silver_dir / f"{self.dataset.ingestion.version}.parquet"


class DefaultPipeline(BaseDatasetPipeline):
    """Default pipeline that copies bronze to silver without transformation.

    Use this for datasets that don't require custom transformation logic.
    """

    def transform_to_silver(self, bronze_df: pl.DataFrame) -> pl.DataFrame:
        """Pass through without transformation."""
        return bronze_df


# Registry for dataset-specific pipelines
_PIPELINE_REGISTRY: dict[str, type[BaseDatasetPipeline]] = {}


def register_pipeline(dataset_name: str):
    """Decorator to register a pipeline class for a dataset.

    Example:
        @register_pipeline("ign_contours_iris")
        class IGNContoursPipeline(BaseDatasetPipeline):
            ...
    """

    def decorator(cls: type[BaseDatasetPipeline]) -> type[BaseDatasetPipeline]:
        _PIPELINE_REGISTRY[dataset_name] = cls
        return cls

    return decorator


def get_pipeline(dataset_name: str, dataset: Dataset) -> BaseDatasetPipeline:
    """Get pipeline instance for a dataset.

    Returns registered pipeline if available, otherwise DefaultPipeline.

    Args:
        dataset_name: Dataset identifier
        dataset: Dataset configuration

    Returns:
        Pipeline instance for the dataset
    """
    pipeline_class = _PIPELINE_REGISTRY.get(dataset_name, DefaultPipeline)
    return pipeline_class(dataset)
