"""Typed results for pipeline stages.

This module centralizes all result types used across pipeline stages,
providing strong typing and explicit conversion to/from serializable formats.

Design principles:
- Frozen dataclasses (immutable results)
- Explicit to_serializable() for Airflow XCom
- Path objects in typed layer, strings in serialized layer
- One result type per pipeline stage concept
- SHA256 tracking: archive hash + extracted file hash for integrity

Architecture:
    Pipeline Functions (typed) ←→ Airflow Tasks (dicts via XCom)

    Example flow:
    download() → HttpDownloadResult → to_serializable() → XCom dict
    XCom dict → from_xcom() → ExtractionResult → extract_archive()
"""

from pathlib import Path

from de_projet_perso.core.models import StrictModel
from de_projet_perso.utils.remote_checker import RemoteFileMetadata


class IngestionDecision(StrictModel):
    """todo."""

    should_ingest: bool
    is_healing: bool
    remote_metadata: RemoteFileMetadata


class DownloadStageResult(StrictModel):
    """todo."""

    version: str
    remote_metadata: RemoteFileMetadata
    # todo, prefix raw_
    path: Path
    sha256: str
    size_mib: float

    @property
    def landing_path(self) -> Path:
        """Todo."""
        return self.path


class ExtractionStageResult(DownloadStageResult):
    """todo."""

    extracted_file_path: Path
    extracted_file_sha256: str
    extracted_file_size_mib: float

    @property
    def landing_path(self) -> Path:
        """Todo."""
        return self.extracted_file_path


class BronzeStageResult(DownloadStageResult):
    """todo."""

    # Optional fields for ExtractionStageResult
    extracted_file_path: Path | None = None
    extracted_file_sha256: str | None = None
    extracted_file_size_mib: float | None = None

    bronze_file_size_mib: float
    bronze_row_count: int
    bronze_columns: list[str]


class SilverStageResult(BronzeStageResult):
    """todo."""

    silver_file_size_mib: float
    silver_row_count: int
    silver_columns: list[str]


class PipelineRunSnapshot(StrictModel):
    """todo."""

    # ingest
    remote_metadata: RemoteFileMetadata
    version: str
    raw_file_sha256: str
    raw_file_size_mib: float
    # extract
    extracted_file_sha256: str | None = None
    extracted_file_size_mib: float | None = None
    # bronze
    bronze_file_size_mib: float
    bronze_row_count: int
    bronze_columns: list[str]
    # silver
    silver_file_size_mib: float
    silver_row_count: int
    silver_columns: list[str]


class GoldStageResult(StrictModel):
    """Result from Gold layer transformation.

    Gold transformations join multiple Silver sources to create analytical
    datasets. This result captures the output metadata.

    Attributes:
        dataset_name: Name of the Gold dataset produced
        path: Path to the output parquet file (gold/*/current.parquet)
        row_count: Number of rows in the output
        columns: List of column names in the output
        file_size_mib: Output file size in mebibytes
        dependencies: List of Silver dataset names used as input
    """

    dataset_name: str
    path: Path
    row_count: int
    columns: list[str]
    file_size_mib: float
    dependencies: list[str]
