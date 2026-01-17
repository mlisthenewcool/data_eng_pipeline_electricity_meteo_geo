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
    download() → DownloadResult → to_serializable() → XCom dict
    XCom dict → from_xcom() → ExtractionResult → extract_archive()
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol


class SerializableResult(Protocol):
    """Protocol for all pipeline stage results.

    All result types must implement to_serializable() to enable
    conversion to Airflow XCom-compatible dictionaries.
    """

    def to_serializable(self) -> dict[str, Any]:
        """Convert to dictionary for Airflow XCom serialization."""
        ...


@dataclass(frozen=True)
class DownloadResult:
    """Result from HTTP download operation.

    Attributes:
        path: Path to downloaded file (archive or final file)
        sha256: SHA256 hash of downloaded content
        size_mib: File size in mebibytes
    """

    path: Path
    sha256: str
    size_mib: float

    def to_serializable(self) -> dict[str, str | float]:
        """Convert to dict for Airflow XCom serialization.

        Returns:
            Dict with string/numeric values only (Path → str)
        """
        return {
            "path": str(self.path),
            "sha256": self.sha256,
            "size_mib": self.size_mib,
        }


@dataclass(frozen=True)
class ExtractionResult:
    """Result from archive extraction (or pass-through if not an archive).

    For archive formats (7z, zip):
        - extracted_sha256: SHA256 of the extracted file (recalculated for integrity)
        - archive_sha256: SHA256 of the original archive (propagated from download)

    For non-archive formats (direct .gpkg, .parquet):
        - extracted_sha256 == archive_sha256 (same file)

    Attributes:
        path: Path to extracted file (or original if not archive)
        size_mib: File size in mebibytes
        extracted_sha256: SHA256 of the final extracted file
        archive_sha256: SHA256 of the source archive (for traceability)
    """

    path: Path
    size_mib: float
    extracted_sha256: str
    archive_sha256: str

    def to_serializable(self) -> dict[str, str | float]:
        """Convert to dict for Airflow XCom serialization.

        Returns:
            Dict with string/numeric values only
        """
        return {
            "path": str(self.path),
            "size_mib": self.size_mib,
            "extracted_sha256": self.extracted_sha256,
            "archive_sha256": self.archive_sha256,
        }


@dataclass(frozen=True)
class LandingResult:
    """Result from landing layer validation.

    The landing layer stores raw files after download/extraction
    but before any transformation. This result confirms the file
    is valid and ready for bronze conversion.

    Attributes:
        path: Path to validated landing file
        sha256: SHA256 of the file (extracted_sha256 from extraction)
        size_mib: File size in mebibytes
        archive_sha256: Original archive SHA256 (for traceability)
        layer: Always "landing"
    """

    path: Path
    sha256: str
    size_mib: float
    archive_sha256: str
    layer: str = "landing"

    def to_serializable(self) -> dict[str, str | float]:
        """Convert to dict for Airflow XCom serialization.

        Returns:
            Dict with string/numeric values only
        """
        return {
            "path": str(self.path),
            "sha256": self.sha256,
            "size_mib": self.size_mib,
            "archive_sha256": self.archive_sha256,
            "layer": self.layer,
        }


@dataclass(frozen=True)
class BronzeResult:
    """Result from bronze layer transformation.

    The bronze layer converts raw files to Parquet with:
    - Normalized column names (snake_case)
    - Type inference
    - Optional custom transformations

    Attributes:
        path: Path to bronze Parquet file
        row_count: Number of rows in dataset
        columns: List of column names (post-normalization)
        sha256: SHA256 of source file (propagated from landing)
        archive_sha256: Original archive SHA256 (for traceability)
        layer: Always "bronze"
    """

    path: Path
    row_count: int
    columns: list[str]
    sha256: str
    archive_sha256: str
    layer: str = "bronze"

    def to_serializable(self) -> dict[str, str | int | list[str]]:
        """Convert to dict for Airflow XCom serialization.

        Returns:
            Dict with string/numeric/list values only
        """
        return {
            "path": str(self.path),
            "row_count": self.row_count,
            "columns": self.columns,
            "sha256": self.sha256,
            "archive_sha256": self.archive_sha256,
            "layer": self.layer,
        }


@dataclass(frozen=True)
class SilverResult:
    """Result from silver layer transformation.

    The silver layer applies business logic transformations:
    - Data quality checks
    - Derived columns
    - Filtering
    - Aggregations

    This is the final curated dataset ready for consumption.

    Attributes:
        path: Path to silver Parquet file
        row_count: Number of rows in transformed dataset
        columns: List of column names (post-transformation)
        sha256: SHA256 of source file (propagated from landing)
        archive_sha256: Original archive SHA256 (for traceability)
        layer: Always "silver"
    """

    path: Path
    row_count: int
    columns: list[str]
    sha256: str
    archive_sha256: str
    layer: str = "silver"

    def to_serializable(self) -> dict[str, str | int | list[str]]:
        """Convert to dict for Airflow XCom serialization.

        Returns:
            Dict with string/numeric/list values only
        """
        return {
            "path": str(self.path),
            "row_count": self.row_count,
            "columns": self.columns,
            "sha256": self.sha256,
            "archive_sha256": self.archive_sha256,
            "layer": self.layer,
        }
