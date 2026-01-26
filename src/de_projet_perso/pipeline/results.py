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

from pathlib import Path

from de_projet_perso.core.models import StrictModel
from de_projet_perso.pipeline.constants import PipelineAction
from de_projet_perso.utils.remote_checker import RemoteFileInfo


class CheckMetadataResult(StrictModel):
    """todo."""

    action: PipelineAction
    remote_metadata: RemoteFileInfo


class IngestResult(StrictModel):
    """todo."""

    version: str
    remote_metadata: RemoteFileInfo
    path: Path
    sha256: str
    size_mib: float

    @property
    def landing_path(self) -> Path:
        """Todo."""
        return self.path


class ExtractResult(IngestResult):
    """todo."""

    extracted_file_path: Path
    extracted_file_sha256: str
    extracted_file_size_mib: float

    @property
    def landing_path(self) -> Path:
        """Todo."""
        return self.extracted_file_path


class BronzeResult(IngestResult):
    """todo."""

    # Optional fields for ExtractResult
    extracted_file_path: Path | None = None
    extracted_file_sha256: str | None = None
    extracted_file_size_mib: float | None = None

    parquet_file_size_mib: float
    row_count: int
    columns: list[str]


class SilverResult(BronzeResult):
    """todo."""

    silver_parquet_file_size_mib: float
    silver_row_count: int
    silver_columns: list[str]


#
# @dataclass(frozen=True)
# class CheckMetadataResult:
#     """TODO."""
#
#     action: PipelineAction
#     remote_file_metadata: RemoteFileInfo
#
#     def to_serializable(self) -> dict[str, str | int | datetime | None]:
#         """Convert to dict for Airflow XCom serialization."""
#         return {
#             "action": self.action.value,
#             "etag": self.remote_file_metadata.etag,
#             "last_modified": self.remote_file_metadata.last_modified,
#             "content_length": self.remote_file_metadata.content_length,
#         }
#
#     @classmethod
#     def from_dict(cls, _dict: dict[str, Any]) -> Self:
#         """Convert from dict for Airflow XCom deserialization."""
#         return cls(
#             action=PipelineAction(_dict["action"]),
#             remote_file_metadata=RemoteFileInfo(
#                 etag=_dict["etag"],
#                 last_modified=_dict["last_modified"],
#                 content_length=_dict["content_length"],
#             ),
#         )
#
#
# @dataclass(frozen=True)
# class DownloadResult:
#     """Result from HTTP download operation.
#
#     Attributes:
#         path: Path to downloaded file (archive or final file)
#         sha256: SHA256 hash of downloaded content
#         size_mib: File size in mebibytes
#     """
#
#     path: Path
#     sha256: str
#     size_mib: float
#
#     def to_serializable(self) -> dict[str, str | float]:
#         """Convert to dict for Airflow XCom serialization."""
#         return {
#             "path": str(self.path),
#             "sha256": self.sha256,
#             "size_mib": self.size_mib,
#         }
#
#     @classmethod
#     def from_dict(cls, _dict: dict[str, Any]) -> Self:
#         """Convert from dict for Airflow XCom deserialization."""
#         return cls(
#             path=Path(_dict["path"]),
#             sha256=_dict["sha256"],
#             size_mib=_dict["size_mib"],
#         )
#
#
# @dataclass(frozen=True)
# class ExtractionResult:
#     """Result from archive extraction (or pass-through if not an archive).
#
#     For archive formats (7z, zip):
#         - extracted_sha256: SHA256 of the extracted file (recalculated for integrity)
#         - archive_sha256: SHA256 of the original archive (propagated from download)
#
#     For non-archive formats (direct .gpkg, .parquet):
#         - extracted_sha256 == archive_sha256 (same file)
#
#     Attributes:
#         archive_path: Path to extracted file (or original if not archive)
#         extracted_file_path: SHA256 of the final extracted file
#         extracted_file_sha256: SHA256 of the source archive (for traceability)
#         size_mib: File size in mebibytes
#     """
#
#     archive_path: Path
#     extracted_file_path: Path
#     extracted_file_sha256: str
#     size_mib: float
#
#     def to_serializable(self) -> dict[str, Path | str | float]:
#         """Convert to dict for Airflow XCom serialization."""
#         return {
#             "archive_path": str(self.archive_path),
#             "extracted_file_path": str(self.extracted_file_path),
#             "extracted_file_sha256": self.extracted_file_sha256,
#             "size_mib": self.size_mib,
#         }
#
#     @classmethod
#     def from_dict(cls, _dict: dict[str, Any]) -> Self:
#         """Convert from dict for Airflow XCom deserialization."""
#         return cls(
#             archive_path=Path(_dict["archive_path"]),
#             extracted_file_path=Path(_dict["extracted_file_path"]),
#             extracted_file_sha256=_dict["extracted_file_sha256"],
#             size_mib=_dict["size_mib"],
#         )
#
#
# @dataclass(frozen=True)
# class BronzeResult:
#     """Result from bronze layer transformation.
#
#     The bronze layer converts raw files to Parquet with:
#     - Normalized column names (snake_case)
#     - Type inference
#     - Optional custom transformations
#
#     Attributes:
#         row_count: Number of rows in dataset
#         columns: List of column names (post-normalization)
#     """
#
#     row_count: int
#     columns: list[str]
#
#     def to_serializable(self) -> dict[str, str | int | list[str]]:
#         """Convert to dict for Airflow XCom serialization."""
#         return {"row_count": self.row_count, "columns": self.columns}
#
#     @classmethod
#     def from_dict(cls, _dict: dict[str, Any]) -> Self:
#         """Convert from dict for Airflow XCom deserialization."""
#         return cls(row_count=_dict["row_count"], columns=_dict["columns"])
#
#
# @dataclass(frozen=True)
# class SilverResult:
#     """Result from silver layer transformation.
#
#     The silver layer applies business logic transformations:
#     - Data quality checks
#     - Derived columns
#     - Filtering
#     - Aggregations
#
#     This is the final curated dataset ready for consumption.
#
#     Attributes:
#         row_count: Number of rows in transformed dataset
#         columns: List of column names (post-transformation)
#     """
#
#     row_count: int
#     columns: list[str]
#
#     def to_serializable(self) -> dict[str, str | int | list[str]]:
#         """Convert to dict for Airflow XCom serialization."""
#         return {"row_count": self.row_count, "columns": self.columns}
#
#     @classmethod
#     def from_dict(cls, _dict: dict[str, Any]) -> Self:
#         """Convert from dict for Airflow XCom deserialization."""
#         return cls(row_count=_dict["row_count"], columns=_dict["columns"])
