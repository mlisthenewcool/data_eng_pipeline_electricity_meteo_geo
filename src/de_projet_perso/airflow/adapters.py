"""Adapters to bridge typed pipeline logic with Airflow XCom serialization.

This module provides conversion utilities between:
- Typed pipeline results (dataclasses with Path, etc.)
- Serializable dicts (strings/numbers only, for Airflow XCom)

The adapter pattern ensures pipeline functions remain independent of Airflow
orchestration concerns. Pipeline logic works with rich types (Path, dataclasses),
while Airflow tasks handle serialization via this adapter.

Architecture:
    Pipeline Layer (typed) ←→ Adapter ←→ Airflow Layer (dicts)

Example:
    download() → DownloadResult → to_xcom() → {"path": "/data/...", ...}
    {"path": "/data/..."} → from_xcom_download() → DownloadResult → extract()

Usage:
    # In Airflow task (serialize for XCom)
    result = await download(...)
    return AirflowTaskAdapter.to_xcom(result)

    # In next task (deserialize from XCom)
    download_result = AirflowTaskAdapter.from_xcom_download(xcom_data)
    extraction_result = await extract(download_result.path, ...)
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, TypeVar

from de_projet_perso.pipeline.results import (
    BronzeResult,
    DownloadResult,
    ExtractionResult,
    LandingResult,
    SerializableResult,
    SilverResult,
)

T = TypeVar("T", bound=SerializableResult)


class AirflowTaskAdapter:
    """Adapter for converting between typed results and Airflow XCom dicts.

    This class provides bidirectional conversion:
    - to_xcom(): Typed result → XCom dict (generic, works for any result type)
    - from_xcom_*(): XCom dict → Typed result (specific methods per type)

    Design rationale:
    - Generic to_xcom() leverages Protocol (any SerializableResult works)
    - Specific from_xcom_*() methods provide type safety and IDE auto-complete
    - Centralized conversion logic (single source of truth)
    """

    @staticmethod
    def to_xcom(result: T) -> dict[str, Any]:
        """Serialize a typed result to dict for Airflow XCom.

        This generic method works for any result type implementing
        the SerializableResult protocol (has to_serializable() method).

        Args:
            result: Any pipeline result implementing SerializableResult

        Returns:
            Dict with string/numeric/list values suitable for XCom storage

        Example:
            download_result = DownloadResult(path=Path("/data/file.7z"), ...)
            xcom_dict = AirflowTaskAdapter.to_xcom(download_result)
            # → {"path": "/data/file.7z", "sha256": "abc...", "size_mib": 123.4}
        """
        return result.to_serializable()

    @staticmethod
    def from_xcom_download(data: dict[str, Any]) -> DownloadResult:
        """Deserialize XCom dict to DownloadResult.

        Args:
            data: XCom dict from download task

        Returns:
            Typed DownloadResult with Path object reconstructed

        Example:
            xcom = {"path": "/data/file.7z", "sha256": "abc...", "size_mib": 123.4}
            result = AirflowTaskAdapter.from_xcom_download(xcom)
            # → DownloadResult(path=Path("/data/file.7z"), ...)
        """
        return DownloadResult(
            path=Path(data["path"]),
            sha256=str(data["sha256"]),
            size_mib=float(data["size_mib"]),
            original_filename=str(data["original_filename"]),
        )

    @staticmethod
    def from_xcom_extraction(data: dict[str, Any]) -> ExtractionResult:
        """Deserialize XCom dict to ExtractionResult.

        Args:
            data: XCom dict from extraction task

        Returns:
            Typed ExtractionResult with Path object reconstructed

        Example:
            xcom = {
                "path": "/data/file.gpkg",
                "size_mib": 50.0,
                "extracted_sha256": "def...",
                "archive_sha256": "abc..."
            }
            result = AirflowTaskAdapter.from_xcom_extraction(xcom)
        """
        return ExtractionResult(
            path=Path(data["path"]),
            size_mib=float(data["size_mib"]),
            extracted_sha256=str(data["extracted_sha256"]),
            archive_sha256=str(data["archive_sha256"]),
            original_filename=str(data["original_filename"]),
        )

    @staticmethod
    def from_xcom_landing(data: dict[str, Any]) -> LandingResult:
        """Deserialize XCom dict to LandingResult.

        Args:
            data: XCom dict from landing validation task

        Returns:
            Typed LandingResult with Path object reconstructed
        """
        return LandingResult(
            path=Path(data["path"]),
            sha256=str(data["sha256"]),
            size_mib=float(data["size_mib"]),
            archive_sha256=str(data["archive_sha256"]),
            original_filename=str(data["original_filename"]),
            layer=str(data.get("layer", "landing")),
        )

    @staticmethod
    def from_xcom_bronze(data: dict[str, Any]) -> BronzeResult:
        """Deserialize XCom dict to BronzeResult.

        Args:
            data: XCom dict from bronze transformation task

        Returns:
            Typed BronzeResult with Path object reconstructed
        """
        return BronzeResult(
            path=Path(data["path"]),
            row_count=int(data["row_count"]),
            columns=list(data["columns"]),
            sha256=str(data["sha256"]),
            archive_sha256=str(data["archive_sha256"]),
            layer=str(data.get("layer", "bronze")),
        )

    @staticmethod
    def from_xcom_silver(data: dict[str, Any]) -> SilverResult:
        """Deserialize XCom dict to SilverResult.

        Args:
            data: XCom dict from silver transformation task

        Returns:
            Typed SilverResult with Path object reconstructed
        """
        return SilverResult(
            path=Path(data["path"]),
            row_count=int(data["row_count"]),
            columns=list(data["columns"]),
            sha256=str(data["sha256"]),
            archive_sha256=str(data["archive_sha256"]),
            layer=str(data.get("layer", "silver")),
        )
