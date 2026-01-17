"""Pipeline download and extraction logic.

This module handles downloading source files and extracting archives,
completely decoupled from Airflow orchestration.

Key features:
- Download from URL with SHA256 integrity check
- Extract archives (7z) with automatic file detection
- Recalculate SHA256 after extraction (detects corruption)
- Propagate both archive and extracted file hashes for traceability
"""

from pathlib import Path

from de_projet_perso.core.data_catalog import Dataset
from de_projet_perso.core.logger import logger
from de_projet_perso.pipeline.results import DownloadResult, ExtractionResult, LandingResult
from de_projet_perso.utils.downloader import (
    download_to_file,
    extract_7z,
)


class PipelineDownloader:
    """Download and extraction logic for pipeline data sources."""

    @staticmethod
    def download(
        dataset_name: str,
        dataset: Dataset,
        dest_dir: Path,
    ) -> DownloadResult:
        """Download source file from URL.

        Args:
            dataset_name: Dataset identifier
            dataset: Dataset configuration
            dest_dir: Destination directory (typically landing/{dataset_name})

        Returns:
            DownloadResult with path, sha256, and size
        """
        logger.info(f"Downloading {dataset_name}", extra={"url": str(dataset.source.url)})

        # Ensure destination directory exists
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_path = dest_dir / Path(dataset.source.url_as_str).name

        return download_to_file(
            url=dataset.source.url_as_str,
            dest_path=dest_path,
        )

    @staticmethod
    def extract_archive(
        archive_path: Path,
        dataset: Dataset,
        dest_dir: Path,
        archive_sha256: str,
    ) -> ExtractionResult:
        """Extract archive if format requires it and recalculate SHA256.

        For archive formats (7z):
            1. Extract the target file from archive
            2. Recalculate SHA256 of extracted file (detects corruption during extraction)
            3. Return both archive SHA256 (for traceability) and extracted SHA256

        For non-archive formats:
            1. Pass through the original file
            2. Both SHA256 values are identical (same file)

        Args:
            archive_path: Path to archive file (or direct file if not archive)
            dataset: Dataset configuration
            dest_dir: Destination directory for extraction
            archive_sha256: SHA256 of the downloaded archive (propagated from download)

        Returns:
            ExtractionResult with extracted file info and dual SHA256 tracking

        Raises:
            ValueError: If archive format requires inner_file but none specified
            FileNotFoundError: If extracted file doesn't exist after extraction
        """
        # Skip if not an archive format
        if not dataset.source.format.is_archive:
            logger.info(
                f"No extraction needed for {dataset.source.format}",
                extra={"format": str(dataset.source.format)},
            )
            # Not an archive: the downloaded file IS the final file
            # Both SHA256 values are the same
            return ExtractionResult(
                path=archive_path,
                size_mib=archive_path.stat().st_size / (1024**2) if archive_path.exists() else 0,
                extracted_sha256=archive_sha256,  # Same as archive (no extraction)
                archive_sha256=archive_sha256,
            )

        if dataset.source.inner_file is None:
            raise ValueError(f"inner_file required for archive format: {dataset.source.format}")

        dest_path = dest_dir / dataset.source.inner_file

        logger.info(
            "Extracting archive",
            extra={"archive": archive_path.name, "target": dataset.source.inner_file},
        )

        # Extract file from archive (low-level utility)
        file_info = extract_7z(
            archive_path=archive_path,
            target_filename=dataset.source.inner_file,
            dest_path=dest_path,
            validate_sqlite=dest_path.suffix == ".gpkg",
        )

        # Assemble ExtractionResult with full traceability
        logger.info(
            "Extraction completed with integrity check",
            extra={
                "extracted_file": dest_path.name,
                "size_mib": file_info.size_mib,
                "extracted_sha256": file_info.sha256,
                "archive_sha256": archive_sha256,
            },
        )

        return ExtractionResult(
            path=file_info.path,
            size_mib=file_info.size_mib,
            extracted_sha256=file_info.sha256,  # From extracted file
            archive_sha256=archive_sha256,  # Propagated from download
        )

    @staticmethod
    def validate_landing(extraction_result: ExtractionResult) -> LandingResult:
        """Validate and record landing layer file.

        The landing layer contains the raw file ready for transformation.
        This validation ensures the file exists and is ready for bronze conversion.

        Args:
            extraction_result: Result from extraction step (contains file path and SHA256s)

        Returns:
            LandingResult with validated file metadata

        Raises:
            FileNotFoundError: If expected file does not exist
        """
        if not extraction_result.path.exists():
            raise FileNotFoundError(f"Expected file not found: {extraction_result.path}")

        logger.info(
            "File saved to landing",
            extra={
                "path": str(extraction_result.path),
                "size_mib": extraction_result.size_mib,
                "sha256": extraction_result.extracted_sha256,
            },
        )

        return LandingResult(
            path=extraction_result.path,
            sha256=extraction_result.extracted_sha256,  # Use extracted file SHA256
            size_mib=extraction_result.size_mib,
            archive_sha256=extraction_result.archive_sha256,  # Propagate for traceability
        )
