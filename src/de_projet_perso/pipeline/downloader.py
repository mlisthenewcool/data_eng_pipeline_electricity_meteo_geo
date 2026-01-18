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
    def download(dataset: Dataset) -> DownloadResult:
        """Download source file from URL.

        Downloads to landing directory preserving original filename from server.
        The filename is extracted from Content-Disposition header or URL path.

        Args:
            dataset: Dataset configuration

        Returns:
            DownloadResult with path, sha256, size, and original_filename
        """
        # Get landing directory (not file path - preserves original filename)
        landing_dir = dataset.get_landing_dir()

        return download_to_file(
            url=dataset.source.url_as_str,
            dest_dir=landing_dir,
        )

    @staticmethod
    def extract_archive(
        archive_path: Path,
        dataset: Dataset,
        archive_sha256: str,
    ) -> ExtractionResult:
        """Extract archive and recalculate SHA256.

        For archive formats (7z):
            1. Extract the target file from archive
            2. Recalculate SHA256 of extracted file (detects corruption during extraction)
            3. Return both archive SHA256 (for traceability) and extracted SHA256

        Args:
            archive_path: Path to archive file (or direct file if not archive)
            dataset: Dataset configuration
            archive_sha256: SHA256 of the downloaded archive (propagated from download)

        Returns:
            ExtractionResult with extracted file info, dual SHA256 tracking, and original filename

        Raises:
            ValueError: If archive format requires inner_file but none specified
            FileNotFoundError: If extracted file doesn't exist after extraction
        """
        if not dataset.source.format.is_archive:
            logger.warning(f"Trying to extract a non-archive format: {dataset.source.format.value}")
            raise Exception  # TODO exception spécifique

        if dataset.source.inner_file is None:
            raise ValueError(f"inner_file required for archive format: {dataset.source.format}")

        # Extract to same directory as archive (preserves directory structure)
        landing_dir = archive_path.parent

        file_info = extract_7z(
            archive_path=archive_path,
            target_filename=dataset.source.inner_file,
            dest_dir=landing_dir,
            validate_sqlite=Path(dataset.source.inner_file).suffix == ".gpkg",
        )

        logger.debug(
            "Extraction completed with integrity check",
            extra={
                "extracted_file": file_info.path,
                "size_mib": file_info.size_mib,
                "extracted_sha256": file_info.sha256,
                "archive_sha256": archive_sha256,
                "original_filename": dataset.source.inner_file,
            },
        )

        return ExtractionResult(
            path=file_info.path,
            size_mib=file_info.size_mib,
            extracted_sha256=file_info.sha256,
            archive_sha256=archive_sha256,
            original_filename=dataset.source.inner_file,
        )

    @staticmethod
    def validate_landing(extraction_result: ExtractionResult) -> LandingResult:
        """Validate and record landing layer file.

        The landing layer contains the raw file ready for transformation.
        This validation ensures the file exists and is ready for bronze conversion.
        Original filenames are preserved for audit trail and traceability.

        Args:
            extraction_result: Result from extraction step (contains file path and SHA256s)

        Returns:
            LandingResult with validated file metadata and original filename

        Raises:
            FileNotFoundError: If expected file does not exist
        """
        if not extraction_result.path.exists():
            raise FileNotFoundError(f"Expected file not found: {extraction_result.path}")

        logger.info(
            "File saved to landing",
            extra={
                "path": str(extraction_result.path),
                "original_filename": extraction_result.original_filename,
                "size_mib": extraction_result.size_mib,
                "sha256": extraction_result.extracted_sha256,
            },
        )

        return LandingResult(
            path=extraction_result.path,
            sha256=extraction_result.extracted_sha256,  # Use extracted file SHA256
            size_mib=extraction_result.size_mib,
            archive_sha256=extraction_result.archive_sha256,  # Propagate for traceability
            original_filename=extraction_result.original_filename,  # Preserve original name
        )
