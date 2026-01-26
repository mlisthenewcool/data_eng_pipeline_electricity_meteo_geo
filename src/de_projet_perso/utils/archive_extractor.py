"""TODO, doc."""

import shutil
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

import py7zr
from py7zr.callbacks import ExtractCallback
from tqdm import tqdm

from de_projet_perso.core.exceptions import (
    ArchiveNotFoundError,
    FileIntegrityError,
    FileNotFoundInArchiveError,
)
from de_projet_perso.core.logger import TqdmToLoguru, logger
from de_projet_perso.core.settings import settings
from de_projet_perso.utils.hasher import FileHasher


@dataclass(frozen=True)
class ExtractionResult:
    """Result from archive extraction.

    Attributes:
        archive_path: Path to extracted file (or original if not archive)
        extracted_file_path: SHA256 of the final extracted file
        extracted_file_sha256: SHA256 of the source archive (for traceability)
        size_mib: File size in mebibytes
    """

    archive_path: Path
    extracted_file_path: Path
    extracted_file_sha256: str
    size_mib: float

    def to_dict(self) -> dict[str, str | float]:
        """Convert to dict for serialization."""
        return {
            "archive_path": str(self.archive_path),
            "extracted_file_path": str(self.extracted_file_path),
            "extracted_file_sha256": self.extracted_file_sha256,
            "size_mib": self.size_mib,
        }


class TqdmExtractCallback(ExtractCallback):
    """Bridge between py7zr extraction and tqdm progress bar."""

    def __init__(self, pbar: tqdm):
        """..."""
        self.pbar = pbar

    def report_start(self, processing_file_path: str, processing_bytes: str) -> None:
        """..."""
        pass

    def report_end(self, processing_file_path: str, wrote_bytes: str) -> None:
        """..."""
        pass

    def report_update(self, decompressed_bytes: str) -> None:
        """..."""
        # self.pbar.n += int(decompressed_bytes)
        # self.pbar.refresh()
        self.pbar.update(int(decompressed_bytes))

    def report_start_preparation(self) -> None:
        """Called when a file extraction starts."""
        pass

    def report_warning(self, message: str) -> None:
        """Called when a file extraction starts."""
        pass

    def report_postprocess(self) -> None:
        """Called when post-processing starts."""
        pass


# =============================================================================
# Archive extraction
# =============================================================================


@dataclass(frozen=True)
class ExtractionInfo:
    """Information about a file extracted from an archive.

    This is a low-level utility data structure containing only
    information about the extracted file itself, without metadata
    from the extraction context (archive hash, download info, etc.).

    For pipeline orchestration with full traceability, use ExtractionResult
    from de_projet_perso.pipeline.results instead.

    Attributes:
        path: Path to the extracted file
        size_mib: File size in mebibytes
        sha256: SHA256 hash of the extracted file content

    Example:
        file_info = extract_7z(archive_path, "data.gpkg", dest_path)
        print(f"Extracted {file_info.path}: {file_info.sha256}")
    """

    path: Path
    size_mib: float
    sha256: str


def validate_sqlite_header(path: Path) -> None:
    """Validate that a file has a valid SQLite/GeoPackage header.

    GeoPackage files are SQLite databases. This performs a quick validation
    by checking the first 16 bytes match the SQLite magic header.

    Args:
        path: Path to the file to validate.

    Raises:
        FileIntegrityError: If file is missing, empty, or has invalid header.
    """
    if not path.exists():
        raise FileIntegrityError(path, "File does not exist")

    if path.stat().st_size == 0:
        raise FileIntegrityError(path, "File is empty")

    try:
        with path.open(mode="rb") as f:
            header = f.read(16)
            if header != b"SQLite format 3\x00":
                raise FileIntegrityError(path, "Invalid SQLite/GeoPackage header")
    except OSError as e:
        raise FileIntegrityError(path, f"Could not read file header: {e}")


def extract_7z(
    archive_path: Path,
    target_filename: str,
    dest_dir: Path,
    validate_sqlite: bool = True,
) -> ExtractionInfo:
    """Extract a specific file from a 7z archive.

    This is a low-level utility function that handles pure extraction logic.
    For pipeline integration with full traceability (archive hash propagation),
    use PipelineDownloader.extract_archive() instead.

    Searches for target_filename within the archive, extracts it to a
    temporary directory, then atomically moves it to the destination directory,
    preserving the original filename.

    Args:
        archive_path: Path to the .7z archive.
        target_filename: Name or suffix of file to extract (handles nested paths).
        dest_dir: Directory where the extracted file will be saved.
        validate_sqlite: If True, validate SQLite header after extraction.

    Returns:
        ExtractionInfo with path, size, and SHA256 of extracted file only.

    Raises:
        ArchiveNotFoundError: If archive_path doesn't exist.
        FileNotFoundInArchiveError: If target_filename not found in archive.
        FileIntegrityError: If validation enabled and file is invalid.
    """
    if not archive_path.exists():
        raise ArchiveNotFoundError(archive_path)

    logger.info(
        "Starting extraction",
        extra={"archive": archive_path.name, "target": target_filename},
    )

    with tempfile.TemporaryDirectory(prefix="7z_extract_") as tmp_dir:
        tmp_dir_path = Path(tmp_dir)

        with py7zr.SevenZipFile(archive_path, mode="r") as archive:
            all_files = archive.getnames()

            # Flexible search: IGN archives have inconsistent internal structures
            # e.g., "CONTOURS-IRIS_3-0/iris.gpkg" when we search for "iris.gpkg"
            try:
                target_internal_path = next(f for f in all_files if f.endswith(target_filename))
            except StopIteration:
                raise FileNotFoundInArchiveError(target_filename, archive_path)

            logger.info(f"Found target in archive: {target_internal_path}")

            # Récupérer les infos du fichier pour connaître sa taille décompressée
            target_info = next(
                info for info in archive.list() if info.filename == target_internal_path
            )
            uncompressed_size = target_info.uncompressed

            # Extract to temp directory
            # Initialisation de tqdm
            with tqdm(
                total=uncompressed_size,
                unit="B",
                unit_scale=True,
                desc=f"Extracting {target_filename}",
                leave=False,
                file=TqdmToLoguru(logger.info) if settings.is_running_on_airflow else sys.stderr,
                mininterval=5.0 if settings.is_running_on_airflow else 1.0,
            ) as pbar:
                # Le callback reçoit le nombre d'octets écrits durant l'intervalle
                extraction_callback = TqdmExtractCallback(pbar)

                archive.extract(
                    path=tmp_dir_path, targets=[target_internal_path], callback=extraction_callback
                )

            extracted_file = tmp_dir_path / target_internal_path

            # Compute final destination path (preserve original filename from archive)
            dest_path = dest_dir / target_filename
            dest_path.parent.mkdir(parents=True, exist_ok=True)

            # Atomic move to final destination
            if dest_path.exists():
                dest_path.unlink()
            shutil.move(src=extracted_file, dst=dest_path)

            # Optional SQLite validation for GeoPackage files
            if validate_sqlite:
                try:
                    validate_sqlite_header(dest_path)
                except FileIntegrityError:
                    # Clean up invalid file
                    if dest_path.exists():
                        dest_path.unlink()
                    raise

            extracted_file_hash = FileHasher.hash_file(dest_path)
            size_mib = round(dest_path.stat().st_size / 1024**2, 2)

            logger.info(
                "Extraction completed",
                extra={
                    "path": dest_path,
                    "size_mib": size_mib,
                    "sha256": extracted_file_hash,
                },
            )

            return ExtractionInfo(
                path=dest_path,
                size_mib=size_mib,
                sha256=extracted_file_hash,
            )
