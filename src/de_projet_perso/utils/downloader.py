"""Download and archive extraction utilities.

This module provides robust utilities for:
- Downloading large files over HTTP/2 with streaming
- Progress bars with tqdm and SHA256 integrity checks
- Extracting specific files from 7z archives

The download functions are designed for data pipelines where reliability is critical.
Airflow handles retries at the task level, so no retry logic is included here.

Example:
    # Download a file
    >>> result = download_to_file(url, dest_path)
    ... print(f"Downloaded {result.size_mib} MiB, SHA256: {result.sha256}")

    # Extract from archive (low-level utility)
    >>> file_info = extract_7z(archive_path, "data.gpkg", output_path)
    ... print(f"Extracted: {file_info.path}, SHA256: {file_info.sha256}")
"""

import shutil
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

import httpx
import py7zr
from py7zr.callbacks import ExtractCallback
from tqdm import tqdm

from de_projet_perso.core.exceptions import (
    ArchiveNotFoundError,
    FileIntegrityError,
    FileNotFoundInArchiveError,
)
from de_projet_perso.core.logger import logger
from de_projet_perso.core.settings import settings
from de_projet_perso.pipeline.results import DownloadResult
from de_projet_perso.utils.hasher import FileHasher


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


class TqdmToLoguru:
    """Proxy that redirects tqdm output to Loguru logger."""

    def __init__(self, logger_func):
        """Todo."""
        self.logger_func = logger_func
        self.buf = ""

    def write(self, buf):
        """Todo."""
        # tqdm envoie souvent des segments de texte avec \r
        self.buf = buf.strip("\r\n\t ")
        if self.buf:
            self.logger_func(self.buf)

    def flush(self):
        """Todo."""
        pass


def download_to_file(url: str, dest_path: Path) -> DownloadResult:
    """Download a file from URL with streaming, progress bar, and SHA256.

    Performs memory-efficient download by streaming chunks to disk.
    Automatically creates parent directories if needed.
    Uses HTTP/2 when available for better performance.

    Args:
        url: URL of the file to download.
        dest_path: Local path where the file will be saved.

    Returns:
        DownloadResult with path, sha256, and size_mib.

    Raises:
        httpx.HTTPStatusError: If server returns error status (4xx/5xx).
        httpx.TimeoutException: If request times out.

    Example:
        result = download_to_file(
            "https://example.com/data.7z",
            Path("/data/landing/data.7z")
        )
        print(f"Downloaded {result.size_mib} MiB, hash: {result.sha256}")
    """
    if dest_path.exists():
        logger.warning(message="File already exists", extra={"url": url})

    logger.info("Starting download", extra={"url": url, "will_save_to": dest_path.name})

    timeout = httpx.Timeout(  # TODO, documenter & ajouter arguments write/pool
        timeout=settings.download_timeout_total,
        connect=settings.download_timeout_connect,
        read=settings.download_timeout_sock_read,
        write=None,
        pool=None,
    )

    with httpx.Client(http2=True, timeout=timeout, follow_redirects=True) as client:
        with client.stream("GET", url) as response:
            response.raise_for_status()

            dest_path.parent.mkdir(parents=True, exist_ok=True)
            total_bytes = 0
            total_size = int(response.headers.get("content-length", 0))

            hasher = FileHasher()

            progress_bar = tqdm(
                total=total_size,
                unit="iB",
                unit_scale=True,
                unit_divisor=1024,
                desc=f"Downloading {dest_path.name}",
                file=TqdmToLoguru(logger.info) if settings.is_running_on_airflow else sys.stderr,
                leave=False,  # disappears when complete
                # dynamic_ncols=True,
            )

            try:
                with open(dest_path, mode="wb") as f:
                    for chunk in response.iter_bytes(chunk_size=settings.download_chunk_size):
                        f.write(chunk)
                        hasher.update(chunk)
                        chunk_len = len(chunk)
                        total_bytes += chunk_len
                        progress_bar.update(chunk_len)
            finally:
                progress_bar.close()

            sha256_result = hasher.hexdigest

            logger.info(
                "Download completed",
                extra={
                    "path": str(dest_path),
                    "size_mib": round(total_bytes / (1024**2), 2),
                    "sha256": sha256_result,
                },
            )

            return DownloadResult(path=dest_path, sha256=sha256_result, size_mib=total_bytes)


# =============================================================================
# Archive extraction
# =============================================================================


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
    dest_path: Path,
    validate_sqlite: bool = True,
) -> ExtractionInfo:
    """Extract a specific file from a 7z archive.

    This is a low-level utility function that handles pure extraction logic.
    For pipeline integration with full traceability (archive hash propagation),
    use PipelineDownloader.extract_archive() instead.

    Searches for target_filename within the archive, extracts it to a
    temporary directory, then atomically moves it to dest_path.

    Args:
        archive_path: Path to the .7z archive.
        target_filename: Name or suffix of file to extract (handles nested paths).
        dest_path: Final destination path for extracted file.
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

            logger.info(
                "Found target in archive",
                extra={"internal_path": target_internal_path},
            )

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
                # dynamic_ncols=True,
            ) as pbar:
                # Le callback reçoit le nombre d'octets écrits durant l'intervalle
                extraction_callback = TqdmExtractCallback(pbar)

                archive.extract(
                    path=tmp_dir_path, targets=[target_internal_path], callback=extraction_callback
                )

            extracted_file = tmp_dir_path / target_internal_path

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
                    "path": str(dest_path),
                    "size_mib": size_mib,
                    "sha256": extracted_file_hash,
                },
            )

            return ExtractionInfo(
                path=dest_path,
                size_mib=size_mib,
                sha256=extracted_file_hash,
            )


# =============================================================================
# CLI entry point for testing
# =============================================================================


def _test_download() -> None:
    """Test download with IGN ADMIN-EXPRESS-COG dataset."""
    url = (
        "https://data.geopf.fr/telechargement/download/ADMIN-EXPRESS-COG/"
        "ADMIN-EXPRESS-COG_4-0__GPKG_WGS84G_FRA_2025-01-01/"
        "ADMIN-EXPRESS-COG_4-0__GPKG_WGS84G_FRA_2025-01-01.7"
    )
    archive_path = settings.data_dir_path / "landing" / "ADMIN-EXPRESS-COG.7z"
    dest_path = settings.data_dir_path / "landing" / "admin_express_cog.gpkg"

    # Download
    try:
        download_result = download_to_file(url, archive_path)
        logger.info(
            "Download succeeded",
            extra={
                "archive_sha256": download_result.sha256,
                "size_mib": download_result.size_mib,
            },
        )
    except httpx.HTTPStatusError as e:
        logger.error(
            f"Download failed. Server returned code: {e.response.status_code}",
            extra={
                "message": e.response.reason_phrase,
                "url": str(e.request.url),
            },
        )
        sys.exit(-1)
    except httpx.TimeoutException as e:
        logger.error("Download failed. Connection timed out", extra={"more_infos": e})
        sys.exit(-1)
    except httpx.HTTPError as e:
        logger.error("Download failed. Network or request error", extra={"more_infos": e})
        sys.exit(-1)
    except Exception as e:
        # TODO, simuler disque plein ?
        logger.critical("Download failed. Unexpected error", extra={"more_infos": str(e)})
        sys.exit(-1)

    # Extract
    try:
        file_info = extract_7z(
            archive_path=archive_path,
            target_filename="ADE-COG_4-0_GPKG_WGS84G_FRA-ED2025-01-01.gpkg",
            dest_path=dest_path,
            validate_sqlite=True,
        )
        logger.info(
            "Extraction succeeded",
            extra={
                "extracted_file_sha256": file_info.sha256,
                "archive_sha256": download_result.sha256,
            },
        )
    except (
        ArchiveNotFoundError,
        FileNotFoundInArchiveError,
        FileIntegrityError,
    ) as e:
        logger.error("Extraction failed", extra={"error": str(e)})
        sys.exit(-1)


if __name__ == "__main__":
    _test_download()
