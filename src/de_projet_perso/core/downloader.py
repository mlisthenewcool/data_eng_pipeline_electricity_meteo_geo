"""Async download and archive extraction utilities.

This module provides robust utilities for:
- Downloading large files over HTTP with automatic retry and exponential backoff
- Streaming downloads with progress bars and SHA256 checksums
- Extracting specific files from 7z archives

The download functions are designed for data pipelines where reliability is critical.
Network failures trigger automatic retries with exponential backoff.

Example:
    async with aiohttp.ClientSession() as session:
        result = await download_to_file(session, url, dest_path)
        print(f"Downloaded {result['bytes']} bytes, SHA256: {result['sha256']}")

    await extract_7z_async(archive_path, "data.gpkg", output_path)
"""

import asyncio
import hashlib
import shutil
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from functools import wraps
from pathlib import Path
from typing import Any, Callable, Coroutine, ParamSpec, TypeVar

import aiofiles
import aiohttp
import py7zr
from py7zr.callbacks import ExtractCallback
from tqdm.asyncio import tqdm

from de_projet_perso.core.enums import ExistingFileAction
from de_projet_perso.core.exceptions import (
    ArchiveNotFoundError,
    FileIntegrityError,
    FileNotFoundInArchiveError,
    FileShouldNotExist,
    RetryExhaustedError,
)
from de_projet_perso.core.logger import _is_airflow_context, logger
from de_projet_perso.core.settings import (
    DATA_DIR,
    DOWNLOAD_CHUNK_SIZE,
    DOWNLOAD_TIMEOUT_CONNECT,
    DOWNLOAD_TIMEOUT_SOCK_READ,
    DOWNLOAD_TIMEOUT_TOTAL,
    RETRY_BACKOFF_FACTOR,
    RETRY_INITIAL_DELAY,
    RETRY_MAX_ATTEMPTS,
)

# =============================================================================
# Type definitions
# =============================================================================

T = TypeVar("T")
P = ParamSpec("P")
# AsyncFunc = Callable[P, Coroutine[Any, Any, T]]

# =============================================================================
# Retry decorator
# =============================================================================


def stream_retry(
    max_retries: int = RETRY_MAX_ATTEMPTS,
    start_delay: float = RETRY_INITIAL_DELAY,
    backoff_factor: float = RETRY_BACKOFF_FACTOR,
    exceptions: tuple[type[Exception], ...] = (aiohttp.ClientError, asyncio.TimeoutError),
) -> Callable[[Callable[P, Coroutine[Any, Any, T]]], Callable[P, Coroutine[Any, Any, T]]]:
    """Decorator that retries an async function with exponential backoff.

    Catches specified exceptions and retries the decorated function until
    max_retries is reached. Delay between retries grows exponentially.

    Args:
        max_retries: Maximum retry attempts before giving up.
        start_delay: Initial delay between retries in seconds.
        backoff_factor: Multiplier applied to delay after each retry.
        exceptions: Exception types to catch and retry on.

    Returns:
        Decorated async function with retry logic.

    Raises:
        RetryExhaustedError: When all retry attempts are exhausted.
    """

    def decorator(func: Callable[P, Coroutine[Any, Any, T]]) -> Callable[P, Coroutine[Any, Any, T]]:
        @wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            attempt = 0
            current_delay = start_delay
            last_exception: Exception | None = None

            # Extract URL from args/kwargs for error reporting
            url = kwargs.get("url") or (args[1] if len(args) > 1 else "unknown")

            while attempt <= max_retries:
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    attempt += 1

                    if attempt > max_retries:
                        break

                    logger.warning(
                        "Retrying after error",
                        extra={
                            "attempt": attempt,
                            "max_attempts": max_retries + 1,
                            "delay_sec": round(current_delay, 1),
                            "error": str(e),
                        },
                    )
                    await asyncio.sleep(current_delay)
                    current_delay *= backoff_factor

            # All retries exhausted
            raise RetryExhaustedError(
                url=str(url),
                attempts=attempt,
                last_error=last_exception or RuntimeError("Unknown failure"),
            )

        return wrapper

    return decorator


# =============================================================================
# Download functions
# =============================================================================


@dataclass
class DownloadResult:  # noqa: D101
    path: Path
    sha256: str
    size_mib: int


@stream_retry()
async def download_to_file(
    session: aiohttp.ClientSession, url: str, dest_path: Path, if_exists: ExistingFileAction
) -> DownloadResult | None:
    """Download a file from URL with streaming, progress bar, and SHA256.

    Performs memory-efficient download by streaming chunks to disk.
    Automatically creates parent directories if needed.

    Args:
        session: Active aiohttp ClientSession for the request.
        url: URL of the file to download.
        dest_path: Local path where the file will be saved.
        if_exists: Action when dest_path already exists.

    Returns:
        Dict with 'path', 'bytes', and 'sha256' keys, or None if skipped.

    Raises:
        RetryExhaustedError: If all retry attempts fail.
        aiohttp.ClientResponseError: If server returns error status (4xx/5xx).
    """
    # Handle existing file
    if dest_path.exists():
        if if_exists == ExistingFileAction.SKIP:
            logger.info("Skipping download: file exists", extra={"path": str(dest_path)})
            return None
        if if_exists == ExistingFileAction.ERROR:
            raise FileShouldNotExist(dest_path)

        logger.info("Overwriting existing file", extra={"path": str(dest_path)})

    logger.info("Starting download", extra={"url": url, "will_save_to": dest_path.name})

    timeout = aiohttp.ClientTimeout(
        total=DOWNLOAD_TIMEOUT_TOTAL,
        connect=DOWNLOAD_TIMEOUT_CONNECT,
        sock_read=DOWNLOAD_TIMEOUT_SOCK_READ,
    )

    async with session.get(url, timeout=timeout) as response:
        response.raise_for_status()

        dest_path.parent.mkdir(parents=True, exist_ok=True)
        sha256_hash = hashlib.sha256()
        total_bytes = 0
        total_size = int(response.headers.get("content-length", 0))

        # Progress bar (disappears when complete cause leave=False)
        progress_bar = tqdm(
            total=total_size,
            unit="iB",
            unit_scale=True,
            unit_divisor=1024,
            desc=f"Downloading {dest_path.name}",
            leave=False,
        )

        try:
            async with aiofiles.open(dest_path, mode="wb") as f:
                async for chunk in response.content.iter_chunked(n=DOWNLOAD_CHUNK_SIZE):
                    await f.write(chunk)
                    sha256_hash.update(chunk)
                    chunk_len = len(chunk)
                    total_bytes += chunk_len
                    progress_bar.update(chunk_len)
        finally:
            progress_bar.close()

        sha256_result = sha256_hash.hexdigest()

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
        pass

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


def extract_7z_sync(
    archive_path: Path,
    target_filename: str,
    dest_path: Path,
    if_exists: ExistingFileAction = ExistingFileAction.SKIP,
    validate_sqlite: bool = True,
) -> None:
    """Extract a specific file from a 7z archive (synchronous).

    Searches for target_filename within the archive, extracts it to a
    temporary directory, then atomically moves it to dest_path.

    Args:
        archive_path: Path to the .7z archive.
        target_filename: Name or suffix of file to extract (handles nested paths).
        dest_path: Final destination path for extracted file.
        if_exists: Action when dest_path exists (SKIP or OVERWRITE).
        validate_sqlite: If True, validate SQLite header after extraction.

    Raises:
        ArchiveNotFoundError: If archive_path doesn't exist.
        FileNotFoundInArchiveError: If target_filename not found in archive.
        FileIntegrityError: If validation enabled and file is invalid.
    """
    if not archive_path.exists():
        raise ArchiveNotFoundError(archive_path)

    # Handle existing destination
    if dest_path.exists():
        if if_exists == ExistingFileAction.SKIP:
            logger.info("Skipping extraction: file exists", extra={"path": str(dest_path)})
            return None

        if if_exists == ExistingFileAction.ERROR:
            raise FileShouldNotExist(dest_path)

        logger.info("Overwriting existing file", extra={"path": str(dest_path)})

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
                file=TqdmToLoguru(logger.info) if _is_airflow_context() else sys.stderr,
                mininterval=5.0 if _is_airflow_context() else 1.0,
            ) as pbar:
                # Le callback reçoit le nombre d'octets écrits durant l'interval
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

            logger.info(
                "Extraction completed",
                extra={
                    "path": str(dest_path),
                    "size_mib": round(dest_path.stat().st_size / 1024**2, 2),
                },
            )


async def extract_7z_async(
    archive_path: Path,
    target_filename: str,
    dest_path: Path,
    if_exists: ExistingFileAction = ExistingFileAction.SKIP,
    validate_sqlite: bool = True,
) -> None:
    """Extract a file from 7z archive asynchronously.

    Wraps the synchronous extraction in a thread executor to avoid
    blocking the event loop during CPU-intensive decompression.

    Args:
        archive_path: Path to the .7z archive.
        target_filename: Name or suffix of file to extract.
        dest_path: Final destination path for extracted file.
        if_exists: Action when dest_path exists (SKIP or OVERWRITE).
        validate_sqlite: If True, validate SQLite header after extraction.

    Raises:
        ArchiveNotFoundError: If archive doesn't exist.
        FileNotFoundInArchiveError: If target not found in archive.
        FileIntegrityError: If validation fails.
    """
    loop = asyncio.get_running_loop()

    # ThreadPoolExecutor with max_workers=1 ensures thread safety
    # for file operations (unlink, move) during extraction
    with ThreadPoolExecutor(max_workers=1) as pool:
        await loop.run_in_executor(
            pool,
            extract_7z_sync,
            archive_path,
            target_filename,
            dest_path,
            if_exists,
            validate_sqlite,
        )


# =============================================================================
# CLI entry point for testing
# =============================================================================


async def _test_download() -> None:
    """Test download with IGN ADMIN-EXPRESS-COG dataset."""
    url = (
        "https://data.geopf.fr/telechargement/download/ADMIN-EXPRESS-COG/"
        "ADMIN-EXPRESS-COG_4-0__GPKG_WGS84G_FRA_2025-01-01/"
        "ADMIN-EXPRESS-COG_4-0__GPKG_WGS84G_FRA_2025-01-01.7z"
    )
    archive_path = DATA_DIR / "landing" / "ADMIN-EXPRESS-COG.7z"
    dest_path = DATA_DIR / "landing" / "admin_express_cog.gpkg"

    async with aiohttp.ClientSession() as session:
        try:
            await download_to_file(session, url, archive_path, ExistingFileAction.OVERWRITE)
        except (RetryExhaustedError, FileShouldNotExist) as e:
            logger.error("Download failed", extra={"error": str(e)})
            sys.exit(-1)

    try:
        await extract_7z_async(
            archive_path=archive_path,
            target_filename="ADE-COG_4-0_GPKG_WGS84G_FRA-ED2025-01-01.gpkg",
            dest_path=dest_path,
            if_exists=ExistingFileAction.OVERWRITE,
            validate_sqlite=True,
        )
    except (
        ArchiveNotFoundError,
        FileNotFoundInArchiveError,
        FileIntegrityError,
        FileShouldNotExist,
    ) as e:
        logger.error("Extraction failed", extra={"error": str(e)})
        sys.exit(-1)


if __name__ == "__main__":
    asyncio.run(_test_download())
