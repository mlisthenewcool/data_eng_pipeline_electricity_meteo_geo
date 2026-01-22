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
"""

import re
import sys
from pathlib import Path
from urllib.parse import unquote, urlparse

import httpx
from tqdm import tqdm

from de_projet_perso.core.logger import TqdmToLoguru, logger
from de_projet_perso.core.settings import settings
from de_projet_perso.pipeline.results import DownloadResult
from de_projet_perso.utils.hasher import FileHasher


def extract_filename_from_response(response: httpx.Response, url: str) -> str | None:
    """Extract original filename from HTTP response or URL.

    Priority order:
    1. Content-Disposition header (most reliable - server specifies filename)
    2. URL path basename (fallback - extract from URL path)

    Args:
        response: HTTP response object from httpx
        url: Original request URL

    Returns:
        Extracted filename (sanitized for filesystem use) if found, None otherwise.

    Example:
        >>> response.headers = {"Content-Disposition": "attachment; filename=data.parquet"}
        ... extract_filename_from_response(response, url)
        'data.parquet'

        >>> response.headers = {}
        ... extract_filename_from_response(response, "https://example.com/export/file.csv")
        'file.csv'
    """
    # Try Content-Disposition header first
    content_disp = response.headers.get("content-disposition", "")
    if content_disp:
        # Parse Content-Disposition header (handles various formats)
        # Examples: "attachment; filename=data.csv"
        #           "attachment; filename*=UTF-8''data%20file.csv"

        # Try standard filename parameter
        regex = r'filename=["\']?([^"\';\n]+)["\']?'
        # regex_simple = r'filename="?([^";\n]+)"?'
        match = re.search(regex, content_disp)
        if match:
            filename = match.group(1).strip()
            # Remove any path separators for security
            filename = Path(filename).name
            if filename and filename != ".":
                logger.debug(
                    "Extracted filename from Content-Disposition",
                    extra={"filename": filename, "header": content_disp},
                )
                return filename

    # Fallback: extract from URL path
    url_path = urlparse(url).path
    if url_path:
        # decode URL encoding, unquote handles %20 and other special chars
        filename = Path(unquote(url_path)).name

        # check if it's an actual file and not a folder
        if filename and filename != "." and "." in filename:
            logger.debug(
                "Extracted filename from URL path", extra={"filename": filename, "url": url}
            )
            return filename

    logger.warning("Could not extract filename", extra={"url": url})
    return None


def download_to_file(url: str, dest_dir: Path, default_name: str) -> DownloadResult:
    """Download a file from URL with streaming, progress bar, and SHA256.

    Performs memory-efficient download by streaming chunks to disk.
    Automatically creates parent directories if needed.
    Uses HTTP/2 when available for better performance.

    The filename is extracted from the Content-Disposition header or URL path,
    preserving the original server-provided filename.

    Args:
        url: URL of the file to download.
        dest_dir: Directory where the file will be saved (filename extracted from response).
        default_name: Default name to fallback if any filename could be extracted from server.

    Returns:
        DownloadResult with path, sha256, size_mib, and original_filename.

    Raises:
        httpx.HTTPStatusError: If server returns error status (4xx/5xx).
        httpx.TimeoutException: If request times out.

    Example:
        >>> result = download_to_file(
        ...    "https://example.com/data.7z",
        ...    Path("/data/landing/ign_contours_iris")
        ...)
        ... # File saved to: /data/landing/ign_contours_iris/CONTOURS-IRIS_3-0...7z
        ... print(f"Downloaded {result.original_filename}: {result.size_mib} MiB")
    """
    logger.debug("Starting download", extra={"url": url, "dest_dir": dest_dir})

    # TODO, documenter & ajouter arguments write/pool
    timeout = httpx.Timeout(
        timeout=settings.download_timeout_total,
        connect=settings.download_timeout_connect,
        read=settings.download_timeout_sock_read,
        write=None,
        pool=None,
    )

    with httpx.Client(http2=True, timeout=timeout, follow_redirects=True) as client:
        with client.stream("GET", url) as response:
            response.raise_for_status()

            # Extract original filename from response headers or URL
            original_filename = extract_filename_from_response(response, url)
            if original_filename is None:
                logger.warning(
                    f"Could not extract original filename, fallback to default: {default_name}"
                )
                original_filename = default_name
            dest_path = dest_dir / original_filename

            if dest_path.exists():
                logger.warning("File already exists", extra={"url": url, "path": dest_path})

            dest_path.parent.mkdir(parents=True, exist_ok=True)
            total_bytes = 0
            total_size = int(response.headers.get("content-length", 0))

            hasher = FileHasher()

            progress_bar = tqdm(
                total=total_size,
                unit="iB",
                unit_scale=True,
                unit_divisor=1024,
                desc=f"Downloading {original_filename} (total_size={total_size})",
                file=TqdmToLoguru(logger.info) if settings.is_running_on_airflow else sys.stderr,
                leave=False,  # disappears when complete
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

            logger.debug(
                "Download completed",
                extra={
                    "path": dest_path,
                    "original_filename": original_filename,
                    "size_mib": round(total_bytes / (1024**2), 2),
                    "sha256": sha256_result,
                },
            )

            return DownloadResult(path=dest_path, sha256=sha256_result, size_mib=total_bytes)
