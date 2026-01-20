"""Remote file checker utilities.

This module provides utilities to check if a remote file has changed
without downloading it, using HTTP HEAD requests and metadata comparison.
"""

from dataclasses import dataclass
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Self

import httpx

from de_projet_perso.core.logger import logger


@dataclass(frozen=True)
class ChangeResult:
    """Verdict du moteur de décision de changement."""

    has_changed: bool
    reason: str

    def __bool__(self) -> bool:
        """Permet d'utiliser 'if result:' directement."""
        return self.has_changed


@dataclass
class RemoteFileInfo:
    """Metadata information about a remote file.

    Attributes:
        etag: ETag header value (strong cache validator)
        last_modified: Last-Modified header as datetime
        content_length: Content-Length in bytes
    """

    etag: str | None
    last_modified: datetime | None
    content_length: int | None

    def _has_any_field(self) -> bool:
        return any(v is not None for v in [self.etag, self.last_modified, self.content_length])

    def compare_with(self, other: Self) -> ChangeResult:  # noqa: PLR0911
        """Compare l'état actuel avec le précédent et renvoie un ChangeResult."""
        # 1. Cas d'incertitude (Fail-safe)
        if not self._has_any_field() or not other._has_any_field():
            return ChangeResult(True, "Missing metadata on current or previous state")

        # 2. Vérification ETag (Priorité absolue)
        if self.etag and other.etag:
            if self.etag != other.etag:
                return ChangeResult(True, f"ETag changed: {other.etag} -> {self.etag}")
            return ChangeResult(False, "ETag identical")

        # 3. Vérification Date (Plus récent)
        if self.last_modified and other.last_modified:
            if self.last_modified > other.last_modified:
                return ChangeResult(True, f"File is newer: {self.last_modified}")
            return ChangeResult(False, "File date is identical or older")

        # 4. Vérification Taille
        if self.content_length is not None and other.content_length is not None:
            if self.content_length != other.content_length:
                return ChangeResult(
                    True, f"Size changed: {other.content_length} -> {self.content_length}"
                )
            return ChangeResult(False, "Size identical")

        return ChangeResult(True, "No matching metadata found to confirm identity")


def get_remote_file_info(
    url: str,
    timeout: int = 30,
    allow_redirects: bool = True,
) -> RemoteFileInfo:
    """Fetch remote file metadata using HTTP HEAD request.

    This function performs a lightweight HEAD request to retrieve
    metadata without downloading the file content.

    Priority order for change detection:
    1. ETag (most reliable)
    2. Last-Modified
    3. Content-Length (least reliable, file can change with same size)

    Args:
        url: Remote file URL
        timeout: Request timeout in seconds
        allow_redirects: Follow HTTP redirects

    Returns:
        RemoteFileInfo with parsed metadata

    Raises:
        httpx.HTTPError: If request fails (network, 404, etc.)

    Example:
        >>> info = get_remote_file_info("https://example.com/data.csv")
        >>> print(info.etag)
        "abc123def456"
        >>> print(info.last_modified)
        datetime(2026, 1, 19, 14, 30, 0)
    """
    logger.info(f"Checking remote file metadata: {url}")

    try:
        with httpx.Client(
            timeout=timeout,
            follow_redirects=allow_redirects,
            http2=True,
        ) as client:
            response = client.head(url)
            response.raise_for_status()
    except httpx.HTTPStatusError as e:
        logger.error(
            f"HTTP error checking remote file: {e.response.status_code}",
            extra={"url": url, "status": e.response.status_code},
        )
        raise
    except httpx.TimeoutException:
        logger.error("Timeout checking remote file", extra={"url": url, "timeout": timeout})
        raise
    except httpx.HTTPError as e:
        logger.error(
            "Network error checking remote file",
            extra={"url": url, "error": str(e)},
        )
        raise

    headers = response.headers

    # Parse ETag (remove quotes if present)
    etag = headers.get("etag")
    if etag:
        etag = etag.strip('"')

    # Parse Last-Modified (RFC 2822 format)
    last_modified = None
    if "last-modified" in headers:
        try:
            last_modified = parsedate_to_datetime(headers["last-modified"])
        except (TypeError, ValueError) as e:
            logger.warning(
                "Failed to parse Last-Modified header",
                extra={"header": headers["last-modified"], "error": str(e)},
            )

    # Parse Content-Length
    content_length = None
    if "content-length" in headers:
        try:
            content_length = int(headers["content-length"])
        except ValueError as e:
            logger.warning(
                "Failed to parse Content-Length header",
                extra={"header": headers["content-length"], "error": str(e)},
            )

    info = RemoteFileInfo(etag=etag, last_modified=last_modified, content_length=content_length)

    logger.debug(
        "Remote file metadata retrieved",
        extra={
            "etag": etag,
            "last_modified": last_modified.isoformat() if last_modified else None,
            "content_length": content_length,
        },
    )

    return info


def has_remote_file_changed(current: RemoteFileInfo, previous: RemoteFileInfo) -> ChangeResult:
    """Compare remote file metadata to detect changes.

    Uses a priority-based comparison:
    1. ETag (if both available) - most reliable
    2. Last-Modified (if both available)
    3. Content-Length (if both available) - least reliable
    4. Default to True if no metadata available

    Args:
        current: Current remote file info
        previous: Previous metadata from state (dict format)
            Expected keys: "etag", "last_modified", "content_length"

    Returns:
        Tuple of (changed: bool, reason: str)

    Example:
        # TODO: changer la sortie
        >>> current = RemoteFileInfo(url="...", etag="new", ...)
        >>> previous = RemoteFileInfo(url= "...", etag="old", last_modified=None)
        >>> changed, reason = has_remote_file_changed(current, previous)
        >>> print(changed, reason)
        True "ETag changed: old → new"
    """
    # TODO: ajouter méthode If-None-Match
    # headers = {}
    # if previous_etag:
    #     headers["If-None-Match"] = f'"{previous_etag}"'  # Format standard avec quotes
    # if previous_modified:
    #     headers["If-Modified-Since"] = previous_modified.strftime("%a, %d %b %Y %H:%M:%S GMT")
    #
    # try:
    #     with httpx.Client(timeout=timeout, follow_redirects=True) as client:
    #         # On tente un GET léger ou un HEAD
    #         response = client.head(url, headers=headers)
    #
    #         # CAS 304 : Le serveur confirme que rien n'a changé
    #         if response.status_code == 304:
    #             logger.info("Server returned 304: File unchanged", extra={"url": url})
    #             return RemoteFileInfo(url=url, etag=previous_etag, available=True), False
    #
    #         response.raise_for_status()
    #
    # except httpx.HTTPError:
    #     raise

    return current.compare_with(previous)
