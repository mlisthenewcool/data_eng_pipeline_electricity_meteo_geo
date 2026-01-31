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
class ChangeDetectionResult:
    """Verdict of the change detection engine.

    Attributes:
        has_changed: Boolean indicating if a change was detected
        reason: Human-readable explanation of the decision
    """

    has_changed: bool
    reason: str

    def __bool__(self) -> bool:
        """Allow direct boolean evaluation of the result."""
        return self.has_changed


@dataclass(frozen=True)
class RemoteFileMetadata:
    """Metadata information about a remote file.

    Attributes:
        etag: ETag header value (strong cache validator)
        last_modified: Last-Modified header as datetime
        content_length: Content-Length in bytes
    """

    etag: str | None = None
    last_modified: datetime | None = None
    content_length: int | None = None

    def _has_any_field(self) -> bool:
        """Check if at least one metadata field is populated."""
        return any(v is not None for v in [self.etag, self.last_modified, self.content_length])

    def compare_with(self, other: Self) -> ChangeDetectionResult:  # noqa: PLR0911
        """Compare current metadata with a previous state to detect changes.

        Follows a strict priority order: ETag > Last-Modified > Content-Length.
        If no metadata is available for comparison, it fails safe by reporting a change.

        Args:
            other: The previous RemoteFileMetadata to compare against.

        Returns:
            A ChangeDetectionResult containing the boolean verdict and the rationale.
        """
        # 1. Uncertainty Case (Fail-safe)
        if not self._has_any_field() or not other._has_any_field():
            return ChangeDetectionResult(True, "Missing metadata on current or previous state")

        # 2. ETag Check (Highest priority)
        if self.etag and other.etag:
            if self.etag != other.etag:
                return ChangeDetectionResult(True, f"ETag changed: {other.etag} -> {self.etag}")
            return ChangeDetectionResult(False, "ETag identical")

        # 3. Date Check
        if self.last_modified and other.last_modified:
            if self.last_modified > other.last_modified:
                return ChangeDetectionResult(True, f"File is newer: {self.last_modified}")
            return ChangeDetectionResult(False, "File date is identical or older")

        # 4. Size Check
        if self.content_length is not None and other.content_length is not None:
            if self.content_length != other.content_length:
                return ChangeDetectionResult(
                    True, f"Size changed: {other.content_length} -> {self.content_length}"
                )
            return ChangeDetectionResult(False, "Size identical")

        return ChangeDetectionResult(True, "No matching metadata found to confirm identity")


def get_remote_file_info(
    url: str,
    timeout: int = 30,
    allow_redirects: bool = True,
) -> RemoteFileMetadata:
    """Fetch remote file metadata using HTTP HEAD request.

    Performs a lightweight request to retrieve headers without downloading
    the body. Parses ETag, Last-Modified, and Content-Length.

    Args:
        url: Remote file URL.
        timeout: Request timeout in seconds.
        allow_redirects: Whether to follow HTTP redirects.

    Returns:
        RemoteFileMetadata with parsed metadata

    Raises:
        httpx.HTTPStatusError: If the server returns an error status (e.g., 404).
        httpx.TimeoutException: If the request times out.
        httpx.HTTPError: For other network-related issues.
    """
    logger.info("Checking remote file metadata", extra={"url": url})

    try:
        with httpx.Client(timeout=timeout, follow_redirects=allow_redirects, http2=True) as client:
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
    etag = headers.get("etag", "").strip('"') or None

    # Parse Last-Modified (RFC 2822 format)
    last_modified = None
    if "last-modified" in headers:
        try:
            last_modified = parsedate_to_datetime(headers["last-modified"])
        except (TypeError, ValueError) as e:
            logger.warning(
                "Invalid Last-Modified header",
                extra={"header": headers["last-modified"], "error": str(e)},
            )

    # Parse Content-Length
    content_length = None
    if "content-length" in headers:
        try:
            content_length = int(headers["content-length"])
        except ValueError as e:
            logger.warning(
                "Invalid Content-Length header",
                extra={"header": headers["content-length"], "error": str(e)},
            )

    info = RemoteFileMetadata(etag=etag, last_modified=last_modified, content_length=content_length)

    logger.info(
        "Remote file metadata retrieved",
        extra={
            "etag": etag,
            "last_modified": last_modified.isoformat() if last_modified else None,
            "content_length": content_length,
        },
    )

    return info


def has_remote_file_changed(
    current: RemoteFileMetadata, previous: RemoteFileMetadata
) -> ChangeDetectionResult:
    """Compare two sets of RemoteFileMetadata to detect changes.

    Wraps the logic of RemoteFileMetadata.compare_with for functional usage.

    Args:
        current: Newly fetched metadata.
        previous: Metadata stored from a previous execution.

    Returns:
        A ChangeDetectionResult object indicating if an update is required.
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
    #             return RemoteFileMetadata(url=url, etag=previous_etag, available=True), False
    #
    #         response.raise_for_status()
    #
    # except httpx.HTTPError:
    #     raise

    return current.compare_with(previous)
