"""Tests for remote file checker utilities."""
#
# from datetime import datetime
#
# from de_projet_perso.utils.remote_checker import (
#     RemoteFileInfo,
#     has_remote_file_changed,
# )
#
#
# class TestHasRemoteFileChanged:
#     """Tests for has_remote_file_changed function."""
#
#     def test_changed_by_etag(self) -> None:
#         """Test detection via ETag change (priority 1)."""
#         current = RemoteFileInfo(url="test", etag="new_etag")
#         previous = {"etag": "old_etag", "last_modified": None, "content_length": None}
#
#         changed, reason = has_remote_file_changed(current, previous)
#
#         assert changed is True
#         assert "ETag changed" in reason
#         assert "old_etag" in reason
#         assert "new_etag" in reason
#
#     def test_unchanged_by_etag(self) -> None:
#         """Test ETag unchanged (skip download)."""
#         current = RemoteFileInfo(url="test", etag="same_etag")
#         previous = {"etag": "same_etag", "last_modified": None, "content_length": None}
#
#         changed, reason = has_remote_file_changed(current, previous)
#
#         assert changed is False
#         assert "ETag unchanged" in reason
#
#     def test_changed_by_last_modified(self) -> None:
#         """Test detection via Last-Modified (priority 2, no ETag)."""
#         current = RemoteFileInfo(url="test", last_modified=datetime(2026, 1, 20, 12, 0, 0))
#         previous = {
#             "etag": None,
#             "last_modified": "2026-01-19T12:00:00",
#             "content_length": None,
#         }
#
#         changed, reason = has_remote_file_changed(current, previous)
#
#         assert changed is True
#         assert "Last-Modified newer" in reason
#
#     def test_unchanged_by_last_modified(self) -> None:
#         """Test Last-Modified not newer (skip download)."""
#         current = RemoteFileInfo(url="test", last_modified=datetime(2026, 1, 19, 12, 0, 0))
#         previous = {
#             "etag": None,
#             "last_modified": "2026-01-20T12:00:00",  # Previous is newer!
#             "content_length": None,
#         }
#
#         changed, reason = has_remote_file_changed(current, previous)
#
#         assert changed is False
#         assert "Last-Modified not newer" in reason
#
#     def test_changed_by_content_length(self) -> None:
#         """Test detection via Content-Length (priority 3, no ETag/Last-Modified)."""
#         current = RemoteFileInfo(url="test", content_length=2048)
#         previous = {"etag": None, "last_modified": None, "content_length": 1024}
#
#         changed, reason = has_remote_file_changed(current, previous)
#
#         assert changed is True
#         assert "Content-Length changed" in reason
#
#     def test_unchanged_by_content_length(self) -> None:
#         """Test Content-Length unchanged (skip download)."""
#         current = RemoteFileInfo(url="test", content_length=1024)
#         previous = {"etag": None, "last_modified": None, "content_length": 1024}
#
#         changed, reason = has_remote_file_changed(current, previous)
#
#         assert changed is False
#         assert "Content-Length unchanged" in reason
#
#     def test_no_metadata_assumes_changed(self) -> None:
#         """Test fail-safe: no metadata → assume changed (download)."""
#         current = RemoteFileInfo(url="test")
#         previous = {}
#
#         changed, reason = has_remote_file_changed(current, previous)
#
#         assert changed is True
#         assert "No metadata" in reason
#
#     def test_etag_takes_priority_over_last_modified(self) -> None:
#         """Test that ETag is checked first (even if Last-Modified also present)."""
#         # ETag says unchanged, but Last-Modified says changed
#         current = RemoteFileInfo(
#             url="test",
#             etag="same",
#             last_modified=datetime(2026, 1, 20),
#         )
#         previous = {
#             "etag": "same",
#             "last_modified": "2026-01-19T00:00:00",  # Older
#             "content_length": None,
#         }
#
#         changed, reason = has_remote_file_changed(current, previous)
#
#         # Should be unchanged because ETag has priority
#         assert changed is False
#         assert "ETag unchanged" in reason
