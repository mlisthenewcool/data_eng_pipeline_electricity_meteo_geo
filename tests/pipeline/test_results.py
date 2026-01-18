"""Tests for pipeline result types.

This module tests the typed result classes used across pipeline stages,
ensuring proper serialization/deserialization for Airflow XCom.
"""
#
# from pathlib import Path
#
# import pytest
#
# from de_projet_perso.pipeline.results import (
#     BronzeResult,
#     DownloadResult,
#     ExtractionResult,
#     LandingResult,
#     SilverResult,
# )
#
# # Test constants for file sizes and row counts
# TEST_SIZE_MIB = 100.5
# TEST_ROW_COUNT_800 = 800
# TEST_ROW_COUNT_1000 = 1000
#
#
# class TestDownloadResult:
#     """Tests for DownloadResult dataclass."""
#
#     def test_creation(self):
#         """Test creating a DownloadResult."""
#         result = DownloadResult(
#             path=Path("/data/landing/test.7z"),
#             sha256="abc123",
#             size_mib=TEST_SIZE_MIB,
#         )
#
#         assert result.path == Path("/data/landing/test.7z")
#         assert result.sha256 == "abc123"
#         assert result.size_mib == TEST_SIZE_MIB
#
#     def test_serialization(self):
#         """Test DownloadResult serialization to dict."""
#         result = DownloadResult(
#             path=Path("/data/landing/test.7z"),
#             sha256="abc123",
#             size_mib=TEST_SIZE_MIB,
#         )
#
#         serialized = result.to_serializable()
#
#         assert serialized == {
#             "path": "/data/landing/test.7z",
#             "sha256": "abc123",
#             "size_mib": TEST_SIZE_MIB,
#         }
#         # Ensure path is string, not Path object
#         assert isinstance(serialized["path"], str)
#         assert isinstance(serialized["sha256"], str)
#         assert isinstance(serialized["size_mib"], float)
#
#     def test_immutability(self):
#         """Test that DownloadResult is immutable (frozen dataclass)."""
#         result = DownloadResult(
#             path=Path("/data/landing/test.7z"),
#             sha256="abc123",
#             size_mib=100.5,
#         )
#
#         with pytest.raises(Exception):  # FrozenInstanceError or AttributeError
#             result.sha256 = "new_hash"  # type: ignore
#
#
# class TestExtractionResult:
#     """Tests for ExtractionResult dataclass."""
#
#     def test_creation_with_dual_sha256(self):
#         """Test ExtractionResult with different archive and extracted SHA256."""
#         result = ExtractionResult(
#             path=Path("/data/landing/extracted.gpkg"),
#             size_mib=50.0,
#             extracted_sha256="extracted_hash",
#             archive_sha256="archive_hash",
#         )
#
#         assert result.path == Path("/data/landing/extracted.gpkg")
#         assert result.extracted_sha256 == "extracted_hash"
#         assert result.archive_sha256 == "archive_hash"
#
#     def test_serialization_includes_both_hashes(self):
#         """Test that serialization includes both SHA256 values."""
#         result = ExtractionResult(
#             path=Path("/data/landing/file.gpkg"),
#             size_mib=50.0,
#             extracted_sha256="extracted_hash",
#             archive_sha256="archive_hash",
#         )
#
#         serialized = result.to_serializable()
#
#         assert "extracted_sha256" in serialized
#         assert "archive_sha256" in serialized
#         assert serialized["extracted_sha256"] == "extracted_hash"
#         assert serialized["archive_sha256"] == "archive_hash"
#
#     def test_non_archive_scenario(self):
#         """Test ExtractionResult when file is not an archive (both SHA256 same)."""
#         result = ExtractionResult(
#             path=Path("/data/landing/direct.gpkg"),
#             size_mib=75.0,
#             extracted_sha256="same_hash",
#             archive_sha256="same_hash",
#         )
#
#         assert result.extracted_sha256 == result.archive_sha256
#         assert result.extracted_sha256 == "same_hash"
#
#
# class TestLandingResult:
#     """Tests for LandingResult dataclass."""
#
#     def test_creation_with_traceability(self):
#         """Test LandingResult creation with archive traceability."""
#         result = LandingResult(
#             path=Path("/data/landing/file.gpkg"),
#             sha256="file_hash",
#             size_mib=50.0,
#             archive_sha256="archive_hash",
#         )
#
#         assert result.sha256 == "file_hash"
#         assert result.archive_sha256 == "archive_hash"
#         assert result.layer == "landing"
#
#     def test_default_layer_value(self):
#         """Test that layer defaults to 'landing'."""
#         result = LandingResult(
#             path=Path("/data/landing/file.gpkg"),
#             sha256="hash",
#             size_mib=50.0,
#             archive_sha256="archive_hash",
#         )
#
#         assert result.layer == "landing"
#
#     def test_serialization(self):
#         """Test LandingResult serialization."""
#         result = LandingResult(
#             path=Path("/data/landing/file.gpkg"),
#             sha256="file_hash",
#             size_mib=50.0,
#             archive_sha256="archive_hash",
#         )
#
#         serialized = result.to_serializable()
#
#         assert serialized["sha256"] == "file_hash"
#         assert serialized["archive_sha256"] == "archive_hash"
#         assert serialized["layer"] == "landing"
#
#
# class TestBronzeResult:
#     """Tests for BronzeResult dataclass."""
#
#     def test_creation(self):
#         """Test BronzeResult creation."""
#         result = BronzeResult(
#             path=Path("/data/bronze/dataset/v1.parquet"),
#             row_count=TEST_ROW_COUNT_1000,
#             columns=["id", "name", "value"],
#             sha256="landing_hash",
#             archive_sha256="archive_hash",
#         )
#
#         assert result.row_count == TEST_ROW_COUNT_1000
#         assert result.columns == ["id", "name", "value"]
#         assert result.layer == "bronze"
#
#     def test_sha256_propagation(self):
#         """Test that SHA256 values are propagated from landing."""
#         result = BronzeResult(
#             path=Path("/data/bronze/dataset/v1.parquet"),
#             row_count=500,
#             columns=["col1"],
#             sha256="propagated_landing_hash",
#             archive_sha256="propagated_archive_hash",
#         )
#
#         assert result.sha256 == "propagated_landing_hash"
#         assert result.archive_sha256 == "propagated_archive_hash"
#
#     def test_serialization(self):
#         """Test BronzeResult serialization."""
#         result = BronzeResult(
#             path=Path("/data/bronze/dataset/v1.parquet"),
#             row_count=1000,
#             columns=["id", "name"],
#             sha256="hash1",
#             archive_sha256="hash2",
#         )
#
#         serialized = result.to_serializable()
#
#         assert isinstance(serialized["path"], str)
#         assert isinstance(serialized["row_count"], int)
#         assert isinstance(serialized["columns"], list)
#         assert serialized["layer"] == "bronze"
#
#
# class TestSilverResult:
#     """Tests for SilverResult dataclass."""
#
#     def test_creation(self):
#         """Test SilverResult creation."""
#         result = SilverResult(
#             path=Path("/data/silver/dataset/v1.parquet"),
#             row_count=TEST_ROW_COUNT_800,
#             columns=["id", "processed_value"],
#             sha256="landing_hash",
#             archive_sha256="archive_hash",
#         )
#
#         assert result.row_count == TEST_ROW_COUNT_800
#         assert result.layer == "silver"
#
#     def test_full_traceability_chain(self):
#         """Test that silver result maintains full traceability to source."""
#         result = SilverResult(
#             path=Path("/data/silver/dataset/v1.parquet"),
#             row_count=500,
#             columns=["col1"],
#             sha256="landing_file_hash",
#             archive_sha256="original_archive_hash",
#         )
#
#         # Silver should maintain both hashes for complete traceability
#         assert result.sha256 == "landing_file_hash"
#         assert result.archive_sha256 == "original_archive_hash"
#
#     def test_serialization_completeness(self):
#         """Test SilverResult serialization includes all fields."""
#         result = SilverResult(
#             path=Path("/data/silver/dataset/v1.parquet"),
#             row_count=1200,
#             columns=["a", "b", "c"],
#             sha256="hash1",
#             archive_sha256="hash2",
#         )
#
#         serialized = result.to_serializable()
#
#         required_fields = ["path", "row_count", "columns", "sha256", "archive_sha256", "layer"]
#         for field in required_fields:
#             assert field in serialized
#
#         assert serialized["layer"] == "silver"
#
#
# class TestResultTypeCoherence:
#     """Tests for type coherence across result types."""
#
#     def test_all_results_have_to_serializable(self):
#         """Test that all result types implement to_serializable()."""
#         results = [
#             DownloadResult(Path("/tmp/file"), "hash", 10.0),
#             ExtractionResult(Path("/tmp/file"), 10.0, "hash1", "hash2"),
#             LandingResult(Path("/tmp/file"), "hash1", 10.0, "hash2"),
#             BronzeResult(Path("/tmp/file"), 100, ["col"], "hash1", "hash2"),
#             SilverResult(Path("/tmp/file"), 100, ["col"], "hash1", "hash2"),
#         ]
#
#         for result in results:
#             assert hasattr(result, "to_serializable")
#             serialized = result.to_serializable()
#             assert isinstance(serialized, dict)
#
#     def test_sha256_propagation_chain(self):
#         """Test SHA256 propagation through the entire pipeline."""
#         # Simulate pipeline flow
#         archive_sha256 = "original_archive_abc123"
#         extracted_sha256 = "extracted_file_def456"
#
#         # Extraction result
#         extraction = ExtractionResult(
#             path=Path("/data/landing/file.gpkg"),
#             size_mib=50.0,
#             extracted_sha256=extracted_sha256,
#             archive_sha256=archive_sha256,
#         )
#
#         # Landing result
#         landing = LandingResult(
#             path=extraction.path,
#             sha256=extraction.extracted_sha256,
#             size_mib=extraction.size_mib,
#             archive_sha256=extraction.archive_sha256,
#         )
#
#         # Bronze result
#         bronze = BronzeResult(
#             path=Path("/data/bronze/file.parquet"),
#             row_count=1000,
#             columns=["col1"],
#             sha256=landing.sha256,
#             archive_sha256=landing.archive_sha256,
#         )
#
#         # Silver result
#         silver = SilverResult(
#             path=Path("/data/silver/file.parquet"),
#             row_count=900,
#             columns=["col1"],
#             sha256=bronze.sha256,
#             archive_sha256=bronze.archive_sha256,
#         )
#
#         # Verify complete traceability
#         assert silver.sha256 == extracted_sha256
#         assert silver.archive_sha256 == archive_sha256
#         assert landing.archive_sha256 == archive_sha256
#         assert bronze.archive_sha256 == archive_sha256
