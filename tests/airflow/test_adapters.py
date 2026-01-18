"""Tests for Airflow XCom adapters.

This module tests the bidirectional conversion between typed pipeline
results and Airflow XCom-compatible dictionaries.
"""
#
# import json
# from pathlib import Path
#
# from de_projet_perso.airflow.adapters import AirflowTaskAdapter
# from de_projet_perso.pipeline.results import (
#     BronzeResult,
#     DownloadResult,
#     ExtractionResult,
#     LandingResult,
#     SilverResult,
# )
#
# # Test constants for file sizes and row counts
# TEST_SIZE_MIB = 150.5
# TEST_ROW_COUNT_500 = 500
# TEST_ROW_COUNT_1000 = 1000
#
#
# class TestAirflowTaskAdapter:
#     """Tests for AirflowTaskAdapter conversion utilities."""
#
#     def test_to_xcom_download_result(self):
#         """Test serialization of DownloadResult to XCom dict."""
#         result = DownloadResult(
#             path=Path("/data/landing/file.7z"),
#             sha256="abc123",
#             size_mib=TEST_SIZE_MIB,
#         )
#
#         xcom_dict = AirflowTaskAdapter.to_xcom(result)
#
#         assert isinstance(xcom_dict, dict)
#         assert xcom_dict["path"] == "/data/landing/file.7z"
#         assert xcom_dict["sha256"] == "abc123"
#         assert xcom_dict["size_mib"] == TEST_SIZE_MIB
#         # Ensure Path was converted to string
#         assert isinstance(xcom_dict["path"], str)
#
#     def test_from_xcom_download_roundtrip(self):
#         """Test deserialization of DownloadResult from XCom dict."""
#         original = DownloadResult(
#             path=Path("/data/landing/file.7z"),
#             sha256="abc123",
#             size_mib=TEST_SIZE_MIB,
#         )
#
#         # Serialize to XCom
#         xcom_dict = AirflowTaskAdapter.to_xcom(original)
#
#         # Deserialize back
#         restored = AirflowTaskAdapter.from_xcom_download(xcom_dict)
#
#         assert restored.path == original.path
#         assert restored.sha256 == original.sha256
#         assert restored.size_mib == original.size_mib
#         assert isinstance(restored.path, Path)  # Path was reconstructed
#
#     def test_from_xcom_extraction_roundtrip(self):
#         """Test deserialization of ExtractionResult from XCom dict."""
#         original = ExtractionResult(
#             path=Path("/data/landing/file.gpkg"),
#             size_mib=75.0,
#             extracted_sha256="extracted_hash",
#             archive_sha256="archive_hash",
#         )
#
#         xcom_dict = AirflowTaskAdapter.to_xcom(original)
#         restored = AirflowTaskAdapter.from_xcom_extraction(xcom_dict)
#
#         assert restored.path == original.path
#         assert restored.extracted_sha256 == original.extracted_sha256
#         assert restored.archive_sha256 == original.archive_sha256
#         assert isinstance(restored.path, Path)
#
#     def test_from_xcom_landing_roundtrip(self):
#         """Test deserialization of LandingResult from XCom dict."""
#         original = LandingResult(
#             path=Path("/data/landing/file.gpkg"),
#             sha256="file_hash",
#             size_mib=80.0,
#             archive_sha256="archive_hash",
#         )
#
#         xcom_dict = AirflowTaskAdapter.to_xcom(original)
#         restored = AirflowTaskAdapter.from_xcom_landing(xcom_dict)
#
#         assert restored.path == original.path
#         assert restored.sha256 == original.sha256
#         assert restored.archive_sha256 == original.archive_sha256
#         assert restored.layer == "landing"
#
#     def test_from_xcom_bronze_roundtrip(self):
#         """Test deserialization of BronzeResult from XCom dict."""
#         original = BronzeResult(
#             path=Path("/data/bronze/dataset/v1.parquet"),
#             row_count=1000,
#             columns=["id", "name", "value"],
#             sha256="landing_hash",
#             archive_sha256="archive_hash",
#         )
#
#         xcom_dict = AirflowTaskAdapter.to_xcom(original)
#         restored = AirflowTaskAdapter.from_xcom_bronze(xcom_dict)
#
#         assert restored.path == original.path
#         assert restored.row_count == original.row_count
#         assert restored.columns == original.columns
#         assert restored.sha256 == original.sha256
#         assert restored.archive_sha256 == original.archive_sha256
#         assert isinstance(restored.path, Path)
#
#     def test_from_xcom_silver_roundtrip(self):
#         """Test deserialization of SilverResult from XCom dict."""
#         original = SilverResult(
#             path=Path("/data/silver/dataset/v1.parquet"),
#             row_count=900,
#             columns=["id", "processed_value"],
#             sha256="landing_hash",
#             archive_sha256="archive_hash",
#         )
#
#         xcom_dict = AirflowTaskAdapter.to_xcom(original)
#         restored = AirflowTaskAdapter.from_xcom_silver(xcom_dict)
#
#         assert restored.path == original.path
#         assert restored.row_count == original.row_count
#         assert restored.columns == original.columns
#         assert restored.sha256 == original.sha256
#         assert restored.archive_sha256 == original.archive_sha256
#
#     def test_xcom_dict_is_json_serializable(self):
#         """Test that XCom dicts contain only JSON-serializable types."""
#         result = BronzeResult(
#             path=Path("/data/bronze/file.parquet"),
#             row_count=TEST_ROW_COUNT_500,
#             columns=["a", "b", "c"],
#             sha256="hash1",
#             archive_sha256="hash2",
#         )
#
#         xcom_dict = AirflowTaskAdapter.to_xcom(result)
#
#         # Should not raise JSONDecodeError
#         json_str = json.dumps(xcom_dict)
#         assert isinstance(json_str, str)
#
#         # Verify round-trip through JSON
#         decoded = json.loads(json_str)
#         assert decoded["path"] == "/data/bronze/file.parquet"
#         assert decoded["row_count"] == TEST_ROW_COUNT_500
#
#     def test_path_reconstruction_handles_nested_paths(self):
#         """Test that Path reconstruction works with nested directory structures."""
#         original = DownloadResult(
#             path=Path("/data/landing/datasets/2025/file.7z"),
#             sha256="hash",
#             size_mib=100.0,
#         )
#
#         xcom_dict = AirflowTaskAdapter.to_xcom(original)
#         restored = AirflowTaskAdapter.from_xcom_download(xcom_dict)
#
#         assert restored.path == Path("/data/landing/datasets/2025/file.7z")
#         assert restored.path.parts == (
#             "/",
#             "data",
#             "landing",
#             "datasets",
#             "2025",
#             "file.7z",
#         )
#
#     def test_complete_pipeline_flow_simulation(self):
#         """Test adapter usage in a simulated complete pipeline flow."""
#         # Step 1: Download
#         download_result = DownloadResult(
#             path=Path("/data/landing/archive.7z"),
#             sha256="archive_hash",
#             size_mib=200.0,
#         )
#         download_xcom = AirflowTaskAdapter.to_xcom(download_result)
#
#         # Step 2: Extract (receives download XCom)
#         download_restored = AirflowTaskAdapter.from_xcom_download(download_xcom)
#         extraction_result = ExtractionResult(
#             path=Path("/data/landing/file.gpkg"),
#             size_mib=150.0,
#             extracted_sha256="file_hash",
#             archive_sha256=download_restored.sha256,
#         )
#         extraction_xcom = AirflowTaskAdapter.to_xcom(extraction_result)
#
#         # Step 3: Landing (receives extraction XCom)
#         extraction_restored = AirflowTaskAdapter.from_xcom_extraction(extraction_xcom)
#         landing_result = LandingResult(
#             path=extraction_restored.path,
#             sha256=extraction_restored.extracted_sha256,
#             size_mib=extraction_restored.size_mib,
#             archive_sha256=extraction_restored.archive_sha256,
#         )
#         landing_xcom = AirflowTaskAdapter.to_xcom(landing_result)
#
#         # Step 4: Bronze (receives landing XCom)
#         landing_restored = AirflowTaskAdapter.from_xcom_landing(landing_xcom)
#         bronze_result = BronzeResult(
#             path=Path("/data/bronze/file.parquet"),
#             row_count=1000,
#             columns=["col1", "col2"],
#             sha256=landing_restored.sha256,
#             archive_sha256=landing_restored.archive_sha256,
#         )
#         bronze_xcom = AirflowTaskAdapter.to_xcom(bronze_result)
#
#         # Step 5: Silver (receives bronze XCom)
#         bronze_restored = AirflowTaskAdapter.from_xcom_bronze(bronze_xcom)
#         silver_result = SilverResult(
#             path=Path("/data/silver/file.parquet"),
#             row_count=950,
#             columns=["col1", "col2"],
#             sha256=bronze_restored.sha256,
#             archive_sha256=bronze_restored.archive_sha256,
#         )
#
#         # Verify complete traceability through the pipeline
#         assert silver_result.sha256 == "file_hash"
#         assert silver_result.archive_sha256 == "archive_hash"
#
#     def test_type_coercion_in_deserialization(self):
#         """Test that deserialization properly coerces types."""
#         # Simulate XCom dict with string numbers (as might come from JSON)
#         xcom_dict = {
#             "path": "/data/bronze/file.parquet",
#             "row_count": str(TEST_ROW_COUNT_1000),  # String instead of int
#             "columns": ["a", "b"],
#             "sha256": "hash1",
#             "archive_sha256": "hash2",
#             "layer": "bronze",
#         }
#
#         # Adapter should handle type coercion
#         result = AirflowTaskAdapter.from_xcom_bronze(xcom_dict)
#
#         assert isinstance(result.row_count, int)
#         assert result.row_count == TEST_ROW_COUNT_1000
