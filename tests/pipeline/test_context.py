"""Tests for PipelineContext serialization and manipulation."""
#
# from datetime import datetime
# from pathlib import Path
#
# import pytest
#
# from de_projet_perso.pipeline.context import PipelineContext
# from de_projet_perso.pipeline.results import (
#     BronzeResult,
#     DownloadResult,
#     ExtractionResult,
#     SilverResult,
# )
# from de_projet_perso.pipeline.state import PipelineAction
#
#
# class TestPipelineContextSerialization:
#     """Test serialization/deserialization of PipelineContext."""
#
#     def test_serialize_empty_context(self) -> None:
#         # Context with no results should serialize to dict with None values
#         ctx = PipelineContext(
#             dataset_name="test_dataset",
#             run_id="test_run_123",
#             action=PipelineAction.FIRST_RUN,
#             start_time=datetime(2026, 1, 19, 14, 30, 0),
#         )
#
#         result = ctx.to_dict()
#
#         assert result["dataset_name"] == "test_dataset"
#         assert result["run_id"] == "test_run_123"
#         assert result["action"] == "first_run"
#         assert result["start_time"] == "2026-01-19T14:30:00"
#         assert result["download"] is None
#         assert result["extraction"] is None
#         assert result["bronze"] is None
#         assert result["silver"] is None
#
#     def test_deserialize_empty_context(self) -> None:
#         # Dict with None results should deserialize to context with None attributes
#         data = {
#             "dataset_name": "test_dataset",
#             "run_id": "test_run_123",
#             "action": "first_run",
#             "start_time": "2026-01-19T14:30:00",
#             "download": None,
#             "extraction": None,
#             "bronze": None,
#             "silver": None,
#         }
#
#         ctx = PipelineContext.from_dict(data)
#
#         assert ctx.dataset_name == "test_dataset"
#         assert ctx.run_id == "test_run_123"
#         assert ctx.action == PipelineAction.FIRST_RUN
#         assert ctx.start_time == datetime(2026, 1, 19, 14, 30, 0)
#         assert ctx.download is None
#         assert ctx.extraction is None
#         assert ctx.bronze is None
#         assert ctx.silver is None
#
#     def test_serialize_context_with_download_result(self) -> None:
#         # Context with download result should serialize Path to string
#         download = DownloadResult(
#             path=Path("/data/landing/test.7z"),
#             sha256="abc123",
#             size_mib=100.5,
#             original_filename="test.7z",
#         )
#
#         ctx = PipelineContext(
#             dataset_name="test",
#             run_id="run_1",
#             action=PipelineAction.FIRST_RUN,
#             start_time=datetime(2026, 1, 19, 14, 30, 0),
#             download=download,
#         )
#
#         result = ctx.to_dict()
#
#         assert result["download"]["path"] == "/data/landing/test.7z"
#         assert result["download"]["sha256"] == "abc123"
#         assert result["download"]["size_mib"] == 100.5
#         assert result["download"]["original_filename"] == "test.7z"
#
#     def test_deserialize_context_with_download_result(self) -> None:
#         # Dict should deserialize string path back to Path object
#         data = {
#             "dataset_name": "test",
#             "run_id": "run_1",
#             "action": "first_run",
#             "start_time": "2026-01-19T14:30:00",
#             "download": {
#                 "path": "/data/landing/test.7z",
#                 "sha256": "abc123",
#                 "size_mib": 100.5,
#                 "original_filename": "test.7z",
#             },
#             "extraction": None,
#             "bronze": None,
#             "silver": None,
#         }
#
#         ctx = PipelineContext.from_dict(data)
#
#         assert ctx.download is not None
#         assert ctx.download.path == Path("/data/landing/test.7z")
#         assert ctx.download.sha256 == "abc123"
#         assert ctx.download.size_mib == 100.5
#         assert ctx.download.original_filename == "test.7z"
#
#     def test_serialize_context_with_all_results(self) -> None:
#         # Full context (download + extraction + bronze + silver) should serialize completely
#         ctx = PipelineContext(
#             dataset_name="iris",
#             run_id="iris_20260119_143000",
#             action=PipelineAction.REFRESH,
#             start_time=datetime(2026, 1, 19, 14, 30, 0),
#             download=DownloadResult(
#                 path=Path("/data/landing/archive.7z"),
#                 sha256="download_hash",
#                 size_mib=50.0,
#                 original_filename="archive.7z",
#             ),
#             extraction=ExtractionResult(
#                 path=Path("/data/landing/iris/data.gpkg"),
#                 size_mib=45.0,
#                 extracted_sha256="extracted_hash",
#                 archive_sha256="download_hash",
#                 original_filename="data.gpkg",
#             ),
#             bronze=BronzeResult(
#                 path=Path("/data/bronze/iris/2025-01.parquet"),
#                 row_count=150,
#                 columns=["col1", "col2", "col3"],
#                 sha256="extracted_hash",
#                 archive_sha256="download_hash",
#             ),
#             silver=SilverResult(
#                 path=Path("/data/silver/iris/2025-01.parquet"),
#                 row_count=150,
#                 columns=["col1", "col2", "col3"],
#                 sha256="extracted_hash",
#                 archive_sha256="download_hash",
#             ),
#         )
#
#         result = ctx.to_dict()
#
#         # Check all results are serialized
#         assert result["download"] is not None
#         assert result["extraction"] is not None
#         assert result["bronze"] is not None
#         assert result["silver"] is not None
#
#         # Verify paths are strings
#         assert isinstance(result["download"]["path"], str)
#         assert isinstance(result["extraction"]["path"], str)
#         assert isinstance(result["bronze"]["path"], str)
#         assert isinstance(result["silver"]["path"], str)
#
#     def test_deserialize_context_with_all_results(self) -> None:
#         # Full serialized dict should reconstruct all typed results correctly
#         data = {
#             "dataset_name": "iris",
#             "run_id": "iris_20260119_143000",
#             "action": "refresh",
#             "start_time": "2026-01-19T14:30:00",
#             "download": {
#                 "path": "/data/landing/archive.7z",
#                 "sha256": "download_hash",
#                 "size_mib": 50.0,
#                 "original_filename": "archive.7z",
#             },
#             "extraction": {
#                 "path": "/data/landing/iris/data.gpkg",
#                 "size_mib": 45.0,
#                 "extracted_sha256": "extracted_hash",
#                 "archive_sha256": "download_hash",
#                 "original_filename": "data.gpkg",
#             },
#             "bronze": {
#                 "path": "/data/bronze/iris/2025-01.parquet",
#                 "row_count": 150,
#                 "columns": ["col1", "col2", "col3"],
#                 "sha256": "extracted_hash",
#                 "archive_sha256": "download_hash",
#             },
#             "silver": {
#                 "path": "/data/silver/iris/2025-01.parquet",
#                 "row_count": 150,
#                 "columns": ["col1", "col2", "col3"],
#                 "sha256": "extracted_hash",
#                 "archive_sha256": "download_hash",
#             },
#         }
#
#         ctx = PipelineContext.from_dict(data)
#
#         # Verify all results reconstructed
#         assert ctx.download is not None
#         assert ctx.extraction is not None
#         assert ctx.bronze is not None
#         assert ctx.silver is not None
#
#         # Verify paths are Path objects
#         assert isinstance(ctx.download.path, Path)
#         assert isinstance(ctx.extraction.path, Path)
#         assert isinstance(ctx.bronze.path, Path)
#         assert isinstance(ctx.silver.path, Path)
#
#         # Verify data integrity
#         assert ctx.bronze.row_count == 150
#         assert ctx.silver.columns == ["col1", "col2", "col3"]
#
#     def test_roundtrip_serialization_preserves_data(self) -> None:
#         # to_dict() → from_dict() should preserve all data (idempotent)
#         original = PipelineContext(
#             dataset_name="test",
#             run_id="run_123",
#             action=PipelineAction.HEAL,
#             start_time=datetime(2026, 1, 19, 14, 30, 45),
#             download=DownloadResult(
#                 path=Path("/tmp/file.gpkg"),
#                 sha256="hash123",
#                 size_mib=25.5,
#                 original_filename="file.gpkg",
#             ),
#         )
#
#         # Roundtrip
#         serialized = original.to_dict()
#         restored = PipelineContext.from_dict(serialized)
#
#         # Verify all fields preserved
#         assert restored.dataset_name == original.dataset_name
#         assert restored.run_id == original.run_id
#         assert restored.action == original.action
#         assert restored.start_time == original.start_time
#         assert restored.download is not None
#         assert restored.download.path == original.download.path
#         assert restored.download.sha256 == original.download.sha256
#         assert restored.download.size_mib == original.download.size_mib
#
#     def test_datetime_serialization_uses_isoformat(self) -> None:
#         # start_time should serialize to ISO 8601 string
#         ctx = PipelineContext(
#             dataset_name="test",
#             run_id="run_1",
#             action=PipelineAction.FIRST_RUN,
#             start_time=datetime(2026, 1, 19, 14, 30, 45, 123456),
#         )
#
#         result = ctx.to_dict()
#
#         assert result["start_time"] == "2026-01-19T14:30:45.123456"
#         assert isinstance(result["start_time"], str)
#
#     def test_action_serialization_uses_value(self) -> None:
#         # PipelineAction enum should serialize to string value
#         ctx = PipelineContext(
#             dataset_name="test",
#             run_id="run_1",
#             action=PipelineAction.RETRY,
#             start_time=datetime.now(),
#         )
#
#         result = ctx.to_dict()
#
#         assert result["action"] == "retry"
#         assert isinstance(result["action"], str)
#
#
# class TestPipelineContextMutation:
#     """Test mutable behavior of PipelineContext."""
#
#     def test_context_is_mutable(self) -> None:
#         # Should be able to update download/extraction/bronze/silver in place
#         ctx = PipelineContext(
#             dataset_name="test",
#             run_id="run_1",
#             action=PipelineAction.FIRST_RUN,
#             start_time=datetime.now(),
#         )
#
#         # Initially all None
#         assert ctx.download is None
#         assert ctx.extraction is None
#
#         # Should be able to mutate
#         ctx.download = DownloadResult(
#             path=Path("/tmp/test.7z"),
#             sha256="hash1",
#             size_mib=10.0,
#             original_filename="test.7z",
#         )
#
#         assert ctx.download is not None
#         assert ctx.download.sha256 == "hash1"
#
#         # Continue mutating
#         ctx.extraction = ExtractionResult(
#             path=Path("/tmp/test.gpkg"),
#             size_mib=9.5,
#             extracted_sha256="hash2",
#             archive_sha256="hash1",
#             original_filename="test.gpkg",
#         )
#
#         assert ctx.extraction is not None
#         assert ctx.extraction.extracted_sha256 == "hash2"
#
#     def test_accumulate_results_sequentially(self) -> None:
#         # Simulate pipeline: update download, then extraction, then bronze, then silver
#         ctx = PipelineContext(
#             dataset_name="iris",
#             run_id="iris_20260119_143000",
#             action=PipelineAction.FIRST_RUN,
#             start_time=datetime(2026, 1, 19, 14, 30, 0),
#         )
#
#         # Step 1: Download
#         ctx.download = DownloadResult(
#             path=Path("/data/landing/iris.7z"),
#             sha256="archive_hash",
#             size_mib=50.0,
#             original_filename="iris.7z",
#         )
#         assert ctx.download is not None
#         assert ctx.extraction is None
#         assert ctx.bronze is None
#         assert ctx.silver is None
#
#         # Step 2: Extraction
#         ctx.extraction = ExtractionResult(
#             path=Path("/data/landing/iris/iris.gpkg"),
#             size_mib=45.0,
#             extracted_sha256="file_hash",
#             archive_sha256="archive_hash",
#             original_filename="iris.gpkg",
#         )
#         assert ctx.download is not None
#         assert ctx.extraction is not None
#         assert ctx.bronze is None
#         assert ctx.silver is None
#
#         # Step 3: Bronze
#         ctx.bronze = BronzeResult(
#             path=Path("/data/bronze/iris/2025-01.parquet"),
#             row_count=150,
#             columns=["sepal_length", "sepal_width"],
#             sha256="file_hash",
#             archive_sha256="archive_hash",
#         )
#         assert ctx.download is not None
#         assert ctx.extraction is not None
#         assert ctx.bronze is not None
#         assert ctx.silver is None
#
#         # Step 4: Silver
#         ctx.silver = SilverResult(
#             path=Path("/data/silver/iris/2025-01.parquet"),
#             row_count=150,
#             columns=["sepal_length", "sepal_width"],
#             sha256="file_hash",
#             archive_sha256="archive_hash",
#         )
#         assert ctx.download is not None
#         assert ctx.extraction is not None
#         assert ctx.bronze is not None
#         assert ctx.silver is not None
#
#         # Verify final state
#         assert ctx.silver.row_count == 150
#
#
# class TestPipelineContextEdgeCases:
#     """Test edge cases and error handling."""
#
#     def test_deserialize_with_missing_optional_fields(self) -> None:
#         # Dict missing 'download'/'extraction'/etc should still work (None values)
#         # This tests backward compatibility if XCom dict doesn't have all fields
#         data = {
#             "dataset_name": "test",
#             "run_id": "run_1",
#             "action": "first_run",
#             "start_time": "2026-01-19T14:30:00",
#             # Missing: download, extraction, bronze, silver
#         }
#
#         ctx = PipelineContext.from_dict(data)
#
#         assert ctx.dataset_name == "test"
#         assert ctx.download is None
#         assert ctx.extraction is None
#         assert ctx.bronze is None
#         assert ctx.silver is None
#
#     def test_deserialize_invalid_action_raises_error(self) -> None:
#         # Invalid action string should raise ValueError
#         data = {
#             "dataset_name": "test",
#             "run_id": "run_1",
#             "action": "invalid_action_name",
#             "start_time": "2026-01-19T14:30:00",
#             "download": None,
#             "extraction": None,
#             "bronze": None,
#             "silver": None,
#         }
#
#        with pytest.raises(
#             ValueError,
#             match="'invalid_action_name' is not a valid PipelineAction"
#        ):
#            PipelineContext.from_dict(data)
#
#     def test_deserialize_invalid_datetime_raises_error(self) -> None:
#         # Invalid ISO datetime should raise ValueError
#         data = {
#             "dataset_name": "test",
#             "run_id": "run_1",
#             "action": "first_run",
#             "start_time": "not-a-valid-datetime",
#             "download": None,
#             "extraction": None,
#             "bronze": None,
#             "silver": None,
#         }
#
#         with pytest.raises(ValueError, match="Invalid isoformat string"):
#             PipelineContext.from_dict(data)
#
#     def test_deserialize_invalid_path_handled_gracefully(self) -> None:
#         # Path construction should work even for weird paths
#         # Python's Path() is quite permissive
#         data = {
#             "dataset_name": "test",
#             "run_id": "run_1",
#             "action": "first_run",
#             "start_time": "2026-01-19T14:30:00",
#             "download": {
#                 "path": "/some/../weird/./path/with spaces/file.gpkg",
#                 "sha256": "hash",
#                 "size_mib": 10.0,
#                 "original_filename": "file.gpkg",
#             },
#             "extraction": None,
#             "bronze": None,
#             "silver": None,
#         }
#
#         ctx = PipelineContext.from_dict(data)
#
#         # Should not raise, Path handles this
#         assert ctx.download is not None
#         assert isinstance(ctx.download.path, Path)
#         # Path normalizes some parts but keeps the string mostly as-is
#         assert "weird" in str(ctx.download.path)
#
#
# class TestPipelineContextIntegration:
#     """Integration tests with actual pipeline flow."""
#
#     def test_context_used_across_mock_tasks(self) -> None:
#         # Simulate full pipeline: create context, update in each "task", verify final state
#         # This simulates how context flows through Airflow tasks via XCom
#
#         # Task 1: check_should_run (creates context)
#         ctx = PipelineContext(
#             dataset_name="ign_contours_iris",
#             run_id="ign_contours_iris_20260119_143000",
#             action=PipelineAction.FIRST_RUN,
#             start_time=datetime(2026, 1, 19, 14, 30, 0),
#         )
#         xcom_1 = ctx.to_dict()
#
#         # Task 2: download_data (receives context, updates download)
#         ctx = PipelineContext.from_dict(xcom_1)
#         ctx.download = DownloadResult(
#             path=Path("/data/landing/ign_contours_iris.7z"),
#             sha256="archive_hash_abc",
#             size_mib=150.0,
#             original_filename="ign_contours_iris.7z",
#         )
#         xcom_2 = ctx.to_dict()
#
#         # Task 3: extract_archive (receives context, updates extraction)
#         ctx = PipelineContext.from_dict(xcom_2)
#         assert ctx.download is not None  # Verify download persisted
#         ctx.extraction = ExtractionResult(
#             path=Path("/data/landing/ign_contours_iris/contours.gpkg"),
#             size_mib=140.0,
#             extracted_sha256="file_hash_xyz",
#             archive_sha256="archive_hash_abc",
#             original_filename="contours.gpkg",
#         )
#         xcom_3 = ctx.to_dict()
#
#         # Task 4: convert_to_bronze (receives context, updates bronze)
#         ctx = PipelineContext.from_dict(xcom_3)
#         assert ctx.download is not None
#         assert ctx.extraction is not None
#         ctx.bronze = BronzeResult(
#             path=Path("/data/bronze/ign_contours_iris/2025-01.parquet"),
#             row_count=50000,
#             columns=["code_iris", "nom_iris", "geometry"],
#             sha256="file_hash_xyz",
#             archive_sha256="archive_hash_abc",
#         )
#         xcom_4 = ctx.to_dict()
#
#         # Task 5: transform_to_silver (receives context, updates silver)
#         ctx = PipelineContext.from_dict(xcom_4)
#         assert ctx.download is not None
#         assert ctx.extraction is not None
#         assert ctx.bronze is not None
#         ctx.silver = SilverResult(
#             path=Path("/data/silver/ign_contours_iris/2025-01.parquet"),
#             row_count=50000,
#             columns=["code_iris", "nom_iris", "geometry"],
#             sha256="file_hash_xyz",
#             archive_sha256="archive_hash_abc",
#         )
#
#         # Verify final state has complete history
#         assert ctx.dataset_name == "ign_contours_iris"
#         assert ctx.run_id == "ign_contours_iris_20260119_143000"
#         assert ctx.action == PipelineAction.FIRST_RUN
#         assert ctx.download.sha256 == "archive_hash_abc"
#         assert ctx.extraction.extracted_sha256 == "file_hash_xyz"
#         assert ctx.bronze.row_count == 50000
#         assert ctx.silver.row_count == 50000
#
#     def test_context_with_archive_pipeline(self) -> None:
#         # Context for archive dataset should have extraction result
#         # Simulates archive workflow: download → extract → bronze → silver
#         ctx = PipelineContext(
#             dataset_name="dataset_with_archive",
#             run_id="run_archive_123",
#             action=PipelineAction.REFRESH,
#             start_time=datetime.now(),
#         )
#
#         # Download archive
#         ctx.download = DownloadResult(
#             path=Path("/data/landing/archive.7z"),
#             sha256="archive_hash",
#             size_mib=100.0,
#             original_filename="archive.7z",
#         )
#
#         # Extract from archive
#         ctx.extraction = ExtractionResult(
#             path=Path("/data/landing/dataset/data.gpkg"),
#             size_mib=95.0,
#             extracted_sha256="extracted_hash",
#             archive_sha256="archive_hash",
#             original_filename="data.gpkg",
#         )
#
#         # Both download and extraction should be populated
#         assert ctx.download is not None
#         assert ctx.extraction is not None
#         assert ctx.extraction.archive_sha256 == ctx.download.sha256
#
#     def test_context_with_non_archive_pipeline(self) -> None:
#         # Context for non-archive dataset should have extraction populated from download
#         # Simulates non-archive workflow: download (creates extraction) → bronze → silver
#         ctx = PipelineContext(
#             dataset_name="dataset_direct_download",
#             run_id="run_direct_456",
#             action=PipelineAction.FIRST_RUN,
#             start_time=datetime.now(),
#         )
#
#         # Download non-archive file
#         ctx.download = DownloadResult(
#             path=Path("/data/landing/data.gpkg"),
#             sha256="file_hash",
#             size_mib=50.0,
#             original_filename="data.gpkg",
#         )
#
#         # For non-archive, extraction is created from download (same file)
#         ctx.extraction = ExtractionResult(
#             path=ctx.download.path,
#             size_mib=ctx.download.size_mib,
#             extracted_sha256=ctx.download.sha256,
#             archive_sha256=ctx.download.sha256,  # Same hash
#             original_filename=ctx.download.original_filename,
#         )
#
#         # Both should be populated, but with same data
#         assert ctx.download is not None
#         assert ctx.extraction is not None
#         assert ctx.extraction.path == ctx.download.path
#         assert ctx.extraction.extracted_sha256 == ctx.download.sha256
#         assert ctx.extraction.archive_sha256 == ctx.download.sha256
