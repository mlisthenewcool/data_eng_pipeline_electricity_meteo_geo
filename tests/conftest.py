"""TODO."""

# import pytest
# import yaml
#
#
# @pytest.fixture
# def sample_catalog_data():  # noqa: D103
#     return {
#         "datasets": {
#             "test_dataset": {
#                 "source": {
#                     "provider": "TEST",
#                     "url": "https://example.com/data.7z",
#                     "format": "7z",
#                     "inner_file": "data.gpkg",
#                 },
#                 "ingestion": {"version": "2025_01_01", "frequency": "daily"},
#                 "storage": "test/{version}.parquet",
#             }
#         }
#     }
#
#
# @pytest.fixture
# def temp_catalog(tmp_path, sample_catalog_data):  # noqa: D103
#     catalog_path = tmp_path / "catalog.yaml"
#     with catalog_path.open("w") as f:
#         yaml.dump(sample_catalog_data, f)
#     return catalog_path
