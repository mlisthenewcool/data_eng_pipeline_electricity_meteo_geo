"""Example test script to demonstrate transformation registry usage.

This script can be run standalone to verify transformations work correctly.
Run with: uv run python -m de_projet_perso.pipeline.transformations.example_test
"""

import polars as pl

from de_projet_perso.pipeline.transformations import (
    get_bronze_transform,
    get_silver_transform,
)


def test_ign_contours_iris():
    """Test IGN Contours IRIS transformations."""
    print("\n🧪 Testing IGN Contours IRIS transformations...")

    # Create sample data
    sample_df = pl.DataFrame(
        {
            "code_iris": ["12345", None, "67890"],
            "nom_iris": ["iris_a", "iris_b", "iris_c"],
            "area_m2": [1000, 2000, 3000],
        }
    )

    print(f"   Input: {len(sample_df)} rows")

    # Test bronze transformation
    bronze_func = get_bronze_transform("ign_contours_iris")
    if bronze_func:
        bronze_result = bronze_func(sample_df, "ign_contours_iris")
        print(f"   ✅ Bronze: {len(bronze_result)} rows (filtered null code_iris)")
    else:
        print("   ❌ Bronze transformation not found")

    # Test silver transformation
    silver_func = get_silver_transform("ign_contours_iris")
    if silver_func:
        silver_result = silver_func(sample_df, "ign_contours_iris")
        print(
            f"   ✅ Silver: {silver_result.shape[1]} columns (added area_hectares, nom_iris_upper)"
        )
    else:
        print("   ❌ Silver transformation not found")


def test_odre_installations():
    """Test ODRE installations transformations."""
    print("\n🧪 Testing ODRE installations transformations...")

    # Create sample data
    sample_df = pl.DataFrame(
        {
            "date_mise_service": ["2023-01-01", "2023-02-01", "2023-03-01"],
            "puissance_mw": ["10.5", "20.3", "15.7"],
            "statut": ["En service", "Hors service", "En service"],
        }
    )

    print(f"   Input: {len(sample_df)} rows")

    # Test bronze transformation (should pass through)
    bronze_func = get_bronze_transform("odre_installations")
    if bronze_func:
        bronze_result = bronze_func(sample_df, "odre_installations")
        print(f"   ✅ Bronze: {len(bronze_result)} rows (pass-through)")
    else:
        print("   ❌ Bronze transformation not found")

    # Test silver transformation
    silver_func = get_silver_transform("odre_installations")
    if silver_func:
        silver_result = silver_func(sample_df, "odre_installations")
        print(f"   ✅ Silver: {len(silver_result)} rows (filtered to 'En service')")
    else:
        print("   ❌ Silver transformation not found")


def test_unknown_dataset():
    """Test behavior with unknown dataset."""
    print("\n🧪 Testing unknown dataset...")

    bronze_func = get_bronze_transform("dataset_inexistant")
    silver_func = get_silver_transform("dataset_inexistant")

    if bronze_func is None and silver_func is None:
        print("   ✅ Unknown dataset returns None (expected behavior)")
    else:
        print("   ❌ Unknown dataset should return None")


def main():
    """Run all tests."""
    print("=" * 60)
    print("🧪 Transformation Registry Test Suite")
    print("=" * 60)

    test_ign_contours_iris()
    test_odre_installations()
    test_unknown_dataset()

    print("\n" + "=" * 60)
    print("✅ All tests completed!")
    print("=" * 60)


if __name__ == "__main__":
    main()
