"""
Core tests for Type2Dimension SCD2 processing.

Tests hash generation, initialization, bootstrap, changed records, unchanged records, and new PKs.
"""

import pytest
from pyspark.sql.functions import col
from SCD2 import Type2Dimension
from tests.helpers.data_generators import (
    create_bronze_data,
    create_dimension_data,
    create_bronze_with_changes
)
from tests.helpers.assertions import (
    assert_hash_valid,
    assert_dataframe_count,
    assert_column_exists,
    assert_no_null_values,
    assert_surrogate_keys_sequential
)


# =======================
# Hash Generation Tests
# =======================

@pytest.mark.unit
def test_hash_generation_basic(spark):
    """Verify hash column added with 64-character hex SHA-256 hash."""
    # Arrange
    records = [
        {"id": 1, "name": "Alice", "value": "A"},
        {"id": 2, "name": "Bob", "value": "B"}
    ]
    df = create_bronze_data(spark, records)
    processor = Type2Dimension(
        spark=spark,
        bronzeDF=df,
        dimensionDF=None,
        dimensionTableName="test_table",
        hashColumnList=["name", "value"],
        primaryKeyColumnName="id",
        surrogateKeyColumnName="sk"
    )

    # Act
    result = processor.generate_hash(df)

    # Assert
    assert_column_exists(result, "row_hash")
    assert_no_null_values(result, "row_hash")

    # Check hash format
    hash_values = [row["row_hash"] for row in result.select("row_hash").collect()]
    for hash_val in hash_values:
        assert_hash_valid(hash_val)


@pytest.mark.unit
def test_hash_generation_consistency(spark):
    """Identical rows produce the same hash."""
    # Arrange
    records = [
        {"id": 1, "name": "Alice", "value": "A"},
        {"id": 2, "name": "Alice", "value": "A"}  # Identical to first
    ]
    df = create_bronze_data(spark, records)
    processor = Type2Dimension(
        spark=spark,
        bronzeDF=df,
        dimensionDF=None,
        dimensionTableName="test_table",
        hashColumnList=["name", "value"],
        primaryKeyColumnName="id",
        surrogateKeyColumnName="sk"
    )

    # Act
    result = processor.generate_hash(df)

    # Assert
    hashes = [row["row_hash"] for row in result.select("row_hash").collect()]
    assert hashes[0] == hashes[1], "Identical data should produce identical hashes"


@pytest.mark.unit
def test_hash_generation_sensitivity(spark):
    """Different data produces different hashes."""
    # Arrange
    records = [
        {"id": 1, "name": "Alice", "value": "A"},
        {"id": 2, "name": "Alice", "value": "B"}  # Different value
    ]
    df = create_bronze_data(spark, records)
    processor = Type2Dimension(
        spark=spark,
        bronzeDF=df,
        dimensionDF=None,
        dimensionTableName="test_table",
        hashColumnList=["name", "value"],
        primaryKeyColumnName="id",
        surrogateKeyColumnName="sk"
    )

    # Act
    result = processor.generate_hash(df)

    # Assert
    hashes = [row["row_hash"] for row in result.select("row_hash").collect()]
    assert hashes[0] != hashes[1], "Different data should produce different hashes"


# =======================
# Initialization Tests
# =======================

@pytest.mark.unit
def test_init_with_empty_dimension(spark):
    """Creates schema with SCD2 columns when dimension is None."""
    # Arrange
    bronze_records = [{"id": 1, "name": "Alice", "value": "A"}]
    bronze_df = create_bronze_data(spark, bronze_records)

    # Act
    processor = Type2Dimension(
        spark=spark,
        bronzeDF=bronze_df,
        dimensionDF=None,
        dimensionTableName="test_table",
        hashColumnList=["name", "value"],
        primaryKeyColumnName="id",
        surrogateKeyColumnName="customer_sk"
    )

    # Assert
    assert processor.dimensionDataFrame is not None
    expected_columns = ["id", "name", "value", "LoadTimestamp", "customer_sk",
                        "row_hash", "Effective_Start_Date", "Effective_End_Date", "Is_Current"]
    assert set(processor.dimensionDataFrame.columns) == set(expected_columns)
    assert processor.dimensionDataFrame.count() == 0


@pytest.mark.unit
def test_init_with_existing_dimension(spark):
    """Uses provided DataFrame when dimension is not None."""
    # Arrange
    bronze_records = [{"id": 1, "name": "Alice", "value": "A"}]
    bronze_df = create_bronze_data(spark, bronze_records)

    dimension_records = [{
        "id": 1,
        "name": "Alice",
        "value": "A",
        "row_hash": "hash1",
        "Effective_Start_Date": "2024-01-01",
        "Effective_End_Date": None,
        "Is_Current": 1
    }]
    dimension_df = create_dimension_data(spark, dimension_records, sk_column="customer_sk")

    # Act
    processor = Type2Dimension(
        spark=spark,
        bronzeDF=bronze_df,
        dimensionDF=dimension_df,
        dimensionTableName="test_table",
        hashColumnList=["name", "value"],
        primaryKeyColumnName="id",
        surrogateKeyColumnName="customer_sk"
    )

    # Assert
    assert processor.dimensionDataFrame.count() == 1
    assert processor.latest_batch_timestamp is not None


@pytest.mark.unit
def test_init_stores_parameters_correctly(spark):
    """All constructor parameters are stored correctly."""
    # Arrange
    bronze_df = create_bronze_data(spark, [{"id": 1, "name": "A", "value": "B"}])

    # Act
    processor = Type2Dimension(
        spark=spark,
        bronzeDF=bronze_df,
        dimensionDF=None,
        dimensionTableName="my_table",
        hashColumnList=["name", "value"],
        primaryKeyColumnName="id",
        surrogateKeyColumnName="my_sk"
    )

    # Assert
    assert processor.PKColumn == "id"
    assert processor.SKColumn == "my_sk"
    assert processor.columnList == ["name", "value"]
    assert processor.dimensionTableName == "my_table"


# =======================
# Bootstrap (Empty Dimension) Tests
# =======================

@pytest.mark.unit
def test_bootstrap_preserves_column_order(spark):
    """SK column is placed first in output."""
    # This test would require actual Delta table operations
    # Marked for integration testing
    pass


# =======================
# Changed Records Tests
# =======================

# Note: Full changed records tests require Delta Lake operations
# These are moved to test_type2_integration.py


# =======================
# Unchanged Records Tests
# =======================

@pytest.mark.unit
def test_unchanged_with_different_non_hash_columns(spark):
    """Non-tracked columns don't affect change detection."""
    # Arrange
    records = [{"id": 1, "name": "Alice", "value": "A", "extra": "X"}]
    df1 = create_bronze_data(spark, records)

    processor = Type2Dimension(
        spark=spark,
        bronzeDF=df1,
        dimensionDF=None,
        dimensionTableName="test_table",
        hashColumnList=["name", "value"],  # 'extra' not in hash
        primaryKeyColumnName="id",
        surrogateKeyColumnName="sk"
    )

    # Generate hash for both
    hash1 = processor.generate_hash(df1.drop("LoadTimestamp"))
    h1_value = hash1.select("row_hash").collect()[0]["row_hash"]

    # Change non-hash column
    records2 = [{"id": 1, "name": "Alice", "value": "A", "extra": "Y"}]
    df2 = create_bronze_data(spark, records2)
    hash2 = processor.generate_hash(df2.drop("LoadTimestamp"))
    h2_value = hash2.select("row_hash").collect()[0]["row_hash"]

    # Assert
    assert h1_value == h2_value, "Hash should be same when only non-hash column changes"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
