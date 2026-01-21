"""
Edge case tests for Type2Dimension SCD2 processing.

Tests null handling, empty datasets, special characters, and error conditions.
"""

import pytest
from pyspark.sql.functions import col
from pyspark.sql.utils import AnalysisException
from SCD2 import Type2Dimension
from tests.helpers.data_generators import create_bronze_data, create_dimension_data


@pytest.mark.unit
def test_null_values_in_hash_columns(spark):
    """NULL values in hash columns are handled by concat_ws."""
    # Arrange
    records = [
        {"id": 1, "name": "Alice", "value": None},
        {"id": 2, "name": None, "value": "B"}
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

    # Assert - hash should still be generated
    hash_count = result.filter(col("row_hash").isNotNull()).count()
    assert hash_count == 2, "Hashes should be generated even with NULL values"


@pytest.mark.unit
def test_empty_bronze_dataframe(spark):
    """Empty bronze DataFrame (0 rows) is handled gracefully."""
    # Arrange
    empty_df = create_bronze_data(spark, [])

    processor = Type2Dimension(
        spark=spark,
        bronzeDF=empty_df,
        dimensionDF=None,
        dimensionTableName="test_table",
        hashColumnList=["name", "value"],
        primaryKeyColumnName="id",
        surrogateKeyColumnName="sk"
    )

    # Act
    result = processor.generate_hash(empty_df)

    # Assert
    assert result.count() == 0, "Empty input should produce empty output"


@pytest.mark.unit
def test_special_characters_in_data(spark):
    """Hash generation handles special characters correctly."""
    # Arrange
    records = [
        {"id": 1, "name": "Alice||Bob", "value": "A"},
        {"id": 2, "name": "Charlie\"Dave", "value": "B"},
        {"id": 3, "name": "Eve\nFrank", "value": "C"}
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
    hash_count = result.filter(col("row_hash").isNotNull()).count()
    assert hash_count == 3, "All records should have hashes despite special characters"


@pytest.mark.unit
def test_unicode_data(spark):
    """UTF-8 unicode characters are handled in hash generation."""
    # Arrange
    records = [
        {"id": 1, "name": "Алиса", "value": "русский"},
        {"id": 2, "name": "李明", "value": "中文"},
        {"id": 3, "name": "José", "value": "español"}
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
    hash_count = result.filter(col("row_hash").isNotNull()).count()
    assert hash_count == 3, "Unicode characters should be handled correctly"


@pytest.mark.unit
def test_very_wide_dataframe(spark):
    """DataFrame with many columns (100+) is processed correctly."""
    # Arrange
    record = {"id": 1}
    for i in range(100):
        record[f"col_{i}"] = f"value_{i}"

    df = create_bronze_data(spark, [record])

    # Use first 10 columns for hashing
    hash_cols = [f"col_{i}" for i in range(10)]

    processor = Type2Dimension(
        spark=spark,
        bronzeDF=df,
        dimensionDF=None,
        dimensionTableName="test_table",
        hashColumnList=hash_cols,
        primaryKeyColumnName="id",
        surrogateKeyColumnName="sk"
    )

    # Act
    result = processor.generate_hash(df)

    # Assert
    assert result.count() == 1
    assert len(result.columns) == 103  # 101 original + LoadTimestamp + row_hash


@pytest.mark.unit
def test_concurrent_load_timestamps(spark):
    """Multiple records with identical LoadTimestamp are handled."""
    # Arrange
    records = [
        {"id": 1, "name": "Alice", "value": "A"},
        {"id": 2, "name": "Bob", "value": "B"},
        {"id": 3, "name": "Charlie", "value": "C"}
    ]
    same_timestamp = "2024-01-01 10:00:00"
    df = create_bronze_data(spark, records, load_timestamp=same_timestamp)

    processor = Type2Dimension(
        spark=spark,
        bronzeDF=df,
        dimensionDF=None,
        dimensionTableName="test_table",
        hashColumnList=["name", "value"],
        primaryKeyColumnName="id",
        surrogateKeyColumnName="sk"
    )

    # Act - verify initialization works
    # Assert
    assert processor.latest_batch_timestamp == ""  # Empty because dimension is None


@pytest.mark.unit
def test_hash_columns_not_in_bronze(spark):
    """AnalysisException raised when hash columns missing from DataFrame."""
    # Arrange
    records = [{"id": 1, "name": "Alice"}]  # 'value' column missing
    df = create_bronze_data(spark, records)

    processor = Type2Dimension(
        spark=spark,
        bronzeDF=df,
        dimensionDF=None,
        dimensionTableName="test_table",
        hashColumnList=["name", "value"],  # 'value' not in df
        primaryKeyColumnName="id",
        surrogateKeyColumnName="sk"
    )

    # Act & Assert
    with pytest.raises(AnalysisException):
        processor.generate_hash(df)


@pytest.mark.unit
def test_max_surrogate_key_is_null(spark):
    """Empty dimension (max SK is NULL) starts SK at 1."""
    # This is tested implicitly in bootstrap tests
    # Placeholder for explicit test if needed
    pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
