"""
Tests for Type1Dimension SCD1 processing.

Tests basic update logic, outer join behavior, and edge cases.
"""

import pytest
from pyspark.sql.functions import col
from SCD import Type1Dimension
from tests.helpers.data_generators import create_bronze_data


@pytest.mark.unit
def test_type1_basic_update(spark):
    """
    Test basic Type1 update logic.

    Matching records should be updated, new records inserted.
    """
    # Arrange
    source_data = [
        {"id": 1, "name": "Alice_Updated", "value": "A_New"},
        {"id": 3, "name": "Charlie", "value": "C"}
    ]
    source_df = spark.createDataFrame(source_data)

    target_data = [
        {"id": 1, "name": "Alice", "value": "A"},
        {"id": 2, "name": "Bob", "value": "B"}
    ]
    target_df = spark.createDataFrame(target_data)

    processor = Type1Dimension(
        source_df=source_df,
        target_df=target_df,
        key_columns=["id"],
        update_columns=["name", "value"]
    )

    # Act
    result = processor.ProcessUpdate()

    # Assert
    assert result.count() == 3, "Should have 3 records (1 updated, 1 unchanged, 1 new)"

    # Check updated record
    alice = result.filter(col("id") == 1).collect()[0]
    assert alice["name"] == "Alice_Updated", "Alice should be updated"

    # Check new record
    charlie = result.filter(col("id") == 3).collect()[0]
    assert charlie["name"] == "Charlie", "Charlie should be inserted"


@pytest.mark.unit
def test_type1_outer_join_behavior(spark):
    """
    Test that outer join preserves all records from both source and target.
    """
    # Arrange
    source_data = [
        {"id": 2, "name": "Bob", "value": "B"},
        {"id": 3, "name": "Charlie", "value": "C"}
    ]
    source_df = spark.createDataFrame(source_data)

    target_data = [
        {"id": 1, "name": "Alice", "value": "A"},
        {"id": 2, "name": "Bob_Old", "value": "B_Old"}
    ]
    target_df = spark.createDataFrame(target_data)

    processor = Type1Dimension(
        source_df=source_df,
        target_df=target_df,
        key_columns=["id"],
        update_columns=["name", "value"]
    )

    # Act
    result = processor.ProcessUpdate()

    # Assert
    result_ids = sorted([row["id"] for row in result.select("id").collect()])
    assert result_ids == [1, 2, 3], "Should have records with PKs 1, 2, 3"


@pytest.mark.unit
def test_type1_no_matching_keys(spark):
    """
    Test outer join when source and target have no overlapping keys.
    """
    # Arrange
    source_data = [{"id": 3, "name": "Charlie", "value": "C"}]
    source_df = spark.createDataFrame(source_data)

    target_data = [{"id": 1, "name": "Alice", "value": "A"}]
    target_df = spark.createDataFrame(target_data)

    processor = Type1Dimension(
        source_df=source_df,
        target_df=target_df,
        key_columns=["id"],
        update_columns=["name", "value"]
    )

    # Act
    result = processor.ProcessUpdate()

    # Assert
    assert result.count() == 2, "Should have both records from source and target"


@pytest.mark.unit
def test_type1_empty_source(spark):
    """Test with empty source DataFrame."""
    # Arrange
    source_df = spark.createDataFrame([], "id INT, name STRING, value STRING")

    target_data = [{"id": 1, "name": "Alice", "value": "A"}]
    target_df = spark.createDataFrame(target_data)

    processor = Type1Dimension(
        source_df=source_df,
        target_df=target_df,
        key_columns=["id"],
        update_columns=["name", "value"]
    )

    # Act
    result = processor.ProcessUpdate()

    # Assert
    assert result.count() == 1, "Should preserve target record when source is empty"


@pytest.mark.unit
def test_type1_empty_target(spark):
    """Test with empty target DataFrame."""
    # Arrange
    source_data = [{"id": 1, "name": "Alice", "value": "A"}]
    source_df = spark.createDataFrame(source_data)

    target_df = spark.createDataFrame([], "id INT, name STRING, value STRING")

    processor = Type1Dimension(
        source_df=source_df,
        target_df=target_df,
        key_columns=["id"],
        update_columns=["name", "value"]
    )

    # Act
    result = processor.ProcessUpdate()

    # Assert
    assert result.count() == 1, "Should insert source record when target is empty"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
