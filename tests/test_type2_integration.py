"""
Integration tests for Type2Dimension with Delta Lake operations.

Tests actual Delta merge, append, and end-to-end SCD2 processing.
Note: These tests require Delta Lake and will be slower than unit tests.
"""

import pytest
from pyspark.sql.functions import col
from delta import DeltaTable
from SCD2 import Type2Dimension
from tests.helpers.data_generators import create_bronze_data, create_dimension_data
from tests.helpers.assertions import assert_dataframe_count, assert_surrogate_keys_sequential
import shutil


@pytest.mark.integration
def test_bootstrap_empty_dimension_single_record(spark, tmp_path):
    """
    Test bootstrapping an empty dimension with a single record.

    Expected:
    - 1 record inserted
    - SK = 1
    - Is_Current = 1
    - Effective_End_Date = NULL
    """
    # Arrange
    table_path = str(tmp_path / "test_table")
    table_name = "test_dim_bootstrap"

    bronze_records = [{"id": 1, "name": "Alice", "value": "A"}]
    bronze_df = create_bronze_data(spark, bronze_records, load_timestamp="2024-01-01 10:00:00")

    # Create empty Delta table
    empty_dim = spark.createDataFrame([], bronze_df.schema
                                      .add("customer_sk", "long")
                                      .add("row_hash", "string")
                                      .add("Effective_Start_Date", "date")
                                      .add("Effective_End_Date", "date")
                                      .add("Is_Current", "integer"))
    empty_dim.write.format("delta").mode("overwrite").save(table_path)

    processor = Type2Dimension(
        spark=spark,
        bronzeDF=bronze_df,
        dimensionDF=None,
        dimensionTableName=table_name,
        hashColumnList=["name", "value"],
        primaryKeyColumnName="id",
        surrogateKeyColumnName="customer_sk"
    )

    # Register table
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name} USING DELTA LOCATION '{table_path}'")

    # Act
    result_count = processor.ProcessUpdate()

    # Assert
    assert result_count == 1, "Should insert 1 record"

    result_df = spark.read.format("delta").load(table_path)
    assert_dataframe_count(result_df, 1)

    record = result_df.collect()[0]
    assert record["customer_sk"] == 1, "First SK should be 1"
    assert record["Is_Current"] == 1, "Should be current"
    assert record["Effective_End_Date"] is None, "End date should be NULL"
    assert record["id"] == 1
    assert record["name"] == "Alice"

    # Cleanup
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    shutil.rmtree(tmp_path, ignore_errors=True)


@pytest.mark.integration
def test_bootstrap_empty_dimension_multiple_records(spark, tmp_path):
    """
    Test bootstrapping with multiple records.

    Expected:
    - 5 records inserted
    - SKs are sequential (1, 2, 3, 4, 5)
    - All have Is_Current = 1
    """
    # Arrange
    table_path = str(tmp_path / "test_table")
    table_name = "test_dim_multi"

    bronze_records = [
        {"id": 1, "name": "Alice", "value": "A"},
        {"id": 2, "name": "Bob", "value": "B"},
        {"id": 3, "name": "Charlie", "value": "C"},
        {"id": 4, "name": "Dave", "value": "D"},
        {"id": 5, "name": "Eve", "value": "E"}
    ]
    bronze_df = create_bronze_data(spark, bronze_records, load_timestamp="2024-01-01 10:00:00")

    # Create empty Delta table
    empty_dim = spark.createDataFrame([], bronze_df.schema
                                      .add("customer_sk", "long")
                                      .add("row_hash", "string")
                                      .add("Effective_Start_Date", "date")
                                      .add("Effective_End_Date", "date")
                                      .add("Is_Current", "integer"))
    empty_dim.write.format("delta").mode("overwrite").save(table_path)

    processor = Type2Dimension(
        spark=spark,
        bronzeDF=bronze_df,
        dimensionDF=None,
        dimensionTableName=table_name,
        hashColumnList=["name", "value"],
        primaryKeyColumnName="id",
        surrogateKeyColumnName="customer_sk"
    )

    spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name} USING DELTA LOCATION '{table_path}'")

    # Act
    result_count = processor.ProcessUpdate()

    # Assert
    assert result_count == 5, "Should insert 5 records"

    result_df = spark.read.format("delta").load(table_path)
    assert_dataframe_count(result_df, 5)
    assert_surrogate_keys_sequential(result_df, "customer_sk", 1)

    # All should be current
    current_count = result_df.filter(col("Is_Current") == 1).count()
    assert current_count == 5, "All records should be current"

    # Cleanup
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    shutil.rmtree(tmp_path, ignore_errors=True)


@pytest.mark.integration
def test_update_single_changed_record(spark, tmp_path):
    """
    Test updating a single record that has changed.

    Expected:
    - Old record: Is_Current=0, Effective_End_Date set
    - New record: SK=2, Is_Current=1, Effective_End_Date=NULL
    - Total records: 2
    """
    # Arrange
    table_path = str(tmp_path / "test_table")
    table_name = "test_dim_update"

    # Create initial dimension with 1 record
    initial_dim = spark.createDataFrame([
        (1, 1, "Alice", "ValueA", "hash_old", "2024-01-01", None, 1)
    ], ["customer_sk", "id", "name", "value", "row_hash",
        "Effective_Start_Date", "Effective_End_Date", "Is_Current"])

    initial_dim = initial_dim.withColumn("Effective_Start_Date", col("Effective_Start_Date").cast("date")) \
                             .withColumn("Effective_End_Date", col("Effective_End_Date").cast("date"))

    initial_dim.write.format("delta").mode("overwrite").save(table_path)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name} USING DELTA LOCATION '{table_path}'")

    # Bronze with changed record
    bronze_records = [{"id": 1, "name": "Alice", "value": "ValueB"}]  # Changed value
    bronze_df = create_bronze_data(spark, bronze_records, load_timestamp="2024-02-01 10:00:00")

    # Read current dimension
    current_dim = spark.read.format("delta").load(table_path)

    processor = Type2Dimension(
        spark=spark,
        bronzeDF=bronze_df,
        dimensionDF=current_dim,
        dimensionTableName=table_name,
        hashColumnList=["name", "value"],
        primaryKeyColumnName="id",
        surrogateKeyColumnName="customer_sk"
    )

    # Act
    result_count = processor.ProcessUpdate()

    # Assert
    assert result_count == 1, "Should insert 1 new version"

    result_df = spark.read.format("delta").load(table_path)
    assert_dataframe_count(result_df, 2, "Should have 2 total records (old + new)")

    # Check old record
    old_record = result_df.filter(col("customer_sk") == 1).collect()[0]
    assert old_record["Is_Current"] == 0, "Old record should not be current"
    assert old_record["Effective_End_Date"] is not None, "Old record should have end date"

    # Check new record
    new_record = result_df.filter(col("customer_sk") == 2).collect()[0]
    assert new_record["Is_Current"] == 1, "New record should be current"
    assert new_record["Effective_End_Date"] is None, "New record should have NULL end date"
    assert new_record["value"] == "ValueB", "New record should have updated value"

    # Cleanup
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    shutil.rmtree(tmp_path, ignore_errors=True)


@pytest.mark.integration
def test_no_changes_returns_zero(spark, tmp_path):
    """
    Test that unchanged records don't create new versions.

    Expected:
    - Returns 0
    - No new records inserted
    """
    # Arrange
    table_path = str(tmp_path / "test_table")
    table_name = "test_dim_nochange"

    # Create dimension
    initial_dim = spark.createDataFrame([
        (1, 1, "Alice", "ValueA", "hash1", "2024-01-01", None, 1)
    ], ["customer_sk", "id", "name", "value", "row_hash",
        "Effective_Start_Date", "Effective_End_Date", "Is_Current"])

    initial_dim = initial_dim.withColumn("Effective_Start_Date", col("Effective_Start_Date").cast("date")) \
                             .withColumn("Effective_End_Date", col("Effective_End_Date").cast("date"))

    initial_dim.write.format("delta").mode("overwrite").save(table_path)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name} USING DELTA LOCATION '{table_path}'")

    # Bronze with identical record
    bronze_records = [{"id": 1, "name": "Alice", "value": "ValueA"}]  # Unchanged
    bronze_df = create_bronze_data(spark, bronze_records, load_timestamp="2024-02-01 10:00:00")

    current_dim = spark.read.format("delta").load(table_path)

    processor = Type2Dimension(
        spark=spark,
        bronzeDF=bronze_df,
        dimensionDF=current_dim,
        dimensionTableName=table_name,
        hashColumnList=["name", "value"],
        primaryKeyColumnName="id",
        surrogateKeyColumnName="customer_sk"
    )

    # Act
    result_count = processor.ProcessUpdate()

    # Assert
    assert result_count == 0, "Should not insert any records when unchanged"

    result_df = spark.read.format("delta").load(table_path)
    assert_dataframe_count(result_df, 1, "Should still have only 1 record")

    # Cleanup
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    shutil.rmtree(tmp_path, ignore_errors=True)


@pytest.mark.slow
@pytest.mark.integration
def test_large_dataset_performance(spark, tmp_path):
    """
    Performance test with larger dataset.

    Tests 1000 records (reduced from 100k for faster testing).
    """
    # Arrange
    table_path = str(tmp_path / "test_table")
    table_name = "test_dim_perf"

    # Create 1000 bronze records
    bronze_records = [
        {"id": i, "name": f"Name_{i}", "value": f"Value_{i}"}
        for i in range(1000)
    ]
    bronze_df = create_bronze_data(spark, bronze_records, load_timestamp="2024-01-01 10:00:00")

    empty_dim = spark.createDataFrame([], bronze_df.schema
                                      .add("customer_sk", "long")
                                      .add("row_hash", "string")
                                      .add("Effective_Start_Date", "date")
                                      .add("Effective_End_Date", "date")
                                      .add("Is_Current", "integer"))
    empty_dim.write.format("delta").mode("overwrite").save(table_path)

    processor = Type2Dimension(
        spark=spark,
        bronzeDF=bronze_df,
        dimensionDF=None,
        dimensionTableName=table_name,
        hashColumnList=["name", "value"],
        primaryKeyColumnName="id",
        surrogateKeyColumnName="customer_sk"
    )

    spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name} USING DELTA LOCATION '{table_path}'")

    # Act
    result_count = processor.ProcessUpdate()

    # Assert
    assert result_count == 1000, "Should insert all 1000 records"

    result_df = spark.read.format("delta").load(table_path)
    assert_dataframe_count(result_df, 1000)

    # Cleanup
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    shutil.rmtree(tmp_path, ignore_errors=True)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
