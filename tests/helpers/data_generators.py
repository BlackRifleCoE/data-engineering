"""
Test data generation utilities for DimensionProcessor tests.

Provides functions to create bronze and dimension DataFrames with various scenarios.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit
from typing import List, Dict, Any
from datetime import datetime, timedelta
import random
import string


def create_bronze_data(spark: SparkSession, records: List[Dict[str, Any]], load_timestamp: str = "2024-01-01 10:00:00") -> DataFrame:
    """
    Create a bronze DataFrame with specified records and LoadTimestamp.

    Args:
        spark: SparkSession instance
        records: List of dictionaries representing records
        load_timestamp: Timestamp string to add as LoadTimestamp column

    Returns:
        DataFrame with LoadTimestamp column added

    Example:
        >>> records = [{"id": 1, "name": "Alice", "value": "A"}]
        >>> df = create_bronze_data(spark, records, "2024-01-01 10:00:00")
    """
    if not records:
        # Return empty DataFrame with schema
        schema = ["id", "name", "value", "LoadTimestamp"]
        return spark.createDataFrame([], schema)

    df = spark.createDataFrame(records)
    return df.withColumn("LoadTimestamp", lit(load_timestamp))


def create_dimension_data(spark: SparkSession, records: List[Dict[str, Any]], sk_column: str = "customer_sk", sk_start: int = 1) -> DataFrame:
    """
    Create a dimension DataFrame with SCD2 metadata columns.

    Args:
        spark: SparkSession instance
        records: List of dicts with business columns plus SCD2 metadata
        sk_column: Name of surrogate key column
        sk_start: Starting value for surrogate key

    Returns:
        DataFrame with properly typed SCD2 columns

    Example:
        >>> records = [{
        ...     "id": 1, "name": "Alice", "value": "A",
        ...     "row_hash": "hash1",
        ...     "Effective_Start_Date": "2024-01-01",
        ...     "Effective_End_Date": None,
        ...     "Is_Current": 1
        ... }]
        >>> df = create_dimension_data(spark, records)
    """
    if not records:
        schema = [sk_column, "id", "name", "value", "row_hash",
                  "Effective_Start_Date", "Effective_End_Date", "Is_Current"]
        return spark.createDataFrame([], schema)

    # Add surrogate keys if not present
    for i, record in enumerate(records):
        if sk_column not in record:
            record[sk_column] = sk_start + i

    df = spark.createDataFrame(records)

    # Cast date columns to proper types
    if "Effective_Start_Date" in df.columns:
        df = df.withColumn("Effective_Start_Date", col("Effective_Start_Date").cast("date"))
    if "Effective_End_Date" in df.columns:
        df = df.withColumn("Effective_End_Date", col("Effective_End_Date").cast("date"))

    return df


def create_scd2_chain(spark: SparkSession, pk_value: int, versions: List[Dict[str, Any]], sk_column: str = "customer_sk", pk_column: str = "id") -> DataFrame:
    """
    Create a historical chain for a single primary key with multiple versions.

    Args:
        spark: SparkSession instance
        pk_value: Primary key value for all versions
        versions: List of version dicts with attributes and SCD2 metadata
        sk_column: Surrogate key column name
        pk_column: Primary key column name

    Returns:
        DataFrame with historical chain

    Example:
        >>> versions = [
        ...     {"name": "Alice", "value": "A", "Effective_Start_Date": "2024-01-01", "Effective_End_Date": "2024-02-01", "Is_Current": 0},
        ...     {"name": "Alice", "value": "B", "Effective_Start_Date": "2024-02-01", "Effective_End_Date": "2024-03-01", "Is_Current": 0},
        ...     {"name": "Alice", "value": "C", "Effective_Start_Date": "2024-03-01", "Effective_End_Date": None, "Is_Current": 1}
        ... ]
        >>> df = create_scd2_chain(spark, pk_value=1, versions=versions)
    """
    records = []
    for i, version in enumerate(versions):
        record = {pk_column: pk_value, sk_column: i + 1}
        record.update(version)
        records.append(record)

    return create_dimension_data(spark, records, sk_column=sk_column, sk_start=1)


def create_large_dataset(spark: SparkSession, num_records: int, num_columns: int = 5, include_load_timestamp: bool = True) -> DataFrame:
    """
    Generate a large random dataset for performance testing.

    Args:
        spark: SparkSession instance
        num_records: Number of records to generate
        num_columns: Number of data columns (excluding id and LoadTimestamp)
        include_load_timestamp: Whether to add LoadTimestamp column

    Returns:
        DataFrame with random data

    Example:
        >>> df = create_large_dataset(spark, num_records=10000, num_columns=10)
    """
    def random_string(length: int = 10) -> str:
        return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

    records = []
    for i in range(num_records):
        record = {"id": i + 1}
        for col_idx in range(num_columns):
            record[f"col_{col_idx}"] = random_string()
        records.append(record)

    df = spark.createDataFrame(records)

    if include_load_timestamp:
        df = df.withColumn("LoadTimestamp", lit("2024-01-01 10:00:00"))

    return df


def create_bronze_with_changes(spark: SparkSession, original_records: List[Dict[str, Any]],
                                 change_indices: List[int], change_column: str = "value",
                                 new_value_prefix: str = "Changed_") -> DataFrame:
    """
    Create a bronze DataFrame with specified records changed.

    Args:
        spark: SparkSession instance
        original_records: Original records
        change_indices: Indices of records to change
        change_column: Column to modify
        new_value_prefix: Prefix to add to changed values

    Returns:
        DataFrame with changes applied

    Example:
        >>> original = [{"id": 1, "name": "Alice", "value": "A"}, {"id": 2, "name": "Bob", "value": "B"}]
        >>> df = create_bronze_with_changes(spark, original, change_indices=[0], change_column="value")
        # Record 0 will have value="Changed_A"
    """
    records = []
    for i, record in enumerate(original_records):
        new_record = record.copy()
        if i in change_indices and change_column in new_record:
            new_record[change_column] = f"{new_value_prefix}{new_record[change_column]}"
        records.append(new_record)

    return create_bronze_data(spark, records)


def create_dimension_with_current_records(spark: SparkSession, num_records: int, sk_column: str = "customer_sk", pk_column: str = "id") -> DataFrame:
    """
    Create a dimension DataFrame with only current records (Is_Current=1).

    Args:
        spark: SparkSession instance
        num_records: Number of current records to create
        sk_column: Surrogate key column name
        pk_column: Primary key column name

    Returns:
        DataFrame with current records

    Example:
        >>> df = create_dimension_with_current_records(spark, num_records=5)
    """
    records = []
    for i in range(num_records):
        records.append({
            pk_column: i + 1,
            "name": f"Name_{i+1}",
            "value": f"Value_{i+1}",
            "row_hash": f"hash_{i+1}",
            "Effective_Start_Date": "2024-01-01",
            "Effective_End_Date": None,
            "Is_Current": 1
        })

    return create_dimension_data(spark, records, sk_column=sk_column, sk_start=1)
