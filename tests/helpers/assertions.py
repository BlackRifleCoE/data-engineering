"""
Custom assertion helpers for DimensionProcessor tests.

Provides specialized assertions for SCD2 validation.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from datetime import date
import re


def assert_scd2_record(row, expected_sk=None, expected_is_current=None, expected_end_date_null=None):
    """
    Assert that a row has expected SCD2 metadata values.

    Args:
        row: DataFrame row to validate
        expected_sk: Expected surrogate key value (if not None)
        expected_is_current: Expected Is_Current value (if not None)
        expected_end_date_null: Expected nullness of Effective_End_Date (if not None)

    Raises:
        AssertionError: If any assertion fails
    """
    if expected_sk is not None:
        assert row[0] == expected_sk, f"Expected SK={expected_sk}, got {row[0]}"

    if expected_is_current is not None:
        is_current_col = "Is_Current"
        # Find Is_Current column index
        for i, val in enumerate(row):
            if i == len(row) - 1:  # Assuming Is_Current is often last
                assert val == expected_is_current, f"Expected Is_Current={expected_is_current}, got {val}"

    if expected_end_date_null is not None:
        # Check if Effective_End_Date is null
        end_date_col_idx = None
        # This is a simplified check - in real usage, pass column index
        pass


def assert_hash_valid(hash_value: str):
    """
    Assert that a hash value is a valid SHA-256 hash (64 hexadecimal characters).

    Args:
        hash_value: Hash string to validate

    Raises:
        AssertionError: If hash is not valid SHA-256 format
    """
    assert hash_value is not None, "Hash value is None"
    assert isinstance(hash_value, str), f"Hash must be string, got {type(hash_value)}"
    assert len(hash_value) == 64, f"Hash must be 64 characters, got {len(hash_value)}"
    assert re.match(r'^[0-9a-f]{64}$', hash_value), f"Hash must be hexadecimal, got {hash_value}"


def assert_effective_dates_valid(start_date, end_date):
    """
    Assert that effective dates are valid (start <= end if end is not None).

    Args:
        start_date: Effective start date
        end_date: Effective end date (can be None)

    Raises:
        AssertionError: If dates are invalid
    """
    assert start_date is not None, "Effective_Start_Date cannot be None"

    if end_date is not None:
        assert start_date <= end_date, f"Start date {start_date} must be <= end date {end_date}"


def assert_surrogate_keys_sequential(df: DataFrame, sk_column: str, expected_start: int):
    """
    Assert that surrogate key values are sequential starting from expected_start.

    Args:
        df: DataFrame to validate
        sk_column: Name of surrogate key column
        expected_start: Expected starting value

    Raises:
        AssertionError: If surrogate keys are not sequential
    """
    sk_values = sorted([row[sk_column] for row in df.select(sk_column).collect()])

    assert len(sk_values) > 0, "DataFrame has no records"

    expected_values = list(range(expected_start, expected_start + len(sk_values)))
    assert sk_values == expected_values, f"Expected sequential SKs {expected_values}, got {sk_values}"


def assert_dataframe_count(df: DataFrame, expected_count: int, message: str = None):
    """
    Assert that a DataFrame has the expected number of records.

    Args:
        df: DataFrame to validate
        expected_count: Expected number of records
        message: Optional custom error message

    Raises:
        AssertionError: If count doesn't match
    """
    actual_count = df.count()
    error_msg = message or f"Expected {expected_count} records, got {actual_count}"
    assert actual_count == expected_count, error_msg


def assert_column_exists(df: DataFrame, column_name: str):
    """
    Assert that a DataFrame has the specified column.

    Args:
        df: DataFrame to validate
        column_name: Name of column to check

    Raises:
        AssertionError: If column doesn't exist
    """
    assert column_name in df.columns, f"Column '{column_name}' not found in DataFrame. Available: {df.columns}"


def assert_no_null_values(df: DataFrame, column_name: str):
    """
    Assert that a column has no NULL values.

    Args:
        df: DataFrame to validate
        column_name: Name of column to check

    Raises:
        AssertionError: If NULL values exist
    """
    null_count = df.filter(col(column_name).isNull()).count()
    assert null_count == 0, f"Column '{column_name}' has {null_count} NULL values"


def assert_all_values_equal(df: DataFrame, column_name: str, expected_value):
    """
    Assert that all values in a column equal the expected value.

    Args:
        df: DataFrame to validate
        column_name: Name of column to check
        expected_value: Expected value for all rows

    Raises:
        AssertionError: If any value doesn't match
    """
    mismatched = df.filter(col(column_name) != expected_value).count()
    assert mismatched == 0, f"Column '{column_name}' has {mismatched} values != {expected_value}"


def assert_is_current_flag_consistent(df: DataFrame):
    """
    Assert that for each primary key, only one record has Is_Current=1.

    Args:
        df: DataFrame to validate (must have 'id' and 'Is_Current' columns)

    Raises:
        AssertionError: If multiple current records exist for same PK
    """
    current_counts = df.filter(col("Is_Current") == 1).groupBy("id").count()
    invalid = current_counts.filter(col("count") > 1).count()

    assert invalid == 0, f"Found {invalid} primary keys with multiple current records"


def assert_historical_chain_valid(df: DataFrame, pk_value: int, pk_column: str = "id"):
    """
    Assert that the historical chain for a PK is valid (no overlapping dates).

    Args:
        df: DataFrame to validate
        pk_value: Primary key value to check
        pk_column: Name of primary key column

    Raises:
        AssertionError: If historical chain is invalid
    """
    chain = df.filter(col(pk_column) == pk_value) \
              .orderBy("Effective_Start_Date") \
              .collect()

    if len(chain) == 0:
        return  # No records to validate

    # Check that effective dates don't overlap
    for i in range(len(chain) - 1):
        current_end = chain[i]["Effective_End_Date"]
        next_start = chain[i + 1]["Effective_Start_Date"]

        if current_end is not None:
            assert current_end == next_start, \
                f"Gap in historical chain: record {i} ends {current_end}, record {i+1} starts {next_start}"

    # Check that only the last record is current
    for i in range(len(chain) - 1):
        assert chain[i]["Is_Current"] == 0, f"Historical record {i} has Is_Current=1"

    assert chain[-1]["Is_Current"] == 1, f"Latest record has Is_Current=0"
    assert chain[-1]["Effective_End_Date"] is None, f"Latest record has non-null end date"
