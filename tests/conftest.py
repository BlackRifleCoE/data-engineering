"""
Pytest fixtures for DimensionProcessor tests.

Provides SparkSession with Delta Lake support and reusable test data fixtures.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from datetime import datetime
from typing import Callable
import tempfile
import shutil
import os


@pytest.fixture(scope="session")
def spark_session():
    """
    Create a SparkSession with Delta Lake extensions for the entire test session.

    Configured for local testing with Delta support.
    """
    # Workaround for Windows - set a dummy HADOOP_HOME to avoid winutils.exe error
    if os.name == 'nt' and 'HADOOP_HOME' not in os.environ:
        # Create a temp directory for Hadoop home
        hadoop_home = tempfile.mkdtemp(prefix="hadoop_")
        os.environ['HADOOP_HOME'] = hadoop_home

    # Import delta's configure_spark_with_delta_pip to handle Delta JAR dependencies
    from delta import configure_spark_with_delta_pip

    builder = (SparkSession.builder
               .appName("DimensionProcessor_Tests")
               .master("local[*]")
               .config("spark.sql.shuffle.partitions", "2")  # Reduce partitions for tests
               .config("spark.default.parallelism", "2")
               .config("spark.sql.warehouse.dir", tempfile.mkdtemp(prefix="spark_warehouse_")))

    # Configure Delta Lake - this handles JAR dependencies automatically
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    yield spark

    spark.stop()


@pytest.fixture(scope="function")
def spark(spark_session):
    """
    Function-scoped SparkSession that clears catalog between tests for isolation.
    """
    # Clear any tables created in previous tests
    for db in spark_session.catalog.listDatabases():
        for table in spark_session.catalog.listTables(db.name):
            spark_session.sql(f"DROP TABLE IF EXISTS {db.name}.{table.name}")

    yield spark_session


@pytest.fixture
def temp_delta_table(spark, tmp_path):
    """
    Creates a temporary Delta table for testing.

    Yields:
        tuple: (table_name, table_path)
    """
    table_path = str(tmp_path / "delta_table")
    table_name = "test_dimension_table"

    yield table_name, table_path

    # Cleanup
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    if tmp_path.exists():
        shutil.rmtree(tmp_path, ignore_errors=True)


@pytest.fixture
def sample_bronze_df(spark):
    """
    Create a sample bronze DataFrame with LoadTimestamp.

    Returns DataFrame with columns: [id, name, value, LoadTimestamp]
    """
    data = [
        (1, "Alice", "ValueA", "2024-01-01 10:00:00"),
        (2, "Bob", "ValueB", "2024-01-01 10:00:00"),
        (3, "Charlie", "ValueC", "2024-01-01 10:00:00"),
    ]
    return spark.createDataFrame(data, ["id", "name", "value", "LoadTimestamp"])


@pytest.fixture
def sample_dimension_df(spark):
    """
    Create a sample dimension DataFrame with SCD2 metadata columns.

    Returns DataFrame with SCD2 columns including surrogate key, effective dates, etc.
    """
    data = [
        (1, 1, "Alice", "ValueA", "hash1", "2024-01-01", None, 1),
        (2, 2, "Bob", "ValueB", "hash2", "2024-01-01", None, 1),
    ]
    schema = ["customer_sk", "id", "name", "value", "row_hash",
              "Effective_Start_Date", "Effective_End_Date", "Is_Current"]

    df = spark.createDataFrame(data, schema)
    return df.withColumn("Effective_Start_Date", col("Effective_Start_Date").cast("date")) \
             .withColumn("Effective_End_Date", col("Effective_End_Date").cast("date"))


@pytest.fixture
def empty_dimension_schema(spark):
    """
    Returns an empty DataFrame with proper SCD2 schema structure.
    """
    schema = ["customer_sk", "id", "name", "value", "row_hash",
              "Effective_Start_Date", "Effective_End_Date", "Is_Current"]
    return spark.createDataFrame([], schema)


@pytest.fixture
def type2_processor_factory(spark):
    """
    Factory function to create Type2Dimension instances with injected SparkSession.

    Returns:
        Callable that creates Type2Dimension with default parameters
    """
    from SCD2 import Type2Dimension

    def _create_processor(bronze_df, dimension_df, table_name, hash_cols, pk_col, sk_col):
        return Type2Dimension(
            spark=spark,
            bronzeDF=bronze_df,
            dimensionDF=dimension_df,
            dimensionTableName=table_name,
            hashColumnList=hash_cols,
            primaryKeyColumnName=pk_col,
            surrogateKeyColumnName=sk_col
        )

    return _create_processor


@pytest.fixture
def assert_dataframes_equal():
    """
    Utility for comparing DataFrames in tests.

    Returns a function that asserts two DataFrames are equal.
    """
    try:
        from chispa.dataframe_comparer import assert_df_equality
        return assert_df_equality
    except ImportError:
        # Fallback if chispa not available
        def _compare(df1, df2, **kwargs):
            assert df1.collect() == df2.collect(), "DataFrames are not equal"
        return _compare
