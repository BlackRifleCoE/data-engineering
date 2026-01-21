# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sha2, concat_ws, current_date, lit, max as spark_max, row_number, to_date
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from typing import List, Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Type2Dimension:
    def __init__(self, spark: SparkSession, bronzeDF: DataFrame, dimensionDF: Optional[DataFrame], dimensionTableName: str, hashColumnList: List[str], primaryKeyColumnName: str, surrogateKeyColumnName: str):

        # Store SparkSession
        self.spark = spark

        # Initialize the bronze DataFrame
        self.bronzeDataFrame = bronzeDF
        self.latest_batch_timestamp = ""
        self.dimensionDataFrame: Optional[DataFrame] = None

        # Check if dimensionDF is empty, if so create an empty DataFrame with the required schema
        if dimensionDF is None:
            self.dimensionDataFrame = self.spark.createDataFrame([], bronzeDF.schema
                                            .add(f"{surrogateKeyColumnName}", "long")
                                            .add("row_hash", "string")
                                            .add("Effective_Start_Date", "date")
                                            .add("Effective_End_Date", "date")
                                            .add("Is_Current", "integer"))
            logger.info("ProcessSCD2 initializer : dimension dataframe is empty, creating empty dataframe based on bronze")
        else:
            # If dimensionDF is not empty, use the provided DataFrame
            self.dimensionDataFrame = dimensionDF
            # Find the latest batch timestamp and convert to date for Effective_End_Date
            latest_ts = self.bronzeDataFrame.agg(spark_max(col("LoadTimestamp").cast("timestamp")).alias("latest_timestamp")).collect()[0]["latest_timestamp"]
            self.latest_batch_timestamp = latest_ts.date() if latest_ts is not None else None

        # Initialize other class variables
        self.columnList = hashColumnList
        self.PKColumn = primaryKeyColumnName
        self.SKColumn = surrogateKeyColumnName
        self.dimensionTableName = dimensionTableName
        self.dimensionDataFrameCurrent: Optional[DataFrame] = None
        self.updatedCount = 0
        logger.info("ProcessSCD2 initialized")

    def generate_hash(self, df: DataFrame) -> DataFrame:
        # Generate a hash for the specified columns
        logger.info("Generating hash for DataFrame")
        return df.withColumn("row_hash", sha2(concat_ws("||", *[col(c) for c in self.columnList]), 256))

    def ProcessUpdate(self) -> int:
        # Generate hash for bronze DataFrame
        self.bronzeDF = self.generate_hash(self.bronzeDataFrame)

        # Get current active records from dimension table
        self.dimensionDataFrameCurrent = self.dimensionDataFrame.filter(col("Is_Current") == 1).select(f"{self.PKColumn}", "row_hash")

        # Join to identify new or changed records
        staging_df = self.bronzeDF.alias("b").join(
            self.dimensionDataFrameCurrent.alias("d"),
            on=self.PKColumn,
            how="left"
        ).select(
            "b.*",
            col("d.row_hash").alias("ExistingRowHash")
        )

        # Identify new or updated records
        changes_df = staging_df.filter((col("ExistingRowHash").isNull()) | (col("ExistingRowHash") != col("row_hash")))

        # Proceed only if there are changes
        if changes_df.count() > 0:
            logger.info("Changes detected")

            # Prepare updates (set current records to historical)
            updates_df = changes_df.filter(col("ExistingRowHash").isNotNull())

            # Find the number of updates
            self.updatedCount = updates_df.count()

            if self.updatedCount > 0:
                dim_table = DeltaTable.forName(self.spark, self.dimensionTableName)
                dim_table.alias("dim").merge(
                    updates_df.alias("updates"),
                    f"dim.{self.PKColumn} = updates.{self.PKColumn} AND dim.Is_Current = 1"
                ).whenMatchedUpdate(set={
                    "Effective_End_Date": lit(self.latest_batch_timestamp),
                    "Is_Current": "0"
                }).execute()

                logger.info(f"Records updated: {self.updatedCount}")

            # Get current max surrogate key to start incrementing
            max_id_row = self.dimensionDataFrame.select(spark_max(self.SKColumn).alias("maxID")).collect()[0]
            next_id = max_id_row["maxID"] + 1 if max_id_row["maxID"] else 1

            # Add auto-incrementing surrogate key to new records using row_number
            window_spec = Window.orderBy(self.PKColumn)
            new_records_df = changes_df.withColumn(
                self.SKColumn, row_number().over(window_spec) + lit(next_id) - lit(1)
            ).withColumn(
                "Effective_Start_Date", col("LoadTimestamp").cast("date")
            ).withColumn(
                "Effective_End_Date", lit(None).cast("date")
            ).withColumn(
                "Is_Current", lit(1)
            ).drop("LoadTimestamp", "ExistingRowHash")

            # Reorder columns to match dimension table (Risk_SK first)
            dim_cols = [self.SKColumn] + [c for c in new_records_df.columns if c != self.SKColumn]
            new_records_df = new_records_df.select(dim_cols)

            logger.info(f"Records inserted: {new_records_df.count()}")

            # Append new records
            new_records_df.write.format("delta").mode("append").saveAsTable(self.dimensionTableName)
            return new_records_df.count()

        else:
            logger.info("No new or updated records found.")
            return 0
