from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when

class Type1Dimension:
    """
    A class to handle Type 1 Dimension processing using Spark DataFrames.
    """

    def __init__(self, source_df: DataFrame, target_df: DataFrame, key_columns: list, update_columns: list):
        """
        Initialize the processor with source and target dataframes.

        :param source_df: The source Spark DataFrame containing new data.
        :param target_df: The target Spark DataFrame containing existing data.
        :param key_columns: List of columns used to match records between source and target.
        :param update_columns: List of columns to update in the target dataframe.
        """
        self.source_df = source_df
        self.target_df = target_df
        self.key_columns = key_columns
        self.update_columns = update_columns

    def ProcessUpdate(self) -> DataFrame:
        """
        Perform Type 1 Dimension processing.

        :return: Updated target Spark DataFrame.
        """
        # Perform an outer join between source and target dataframes on key columns
        merged_df = self.target_df.join(
            self.source_df,
            on=self.key_columns,
            how='outer'
        )

        # Update records in the target dataframe with source data
        for column in self.update_columns:
            merged_df = merged_df.withColumn(
                column,
                when(col(f"{column}").isNotNull(), col(f"{column}")).otherwise(col(f"{column}_source"))
            )

        # Select only the key columns and updated columns
        updated_target_df = merged_df.select(self.key_columns + self.update_columns).dropDuplicates()

        return updated_target_df
