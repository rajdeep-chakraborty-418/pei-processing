from pyspark.sql import SparkSession, DataFrame

from src.main.utils.constants import (
    DELTA_TABLE_FORMAT,
    DELTA_TABLE_MODE,
    CATALOG_NAME,
    SCHEMA_NAME,
)


class Writer:
    """
    Write Class to Write Dataframes in tables
    """
    def __init__(self, spark: SparkSession):
        """
        Initializes Writer with Spark Session
        :param spark:
        """
        self.spark = spark
        self.schema = f"{CATALOG_NAME}.{SCHEMA_NAME}"
        self.format = DELTA_TABLE_FORMAT
        self.mode  = DELTA_TABLE_MODE

    def write_delta(self, input_dataframe: DataFrame, input_table_name:str):
        """
        Write Dataframe in Databricks Delta Table
        :param input_dataframe:
        :param input_table_name:
        :return:
        """
        input_table_name: str = f"{self.schema}.{input_table_name}"
        input_dataframe.write.format(self.format).mode(self.mode).saveAsTable(input_table_name)
