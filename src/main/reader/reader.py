import pandas as pd
from pyspark.sql import SparkSession, DataFrame

from src.main.enums.input_file_type import InputFileType
from src.main.reader.options.reader_options import (
    CSV_OPTIONS,
    JSON_OPTIONS,
    EXCEL_OPTIONS,
)
from src.main.reader.schema.source_schema import (
    PRODUCTS_SCHEMA,
    ORDERS_SCHEMA,
    CUSTOMERS_SCHEMA,
)


class Reader:
    """
    Reader Class to Read Files
    """
    def __init__(self, spark: SparkSession, file_path: str):
        """
        Initializes Reader with file path
        :param file_path:
        """
        self.spark = spark
        self.file_path = file_path

    def read(self, input_file_typ: str, input_file_name: str) -> DataFrame:
        """
        Reads the content of the file
        :param input_file_typ:
        :param input_file_name:
        :return:
        """
        file_path = self.file_path+"/"+input_file_name
        match input_file_typ:
            case InputFileType.CSV.value:
                output_df = self.read_file(
                    input_format_typ=input_file_typ,
                    input_file_path=file_path,
                    input_schema=PRODUCTS_SCHEMA,
                    input_options=CSV_OPTIONS
                )
            case InputFileType.JSON.value:
                output_df = self.read_file(
                    input_format_typ=input_file_typ,
                    input_file_path=file_path,
                    input_schema=ORDERS_SCHEMA,
                    input_options=JSON_OPTIONS
                )
            case InputFileType.EXCEL.value:
                output_df = self.read_excel(
                    input_file_path=file_path,
                    input_schema=CUSTOMERS_SCHEMA,
                    input_sheet_name=EXCEL_OPTIONS['sheetName']
                )

        return output_df

    def read_file(self, input_format_typ: str, input_file_path: str, input_schema, input_options: dict):
        """
        Read CSV, JSON File with specified Schema and Options
        :param input_format_typ:
        :param input_file_path:
        :param input_schema:
        :param input_options:
        :return:
        """
        return (
            self.spark.read.format(input_format_typ).options(**input_options).schema(input_schema).load(input_file_path)
        )

    def read_excel(self, input_file_path: str, input_schema, input_sheet_name: str) -> DataFrame:
        """
        Read Excel File with Specified Schema
        :param input_file_path:
        :param input_schema:
        :param input_sheet_name:
        :return:
        """
        pandas_dataframe = pd.read_excel(sheet_name=input_sheet_name,io=input_file_path)
        return self.spark.createDataFrame(
            pandas_dataframe,
            schema=input_schema
        )
