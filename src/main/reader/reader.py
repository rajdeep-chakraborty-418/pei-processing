from pyspark.sql import SparkSession, DataFrame

from src.main.reader.mapping import INPUT_FILE_TYPE_SCHEMA_OPTIONS_MAP
from src.main.utils.constants import (
    READER_MAPPING_SCHEMA_KEY_NAME,
    READER_MAPPING_OPTIONS_KEY_NAME,
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
        output_df = self.read_file(
            input_format_typ=input_file_typ,
            input_file_path=file_path,
            input_schema=INPUT_FILE_TYPE_SCHEMA_OPTIONS_MAP[input_file_name][READER_MAPPING_SCHEMA_KEY_NAME],
            input_options=INPUT_FILE_TYPE_SCHEMA_OPTIONS_MAP[input_file_name][READER_MAPPING_OPTIONS_KEY_NAME]
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
