import unittest
from unittest.mock import MagicMock, patch

from pyspark.sql import DataFrame

from src.main.reader.reader_options import (
    CSV_OPTIONS,
    JSON_OPTIONS,
)
from src.main.reader.reader import Reader
from src.main.reader.source_schema import (
    ORDERS_SCHEMA,
    PRODUCTS_SCHEMA,
    CUSTOMERS_SCHEMA,
)


class TestReader(unittest.TestCase):
    """
    Test Class for Reader Module
    """
    def setUp(self):
        """
        Setup Common Params
        :return:
        """
        self.mock_dataframe = MagicMock(spec=DataFrame)
        self.mock_spark = MagicMock()
        self.mock_file_path = "/mock/input_path"
        self.reader_instance = Reader(
            spark=self.mock_spark,
            file_path= self.mock_file_path
        )

    @patch('src.main.reader.reader.Reader.read_file')
    def test_read(self, mock_read_file, ):
        """
        Test Reads the content of the file
        :return:
        """
        mock_read_file.return_value = self.mock_dataframe

        test_scenario = [
            ("csv", "Products.csv", PRODUCTS_SCHEMA, CSV_OPTIONS),
            ("json", "Orders.json", ORDERS_SCHEMA, JSON_OPTIONS),
            ("csv", "Customers.csv", CUSTOMERS_SCHEMA, CSV_OPTIONS)
        ]
        for test_file_type, test_file_name, test_schema, test_options in test_scenario:
            with self.subTest(file_type=test_file_type, file_name=test_file_name, test_schema=test_schema, test_options=test_options):
                result_df = self.reader_instance.read(test_file_type, test_file_name)
                self.assertEqual(result_df, self.mock_dataframe)
                loop_file_path = self.mock_file_path + "/" + test_file_name
                mock_read_file.assert_called_with(
                    input_format_typ=test_file_type,
                    input_file_path=loop_file_path,
                    input_schema=test_schema,
                    input_options=test_options
                )

    def test_read_file(self):
        """
        Test Read CSV, JSON File with specified Schema and Options
        :return:
        """
        mock_read = MagicMock()
        mock_format = MagicMock()
        mock_options = MagicMock()
        mock_schema = MagicMock()
        mock_load = MagicMock(spec=DataFrame)

        self.mock_spark.read=mock_read
        mock_read.format.return_value = mock_format
        mock_format.options.return_value = mock_options
        mock_options.schema.return_value = mock_schema
        mock_schema.load.return_value = mock_load

        mock_file_schema = MagicMock()
        actual_df = self.reader_instance.read_file(
            input_format_typ="dummy",
            input_file_path=self.mock_file_path,
            input_schema=mock_file_schema,
            input_options={"key": "value"}
        )
        self.mock_spark.read.format.assert_called_with("dummy")
        mock_format.options.assert_called_with(**{"key": "value"})
        mock_options.schema.assert_called_with(mock_file_schema)
        mock_schema.load.assert_called_with(self.mock_file_path)
        self.assertEqual(actual_df, mock_load)
