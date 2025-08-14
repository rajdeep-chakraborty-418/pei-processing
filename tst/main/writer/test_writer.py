import unittest
from unittest.mock import MagicMock

from src.main.writer.writer import Writer


class TestWriter(unittest.TestCase):
    """
    Test Writer Module
    """
    def setUp(self):
        """
        Setup Common Parameters
        :return:
        """
        self.spark = MagicMock(spec='SparkSession')
        self.writer = Writer(spark=self.spark)
        self.writer.format = "Test-Delta"
        self.mock_dataframe = MagicMock(spec='DataFrame')

    def test_write_delta(self):
        """
        Test Write Dataframe in Databricks Delta Table
        :return:
        """
        mock_write = MagicMock()
        mock_format = MagicMock()
        mock_mode = MagicMock()

        self.mock_dataframe.write = mock_write
        mock_write.format.return_value = mock_format
        mock_format.mode.return_value = mock_mode
        mock_mode.saveAsTable.return_value = None

        self.writer.write_delta(
            input_dataframe=self.mock_dataframe,
            input_table_name="test_table"
        )
        mock_write.format.assert_called_with(self.writer.format)
        mock_format.mode.assert_called_with("overwrite")
        mock_mode.saveAsTable.assert_called_with("test_table")
        self.assertTrue(mock_mode.saveAsTable.called)
