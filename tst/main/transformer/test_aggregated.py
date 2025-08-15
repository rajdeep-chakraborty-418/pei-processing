import unittest

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from src.main.transformer.aggregate import custom_aggregate


class TestAggregated(unittest.TestCase):
    """
    Test Custom Aggregate
    """
    def setUp(self):
        """
        Set up common attributes
        :return:
        """
        """
        Generate Enriched Dataframe covering scenarios
            Multiple Year
            Multiple Category
            Multi Customer
        """
        self.spark = SparkSession.builder.appName("TestTransformation").master("local[*]").getOrCreate()
        self.enriched_data = [
            {"year": "2025", "category": "Category 1", "sub_category": "Sub-Category 1", "customer_name": "Customer A", "profit": 100.0},
            {"year": "2025", "category": "Category 1", "sub_category": "Sub-Category 1", "customer_name": "Customer A", "profit": 50.0},
            {"year": "2024", "category": "Category 2", "sub_category": "Sub-Category 2", "customer_name": "Customer B", "profit": 200.0},
            {"year": "2024", "category": "Category 2", "sub_category": "Sub-Category 2", "customer_name": "Customer C", "profit": 130.0}
        ]
        self.enriched_dataframe = self.spark.createDataFrame(self.enriched_data)

    def tearDown(self):
        """
        Stop Spark Session
        """
        self.spark.stop()

    def test_custom_aggregate(self):
        """
        Test Custom Aggregate Logic
        :return:
        """
        output_dataframe = custom_aggregate(
            input_dataframe=self.enriched_dataframe
        )
        self.assertEqual(output_dataframe.count(), 3)
        self.assertEqual(output_dataframe.filter(col("year")=="2025").count(), 1)
        self.assertEqual(output_dataframe.filter(col("year")=="2025").select(col("total_profit")).collect()[0][0], 150)
        self.assertEqual(output_dataframe.filter(col("year")=="2024").count(), 2)
