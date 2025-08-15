import unittest

from pyspark.sql import SparkSession
from src.main.cleaner.cleansed import raw_source_cleansed


class TestCleansed(unittest.TestCase):
    """
    Test Clean Raw Source Dataframe
    """
    def setUp(self):
        """
        Setup Initialisation Parameters
        """
        self.spark = SparkSession.builder.appName("TestCleaner").master("local[*]").getOrCreate()
        """
        Setup Raw Source Data to Rename Columns
        """
        self.raw_product_data = [
            {
                "Product ID": "Product-1",
                "Category": "Category 1",
                "Sub-Category": "Sub-Category 1",
                "Product Name": "Product A",
                "State": "State 1",
                "Price per product": 100.0
            },
        ]
        self.raw_customer_data = [
            {
                "Customer ID": "Customer-1",
                "Customer Name": "Customer A",
                "email": "abc@xyz.com",
                "phone": "000000000",
                "address": "Address 1",
                "Segment": "Segment 1",
                "City": "City 1",
                "State": "State 1",
                "Country": "Country 1",
                "Postal Code": "123456",
                "Region": "Region 1"
            }
        ]
        self.raw_order_data = [
            {
                "Row ID": "Row 1",
                "Order ID": "Order-1",
                "Order Date": "15/8/2025",
                "Ship Date": "15/8/2025",
                "Ship Mode": "Ship Mode 1",
                "Customer ID": "Customer-1",
                "Product ID": "Product-1",
                "Quantity": 2,
                "Price": 100.0,
                "Discount": 0.1,
                "Profit": 20.34567
            },
        ]
        self.input_products_dataframe = self.spark.createDataFrame(self.raw_product_data)
        self.input_customers_dataframe = self.spark.createDataFrame(self.raw_customer_data)
        self.input_orders_dataframe = self.spark.createDataFrame(self.raw_order_data)

    def tearDown(self):
        """
        Stop Spark Session
        """
        self.spark.stop()

    def test_raw_source_cleansed(self):
        """
        Test Cleansed Logic
        :return:
        """
        test_cases = [
            ("products", self.input_products_dataframe, [
                "product_id", "category", "sub_category", "product_name", "state", "price_per_product"]
             ),
            ("customers", self.input_customers_dataframe, [
                "customer_id", "customer_name", "email", "phone", "address",
                "segment", "city", "state", "country", "postal_code", "region"]
             ),
            ("orders", self.input_orders_dataframe, [
                "row_id", "order_id", "order_date", "ship_date", "ship_mode",
                "customer_id", "product_id", "quantity", "price", "discount", "profit"]
             ),
        ]
        for test_source_type, test_input_dataframe, test_expected_columns in test_cases:
            with self.subTest(test_source_type=test_source_type, test_input_dataframe=test_input_dataframe, test_expected_columns=test_expected_columns):
                cleansed_df = raw_source_cleansed(test_input_dataframe, test_source_type)
                self.assertEqual(cleansed_df.columns, test_expected_columns)
