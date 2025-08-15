import unittest

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from src.main.transformer.enriched import raw_source_enrichment, custom_enrichment


class TestEnriched(unittest.TestCase):
    """
    Test Enrich Raw Source Dataframe
    """
    def setUp(self):
        """
        Setup Initialisation Parameters
        """
        self.spark = SparkSession.builder.appName("TestTransformation").master("local[*]").getOrCreate()
        """
        Setup Raw Source Data Covering All Possible Scenarios
        - Duplicate Records
        - Blank Records
        - Null Records
        - Valid Records
        - For Orders Year Conversion from Order Date
        """
        self.raw_product_data = [
            {"product_id": "Product-1", "product_name": "Product A", "category": "Category 1", "sub_category": "Sub-Category 1"},
            {"product_id": "Product-1", "product_name": "Product A", "category": "Category 1", "sub_category": "Sub-Category 1"},
            {"product_id": "", "product_name": "Product Blank", "category": "Category Blank", "sub_category": "Sub-Category Blank"},
            {"product_id": None, "product_name": "Product Blank", "category": "Category Blank", "sub_category": "Sub-Category Blank"},
        ]
        self.raw_customer_data = [
            {"customer_id": "Customer-1", "customer_name": "Customer A", "country": "Country 1"},
            {"customer_id": "Customer-1", "customer_name": "Customer A", "country": "Country 1"},
            {"customer_id": "", "customer_name": "Customer Blank", "country": "Customer Blank"},
            {"customer_id": None, "customer_name": "Customer Blank", "country": "Customer Blank"},
        ]
        self.raw_order_data = [
            {"order_id": "Order-1",  "order_date": "15/8/2025", "customer_id": "Customer-1", "product_id": "Product-1", "quantity": 2, "price": 100.0, "discount": 0.1, "profit": 20.34567},
        ]
        self.input_products_dataframe = self.spark.createDataFrame(self.raw_product_data)
        self.input_customers_dataframe = self.spark.createDataFrame(self.raw_customer_data)
        self.input_orders_dataframe = self.spark.createDataFrame(self.raw_order_data)

    def tearDown(self):
        """
        Stop Spark Session
        """
        self.spark.stop()

    def test_raw_source_enrichment(self):
        """
        Test Enrichment Logic
        :return:
        """
        test_cases = [
            ("products", self.input_products_dataframe, 1, ["product_id", "product_name", "category", "sub_category"]),
            ("customers", self.input_customers_dataframe, 1, ["customer_id", "customer_name", "country"]),
            ("orders", self.input_orders_dataframe, 1, ["order_id", "order_date", "year", "customer_id", "product_id", "quantity", "price", "discount", "profit"]),
        ]
        for test_source_type, test_input_dataframe, test_expected_count, test_expected_columns in test_cases:
            with self.subTest(test_source_type=test_source_type, test_input_dataframe=test_input_dataframe, test_expected_count=test_expected_count, test_expected_columns=test_expected_columns):
                enriched_df = raw_source_enrichment(test_input_dataframe, test_source_type)
                self.assertEqual(enriched_df.count(), test_expected_count)
                self.assertEqual(enriched_df.columns, test_expected_columns)
                if test_source_type == "orders":
                    """
                    Specific Validation For Orders
                        Year Conversion
                        Rounding in Profit
                    """
                    self.assertEqual(enriched_df.select(col("year")).collect()[0][0], "2025")
                    self.assertEqual(enriched_df.select(col("profit")).collect()[0][0], 20.35)

    def test_custom_enrichment(self):
        """
        Test Custom Enrichment Logic
        :return:
        """
        input_products_enriched_df = raw_source_enrichment(self.input_products_dataframe, "products")
        input_customers_enriched_df = raw_source_enrichment(self.input_customers_dataframe, "customers")
        input_orders_enriched_df = raw_source_enrichment(self.input_orders_dataframe, "orders")
        enriched_dataframe = custom_enrichment(
            input_products_dataframe=input_products_enriched_df,
            input_customers_dataframe=input_customers_enriched_df,
            input_orders_dataframe=input_orders_enriched_df
        )
        self.assertEqual(enriched_dataframe.count(), 1)
        self.assertEqual(enriched_dataframe.columns, [
            "order_id",
            "order_date",
            "year",
            "customer_id",
            "product_id",
            "quantity",
            "price",
            "discount",
            "profit",
            "customer_name",
            "customer_country",
            "category",
            "sub_category"
        ])
