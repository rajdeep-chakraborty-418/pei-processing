from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, round as spark_round, split, row_number

from src.main.utils.constants import ROUND_DIGITS


def raw_source_enrichment(input_dataframe: DataFrame, input_table_type: str) -> DataFrame:
    """
    Enrich Raw Source Dataframe
        Select Only required Columns and Rename to lower case
        Drop All Rest Columns
        Null Check on Primary Key Columns
        Remove Duplicates based on Primary Key Columns
    :param input_dataframe
    :param input_table_type
    :return:
    """
    """
    Defining Duplicate Window Logic
    """
    product_duplicate_window = Window.partitionBy("product_id").orderBy("product_name")
    customer_duplicate_window = Window.partitionBy("customer_id").orderBy("customer_name")

    output_dataframe: DataFrame = input_dataframe
    match input_table_type:
        case "products":
            output_dataframe = input_dataframe.withColumns(
                {
                    "product_id": input_dataframe["Product ID"],
                    "product_name": input_dataframe["Product Name"],
                    "category": input_dataframe["Category"],
                    "sub_category": input_dataframe["Sub-Category"]
                }
            ).select(
                col("product_id"),
                col("product_name"),
                col("category"),
                col("sub_category")
            ).filter(
                col("product_id").isNotNull()
            ).withColumn(
                "row_num", row_number().over(product_duplicate_window)
            ).filter(
                col("row_num") == 1
            ).drop(*["row_num"])

        case "customers":
            output_dataframe = input_dataframe.withColumns(
                {
                    "customer_id": input_dataframe["Customer ID"],
                    "customer_name": input_dataframe["Customer Name"],
                    "country": input_dataframe["Country"],
                }
            ).select(
                col("customer_id"),
                col("customer_name"),
                col("country")
            ).filter(
                col("customer_id").isNotNull()
            ).withColumn(
                "row_num", row_number().over(customer_duplicate_window)
            ).filter(
                col("row_num") == 1
            ).drop(*["row_num"])

        case "orders":
            output_dataframe = input_dataframe.withColumns(
                {
                    "order_id": input_dataframe["Order ID"],
                    "order_date": input_dataframe["Order Date"],
                    "customer_id": input_dataframe["Customer ID"],
                    "product_id": input_dataframe["Product ID"],
                    "quantity": input_dataframe["Quantity"],
                    "price": input_dataframe["Price"],
                    "discount": input_dataframe["Discount"],
                    "profit": input_dataframe["Profit"]
                }
            ).select(
                col("order_id"),
                col("order_date"),
                split(col("order_date"), "/")[2].alias("year"),
                col("customer_id"),
                col("product_id"),
                col("quantity"),
                col("price"),
                col("discount"),
                spark_round(
                    col("profit"),
                    ROUND_DIGITS
                ).alias("profit")
            )

    return output_dataframe

def custom_enrichment(input_order_dataframe: DataFrame, input_customer_dataframe: DataFrame, input_products_dataframe: DataFrame) -> DataFrame:
    """
    Create an enriched table which has
        order information
        Profit rounded to 2 decimal places
        Customer name and country
        Product category and sub category
    :param input_order_dataframe:
    :param input_customer_dataframe:
    :param input_products_dataframe:
    :return:
    """
    output_dataframe: DataFrame = input_order_dataframe.alias("order").join(
        input_customer_dataframe.alias("customer"),
        on="customer_id",
        how="left"
    ).join(
        input_products_dataframe.alias("products"),
        on="product_id",
        how="left"
    ).select(
        col("order.*"),
        col("customer.customer_name").alias("customer_name"),
        col("customer.country").alias("customer_country"),
        col("products.category").alias("category"),
        col("products.sub_category").alias("sub_category")
    )

    return output_dataframe
