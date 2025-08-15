from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def raw_source_cleansed(input_dataframe: DataFrame, input_table_type: str) -> DataFrame:
    """
    Cleansed Raw Source Dataframe
        Rename Source Columns As per Compatibility with Delta Tables Naming
    :param input_dataframe:
    :param input_table_type:
    :return:
    """
    output_dataframe: DataFrame = None
    match input_table_type:
        case "products":
            output_dataframe = input_dataframe.withColumns(
                {
                    "product_id": input_dataframe["Product ID"],
                    "category": input_dataframe["Category"],
                    "sub_category": input_dataframe["Sub-Category"],
                    "product_name": input_dataframe["Product Name"],
                    "state": input_dataframe["State"],
                    "price_per_product": input_dataframe["Price per product"]
                }
            ).select(
                col("product_id"),
                col("category"),
                col("sub_category"),
                col("product_name"),
                col("state"),
                col("price_per_product")
            )

        case "customers":
            output_dataframe = input_dataframe.withColumns(
                {
                    "customer_id": input_dataframe["Customer ID"],
                    "customer_name": input_dataframe["Customer Name"],
                    "email": input_dataframe["email"],
                    "phone": input_dataframe["phone"],
                    "address": input_dataframe["address"],
                    "segment": input_dataframe["Segment"],
                    "city": input_dataframe["City"],
                    "state": input_dataframe["State"],
                    "country": input_dataframe["Country"],
                    "postal_code": input_dataframe["Postal Code"],
                    "region": input_dataframe["Region"]
                }
            ).select(
                col("customer_id"),
                col("customer_name"),
                col("email"),
                col("phone"),
                col("address"),
                col("segment"),
                col("city"),
                col("state"),
                col("country"),
                col("postal_code"),
                col("region")
            )

        case "orders":
            output_dataframe = input_dataframe.withColumns(
                {
                    "row_id": input_dataframe["Row ID"],
                    "order_id": input_dataframe["Order ID"],
                    "order_date": input_dataframe["Order Date"],
                    "ship_date": input_dataframe["Ship Date"],
                    "ship_mode": input_dataframe["Ship Mode"],
                    "customer_id": input_dataframe["Customer ID"],
                    "product_id": input_dataframe["Product ID"],
                    "quantity": input_dataframe["Quantity"],
                    "price": input_dataframe["Price"],
                    "discount": input_dataframe["Discount"],
                    "profit": input_dataframe["Profit"]
                }
            ).select(
                col("row_id"),
                col("order_id"),
                col("order_date"),
                col("ship_date"),
                col("ship_mode"),
                col("customer_id"),
                col("product_id"),
                col("quantity"),
                col("price"),
                col("discount"),
                col("profit")
            )

    return output_dataframe