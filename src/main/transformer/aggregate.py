from pyspark.sql import DataFrame


def custom_aggregate(input_dataframe: DataFrame) -> DataFrame:
    """
    Create an aggregate table that shows profit by
        Year
        Product Category
        Product Sub Category
        Customer
    :param input_dataframe:
    :return:
    """
    output_dataframe: DataFrame = input_dataframe.groupBy(
        "year",
        "category",
        "sub_category",
        "customer_name"
    ).sum("profit").withColumnRenamed(
        "sum(profit)", "total_profit"
    )

    return output_dataframe
