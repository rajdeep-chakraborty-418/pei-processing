from pyspark.sql import DataFrame
from pyspark.sql.functions import round as spark_round

from src.main.utils.constants import ROUND_DIGITS


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
    ).withColumn(
        "total_profit",
        spark_round("total_profit", ROUND_DIGITS)
    )

    return output_dataframe
