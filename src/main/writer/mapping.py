from src.main.utils.constants import (
    PRODUCT_KEY_NAME,
    ORDER_KEY_NAME,
    CUSTOMER_KEY_NAME,
    YEAR_CAT_SUB_CAT_CUST_KEY_NAME,
    RAW_KEY_NAME,
    ENRICHED_KEY_NAME,
    AGGREGATE_KEY_NAME,
)

TABLE_MAPPING: dict = {
    PRODUCT_KEY_NAME: {
        RAW_KEY_NAME:"products_raw",
        ENRICHED_KEY_NAME:"products_enriched",
    },
    ORDER_KEY_NAME: {
        RAW_KEY_NAME:"orders_raw",
        ENRICHED_KEY_NAME:"orders_custom_enriched",
    },
    CUSTOMER_KEY_NAME: {
        RAW_KEY_NAME:"customers_raw",
        ENRICHED_KEY_NAME:"customers_enriched",
    },
    YEAR_CAT_SUB_CAT_CUST_KEY_NAME: {
        AGGREGATE_KEY_NAME: "year_cat_sub_cat_cust_aggregate"
    }
}

def get_table_name(input_table_type: str, input_table_layer: str) -> str:
    """
    Get Full Qualified Table Name
    :param input_table_type:
    :param input_table_layer:
    :return:
    """
    return TABLE_MAPPING[input_table_type][input_table_layer]
