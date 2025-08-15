CATALOG_NAME: str = "pipeline"
SCHEMA_NAME: str = "pei"
DATABRICKS_INPUT_PATH: str = "/Volumes/pipeline/pei/artifacts/input"

DELTA_TABLE_FORMAT: str = "delta"
DELTA_TABLE_MODE: str = "overwrite"
LOCAL_INPUT_FOLDER: str = "source_data"

CUSTOMER_FILE_NAME: str = "Customer.xlsx"
PRODUCT_FILE_NAME: str = "Products.csv"
ORDER_FILE_NAME: str = "Orders.json"

PRODUCT_KEY_NAME: str = "products"
ORDER_KEY_NAME: str = "orders"
CUSTOMER_KEY_NAME: str = "customers"
YEAR_CAT_SUB_CAT_CUST_KEY_NAME: str = "year_cat_sub_cat_cust"

DATAFRAME_KEY_NAME: str = "dataframe"
TABLE_LAYER_KEY_NAME: str = "table_layer"

RAW_KEY_NAME: str = "raw"
ENRICHED_KEY_NAME: str = "enriched"
AGGREGATE_KEY_NAME: str = "aggregate"

ROUND_DIGITS: int = 2
