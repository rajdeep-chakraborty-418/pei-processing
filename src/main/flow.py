import os

from pyspark.sql import SparkSession, DataFrame

from src.main.enums.input_file_type import InputFileType
from src.main.reader.reader import Reader
from src.main.transformer.aggregate import custom_aggregate
from src.main.transformer.enriched import raw_source_enrichment, custom_enrichment
from src.main.utils import logger_utils
from src.main.utils.constants import (
    LOCAL_INPUT_FOLDER,
    CUSTOMER_FILE_NAME,
    ORDER_FILE_NAME,
    PRODUCT_FILE_NAME,
    PRODUCT_KEY_NAME,
    ORDER_KEY_NAME,
    CUSTOMER_KEY_NAME,
    YEAR_CAT_SUB_CAT_CUST_KEY_NAME,
    DATAFRAME_KEY_NAME,
    TABLE_LAYER_KEY_NAME,
    AGGREGATE_KEY_NAME,
    ENRICHED_KEY_NAME,
    RAW_KEY_NAME, DATABRICKS_INPUT_PATH,
)
from src.main.utils.logger_utils import log_end
from src.main.writer.mapping.mapping import get_table_name, TABLE_MAPPING
from src.main.writer.writer import Writer


def get_local_project_root() -> str:
    """
    Get the Local Root Path where makefile exists
    :return:
    """
    path = os.path.abspath('.')
    while True:
        if os.path.exists(os.path.join(path, 'makefile')):
            return path
        path = os.path.dirname(path)

def write_delta_table_wrapper(input_env: str, writer: Writer, input_write_dict: dict):
    """
    Write Dataframe in Databricks Delta Table For Each Layer
    :param input_env:
    :param writer:
    :param input_write_dict:
    :return:
    """
    if input_env == "local":
        """
        Skip Writing Delta Tables in Local Environment
        """
        return
    for each_element in input_write_dict:
        loop_table_layer: str = input_write_dict[each_element][TABLE_LAYER_KEY_NAME]
        if TABLE_MAPPING[each_element].get(loop_table_layer) is not None:
            input_table_name: str = get_table_name(
                input_table_type=each_element,
                input_table_layer=loop_table_layer
            )
            write_dataframe: DataFrame = input_write_dict[each_element][DATAFRAME_KEY_NAME]
            writer.write_delta(
                input_dataframe=write_dataframe,
                input_table_name=input_table_name
            )


def main():
    """
    Main Flow for the Application Use Cases
    :param: Spark Session
    :return:
    """
    logger = logger_utils.LoggerUtils.setup_logger("PEIAppProcessor")
    start_time = logger_utils.log_start(logger)
    try:
        try:
            spark = (SparkSession.builder
                     .appName("PEIApp")
                     .getOrCreate()
                     )
        except Exception as ex:
            logger.error(f"Spark Session Creation Failed {ex}")
            raise
        """
        Read Execution Environment
        For Local Environment: PEI_ENV -> local
        For Databricks Environment: PEI_ENV -> databricks
        """
        try:
            env: str=os.getenv("PEI_ENV", "databricks")
            local_root: str = get_local_project_root()
            if env == "local":
                """
                Set the Source Folder Containing Input Files
                """
                source_file_path = os.path.join(local_root, LOCAL_INPUT_FOLDER)
            else:
                """
                For Databricks Environment source Folder is set to Volume Root or dbfs
                """
                source_file_path=DATABRICKS_INPUT_PATH
        except Exception as ex:
            logger.error(f"Source Path Construction Failed {ex}")
            raise
        """
        Read Input Files From Pre Defined Source Folder
        """
        try:
            reader_instance = Reader(
                spark=spark,
                file_path=source_file_path
            )
            input_products_dataframe: DataFrame = reader_instance.read(
                input_file_typ=InputFileType.CSV.value,
                input_file_name=PRODUCT_FILE_NAME
            )
            input_orders_dataframe: DataFrame = reader_instance.read(
                input_file_typ=InputFileType.JSON.value,
                input_file_name=ORDER_FILE_NAME
            )
            input_customers_dataframe: DataFrame = reader_instance.read(
                input_file_typ=InputFileType.EXCEL.value,
                input_file_name=CUSTOMER_FILE_NAME
            )
        except Exception as ex:
            logger.error(f"Input File Reading Failed {ex}")
            raise

        try:
            writer_instance = Writer(spark=spark)
            """
            Write Raw Dataframes in Delta Tables
            """
            write_delta_table_wrapper(
                input_env=env,
                writer=writer_instance,
                input_write_dict={
                    PRODUCT_KEY_NAME: {
                        DATAFRAME_KEY_NAME: input_products_dataframe,
                        TABLE_LAYER_KEY_NAME: RAW_KEY_NAME
                    },
                    ORDER_KEY_NAME: {
                        DATAFRAME_KEY_NAME: input_orders_dataframe,
                        TABLE_LAYER_KEY_NAME: RAW_KEY_NAME
                    },
                    CUSTOMER_KEY_NAME: {
                        DATAFRAME_KEY_NAME: input_customers_dataframe,
                        TABLE_LAYER_KEY_NAME: RAW_KEY_NAME
                    }
                }
            )
        except Exception as ex:
            logger.error(f"Raw Dataframe Writing To Delta Failed {ex}")
            raise
        """
        Create Enriched Dataframes
        """
        try:
            enriched_products_dataframe: DataFrame = raw_source_enrichment(
                input_dataframe=input_products_dataframe,
                input_table_type=PRODUCT_KEY_NAME
            )
            enriched_orders_dataframe: DataFrame = raw_source_enrichment(
                input_dataframe=input_orders_dataframe,
                input_table_type=ORDER_KEY_NAME
            )
            enriched_customers_dataframe: DataFrame = raw_source_enrichment(
                input_dataframe=input_customers_dataframe,
                input_table_type=CUSTOMER_KEY_NAME
            )
            custom_enriched_orders_dataframe: DataFrame = custom_enrichment(
                input_order_dataframe=enriched_orders_dataframe,
                input_customer_dataframe=enriched_customers_dataframe,
                input_products_dataframe=enriched_products_dataframe
            )
        except Exception as ex:
            logger.error(f"Dataframe Enrichment Failed {ex}")
            raise
        """
        Write Enriched Dataframes in Delta Tables
        """
        try:
            write_delta_table_wrapper(
                input_env=env,
                writer=writer_instance,
                input_write_dict={
                   PRODUCT_KEY_NAME: {
                        DATAFRAME_KEY_NAME: enriched_products_dataframe,
                        TABLE_LAYER_KEY_NAME: ENRICHED_KEY_NAME
                    },
                    ORDER_KEY_NAME: {
                        DATAFRAME_KEY_NAME: custom_enriched_orders_dataframe,
                        TABLE_LAYER_KEY_NAME: ENRICHED_KEY_NAME
                    },
                    CUSTOMER_KEY_NAME: {
                        DATAFRAME_KEY_NAME: enriched_customers_dataframe,
                        TABLE_LAYER_KEY_NAME: ENRICHED_KEY_NAME
                    }
                }
            )
        except Exception as ex:
            logger.error(f"Enriched Dataframe Writing To Delta Failed {ex}")
            raise
        try:
            """
            Calculate Custom Aggregate Dataframe
            """
            output_aggregate: DataFrame = custom_aggregate(custom_enriched_orders_dataframe)
            # output_aggregate.show(500, truncate=False, vertical=False)
        except Exception as ex:
            logger.error(f"Custom Aggregate Dataframe Creation Failed {ex}")
            raise
        try:
            """
            Write Aggregated Dataframes in Delta Tables
            """
            write_delta_table_wrapper(
                input_env=env,
                writer=writer_instance,
                input_write_dict={
                    YEAR_CAT_SUB_CAT_CUST_KEY_NAME: {
                        DATAFRAME_KEY_NAME: output_aggregate,
                        TABLE_LAYER_KEY_NAME: AGGREGATE_KEY_NAME
                    }
                }
            )
        except Exception as ex:
            logger.error(f"Aggregate Dataframe Writing To Delta Failed {ex}")
            raise
    except Exception as ex:
        logger.error("Main Flow Failed" + " %s", str(ex))
        raise

    finally:
        spark.stop()
        log_end(start_time, logger)


if __name__=='__main__':
    main()
