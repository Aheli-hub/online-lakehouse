from spark_session import get_spark
from logger import get_logger

from readers.metadata_reader import (
    load_silver_config,
    load_schema_config
)

from readers.bronze_reader import read_bronze

from writers.silver_writer import write_silver

logger = get_logger()

spark = get_spark()

logger.info("=" * 60)
logger.info("Silver Pipeline Started")
logger.info("=" * 60)

# Load Metadata
silver_config = load_silver_config(spark)

schema_config = load_schema_config(spark)

for cfg in silver_config.collect():

    table_name = cfg["table_name"]

    bronze_path = cfg["bronze_path"]

    silver_path = cfg["silver_path"]

    primary_key = cfg["primary_key"]

    load_type = cfg["load_type"]

    logger.info(f"Processing : {table_name}")

    # Read Bronze
    df = read_bronze(

        spark,

        bronze_path

    )

    # ------------------------------
    # Transformations
    # (Next Phase)
    # ------------------------------

    write_silver(

        spark=spark,

        df=df,

        silver_path=silver_path,

        load_type=load_type,

        primary_key=primary_key

    )

logger.info("=" * 60)
logger.info("Silver Pipeline Completed")
logger.info("=" * 60)

spark.stop()