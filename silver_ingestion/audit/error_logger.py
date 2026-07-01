from datetime import datetime

from pyspark.sql import Row

from silver_ingestion.config import (
    PROJECT_ID,
    METADATA_DATASET
)

from silver_ingestion.logger import get_logger

logger = get_logger()

# ==========================================================
# ERROR LOGGER
# ==========================================================

def log_error(

    spark,
    run_id,
    table_name,
    error_message

):

    error = [

        Row(

            run_id=run_id,

            table_name=table_name,

            error_message=str(error_message),

            error_time=datetime.now()

        )

    ]

    (

        spark.createDataFrame(error)

        .write

        .format("bigquery")

        .mode("append")

        .option(

            "table",

            f"{PROJECT_ID}.{METADATA_DATASET}.silver_errors"

        )

        .save()

    )

    logger.error(

        f"Error Logged : {table_name}"
    )