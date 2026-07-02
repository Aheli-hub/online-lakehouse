from datetime import datetime

from pyspark.sql import Row

from gold_ingestion.config import (

    PIPELINE_ERROR_TABLE

)

from gold_ingestion.logger import get_logger

logger = get_logger()


# ==========================================================
# LOG PIPELINE ERROR
# ==========================================================

def log_error(

    spark,

    run_id,

    table_name,

    error_message

):

    rows = [

        Row(

            run_id=run_id,

            table_name=table_name,

            error_message=error_message,

            error_time=datetime.utcnow()

        )

    ]

    (

        spark.createDataFrame(rows)

        .write

        .format("bigquery")

        .mode("append")

        .option(

            "table",

            PIPELINE_ERROR_TABLE

        )

        .save()

    )

    logger.error(

        f"Error Logged : {table_name}"

    )