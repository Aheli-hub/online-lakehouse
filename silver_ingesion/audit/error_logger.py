from datetime import datetime

from pyspark.sql import Row

from config import PROJECT_ID
from config import METADATA_DATASET

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

    print("Error Logged")