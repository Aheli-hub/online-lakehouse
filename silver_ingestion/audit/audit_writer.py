from datetime import datetime

from pyspark.sql import Row

from config import PROJECT_ID
from config import METADATA_DATASET

# ==========================================================
# WRITE AUDIT TO BIGQUERY
# ==========================================================

def update_audit(

    spark,
    run_id,
    table_name,
    row_count,
    status

):

    audit = [

        Row(

            run_id=run_id,

            table_name=table_name,

            row_count=row_count,

            status=status,

            load_time=datetime.now()

        )

    ]

    (

        spark.createDataFrame(audit)

        .write

        .format("bigquery")

        .mode("append")

        .option(

            "table",

            f"{PROJECT_ID}.{METADATA_DATASET}.silver_audit"

        )

        .save()

    )

    print("Silver Audit Updated")