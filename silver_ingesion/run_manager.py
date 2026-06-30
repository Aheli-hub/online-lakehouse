from uuid import uuid4
from datetime import datetime

from pyspark.sql import Row

from config import PROJECT_ID
from config import METADATA_DATASET

# ==========================================================
# START PIPELINE
# ==========================================================

def start_pipeline(spark):

    run_id = str(uuid4())

    data = [

        Row(

            run_id=run_id,

            pipeline_name="Silver Pipeline",

            start_time=datetime.now(),

            end_time=None,

            status="RUNNING"

        )

    ]

    (

        spark.createDataFrame(data)

        .write

        .format("bigquery")

        .mode("append")

        .option(

            "table",

            f"{PROJECT_ID}.{METADATA_DATASET}.pipeline_run"

        )

        .save()

    )

    print(f"Run Started : {run_id}")

    return run_id


# ==========================================================
# FINISH PIPELINE
# ==========================================================

def finish_pipeline(

    spark,

    run_id,

    status

):

    data = [

        Row(

            run_id=run_id,

            pipeline_name="Silver Pipeline",

            start_time=None,

            end_time=datetime.now(),

            status=status

        )

    ]

    (

        spark.createDataFrame(data)

        .write

        .format("bigquery")

        .mode("append")

        .option(

            "table",

            f"{PROJECT_ID}.{METADATA_DATASET}.pipeline_run"

        )

        .save()

    )

    print(f"Run Finished : {run_id}")