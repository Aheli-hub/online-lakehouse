from uuid import uuid4
from datetime import datetime

from pyspark.sql import Row

from silver_ingestion.config import (
    PROJECT_ID,
    AUDIT_DATASET,
    PIPELINE_TABLE
)

from silver_ingestion.logger import get_logger

logger = get_logger()


# ==========================================================
# START PIPELINE
# ==========================================================

def start_pipeline(spark):

    run_id = str(uuid4())

    rows = [

        Row(

            run_id=run_id,

            pipeline_name="Silver Pipeline",

            event_type="START",

            event_time=datetime.utcnow(),

            status="RUNNING"

        )

    ]

    (
        spark.createDataFrame(rows)
        .write
        .format("bigquery")
        .mode("append")
        .option(
            "table",
            PIPELINE_TABLE
        )
        .save()
    )

    logger.info(f"Pipeline Started : {run_id}")

    return run_id


# ==========================================================
# FINISH PIPELINE
# ==========================================================

def finish_pipeline(

        spark,

        run_id,

        status

):

    rows = [

        Row(

            run_id=run_id,

            pipeline_name="Silver Pipeline",

            event_type="END",

            event_time=datetime.utcnow(),

            status=status

        )

    ]

    (
        spark.createDataFrame(rows)
        .write
        .format("bigquery")
        .mode("append")
        .option(
            "table",
            PIPELINE_TABLE
        )
        .save()
    )

    logger.info(f"Pipeline Finished : {run_id}")