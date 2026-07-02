from pyspark.sql import SparkSession

from delta import configure_spark_with_delta_pip

from gold_ingestion.config import (

    PROJECT_ID,

    TEMP_GCS_BUCKET

)

from gold_ingestion.logger import get_logger

logger = get_logger()


# ==========================================================
# CREATE SPARK SESSION
# ==========================================================

def get_spark():

    builder = (

        SparkSession.builder

        .appName("Gold Pipeline")

        .config(

            "spark.sql.extensions",

            "io.delta.sql.DeltaSparkSessionExtension"

        )

        .config(

            "spark.sql.catalog.spark_catalog",

            "org.apache.spark.sql.delta.catalog.DeltaCatalog"

        )

        .config(

            "temporaryGcsBucket",

            TEMP_GCS_BUCKET

        )

    )

    spark = configure_spark_with_delta_pip(

        builder

    ).getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    logger.info("Spark Session Created")

    return spark