from pyspark.sql import SparkSession

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

    spark = (
        SparkSession.builder
        .appName("Gold Pipeline")

        # BigQuery
        .config(
            "spark.datasource.bigquery.temporaryGcsBucket",
            TEMP_GCS_BUCKET
        )

        # Delta
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )

        .getOrCreate()
    )

    spark.conf.set(
        "spark.sql.legacy.parquet.nanosAsLong",
        "true"
    )

    spark.conf.set(
        "spark.sql.adaptive.enabled",
        "true"
    )

    spark.conf.set(
        "spark.sql.optimizer.dynamicPartitionPruning.enabled",
        "true"
    )

    spark.conf.set(
        "spark.sql.shuffle.partitions",
        "200"
    )

    return spark