from pyspark.sql import SparkSession
from config import *

# ==========================================================
# CREATE SPARK SESSION
# ==========================================================

def get_spark():

    spark = (
        SparkSession.builder
        .appName(APP_NAME)

        # ==================================================
        # DELTA LAKE
        # ==================================================
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )

        # ==================================================
        # GCS CONNECTOR
        # ==================================================
        .config(
            "spark.hadoop.google.cloud.auth.service.account.enable",
            "true"
        )
        .config(
            "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
            "/opt/gcp/online-lakehouse-2fddd5b53bb5.json"
        )
        .config(
            "spark.hadoop.fs.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
        )
        .config(
            "spark.hadoop.fs.AbstractFileSystem.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
        )

        # ==================================================
        # PERFORMANCE
        # ==================================================
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")

        .config("spark.sql.shuffle.partitions", "8")

        .config("spark.serializer",
                "org.apache.spark.serializer.KryoSerializer")

        .config("spark.sql.parquet.compression.codec", "snappy")

        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")

        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark