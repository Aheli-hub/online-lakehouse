from pyspark.sql import SparkSession

# ==========================================================
# CREATE SPARK SESSION
# ==========================================================

def get_spark():

    spark = (
        SparkSession.builder
        .appName("Silver_Delta_Engine")
        .getOrCreate()
    )

    # Fix Parquet Timestamp(Nanos)
    spark.conf.set(
        "spark.sql.legacy.parquet.nanosAsLong",
        "true"
    )

    return spark