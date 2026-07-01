from pyspark.sql import SparkSession

# ==========================================================
# CREATE SPARK SESSION
# ==========================================================

def get_spark():

    spark = (
        SparkSession.builder
        .appName("Silver_Delta_Engine")

        # Delta Lake
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

    # ======================================================
    # Spark Configurations
    # ======================================================

    # Fix Parquet Timestamp(Nanos)
    spark.conf.set(
        "spark.sql.legacy.parquet.nanosAsLong",
        "true"
    )

    # Adaptive Query Execution
    spark.conf.set(
        "spark.sql.adaptive.enabled",
        "true"
    )

    # Dynamic Partition Pruning
    spark.conf.set(
        "spark.sql.optimizer.dynamicPartitionPruning.enabled",
        "true"
    )

    # Shuffle Partitions
    spark.conf.set(
        "spark.sql.shuffle.partitions",
        "200"
    )

    return spark