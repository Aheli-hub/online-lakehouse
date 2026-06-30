from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql.functions import lit

from config import BRONZE_PATH
from config import MERGE_SCHEMA


# ==========================================================
# WRITE BRONZE DELTA
# ==========================================================

def write_bronze(
    spark,
    df,
    table_name,
    load_type,
    primary_key,
    merge_enabled
):

    today = datetime.now()

    df = (
        df
        .withColumn("ingestion_year", lit(today.strftime("%Y")))
        .withColumn("ingestion_month", lit(today.strftime("%m")))
        .withColumn("ingestion_day", lit(today.strftime("%d")))
        .withColumn("ingestion_timestamp", lit(today))
    )

    bronze_path = f"{BRONZE_PATH}/{table_name}"

    print(f"Writing Bronze : {bronze_path}")

    # =====================================================
    # FIRST LOAD
    # =====================================================

    if not DeltaTable.isDeltaTable(spark, bronze_path):

        (
            df.write
            .format("delta")
            .mode("overwrite")
            .partitionBy(
                "ingestion_year",
                "ingestion_month",
                "ingestion_day"
            )
            .option("mergeSchema", str(MERGE_SCHEMA).lower())
            .save(bronze_path)
        )

        print("Delta table created.")

        return bronze_path

    # =====================================================
    # FULL LOAD
    # =====================================================

    if load_type.upper() == "FULL":

        if merge_enabled:

            delta_table = DeltaTable.forPath(
                spark,
                bronze_path
            )

            (
                delta_table.alias("target")
                .merge(
                    df.alias("source"),
                    f"target.{primary_key} = source.{primary_key}"
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )

            print("Full Load MERGE completed.")

        else:

            (
                df.write
                .format("delta")
                .mode("overwrite")
                .partitionBy(
                    "ingestion_year",
                    "ingestion_month",
                    "ingestion_day"
                )
                .option("overwriteSchema", "true")
                .option("mergeSchema", str(MERGE_SCHEMA).lower())
                .save(bronze_path)
            )

            print("Full overwrite completed.")

        return bronze_path

    # =====================================================
    # INCREMENTAL LOAD
    # =====================================================

    if merge_enabled:

        delta_table = DeltaTable.forPath(
            spark,
            bronze_path
        )

        (
            delta_table.alias("target")
            .merge(
                df.alias("source"),
                f"target.{primary_key} = source.{primary_key}"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        print("Incremental MERGE completed.")

    else:

        (
            df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", str(MERGE_SCHEMA).lower())
            .save(bronze_path)
        )

        print("Incremental APPEND completed.")

    return bronze_path