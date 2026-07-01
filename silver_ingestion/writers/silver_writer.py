from delta.tables import DeltaTable

from silver_ingestion.config import (
    MERGE_SCHEMA,
    OVERWRITE_SCHEMA
)

from silver_ingestion.logger import get_logger

logger = get_logger()

# ==========================================================
# WRITE SILVER DELTA
# ==========================================================

def write_silver(

    spark,
    df,
    silver_path,
    load_type,
    primary_key

):

    logger.info(f"Writing Silver : {silver_path}")

    rows_written = df.count()

    # ======================================================
    # FIRST LOAD
    # ======================================================

    if not DeltaTable.isDeltaTable(
        spark,
        silver_path
    ):

        (

            df.write

            .format("delta")

            .mode("overwrite")

            .option(
                "mergeSchema",
                str(MERGE_SCHEMA).lower()
            )

            .save(silver_path)

        )

        logger.info("Silver Delta Created")

        return rows_written

    # ======================================================
    # FULL LOAD
    # ======================================================

    if load_type.upper() == "FULL":

        (

            df.write

            .format("delta")

            .mode("overwrite")

            .option(
                "overwriteSchema",
                str(OVERWRITE_SCHEMA).lower()
            )

            .option(
                "mergeSchema",
                str(MERGE_SCHEMA).lower()
            )

            .save(silver_path)

        )

        logger.info("Silver Overwrite Completed")

        return rows_written

    # ======================================================
    # INCREMENTAL MERGE
    # ======================================================

    delta_table = DeltaTable.forPath(
        spark,
        silver_path
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

    logger.info("Silver Merge Completed")

    return rows_written