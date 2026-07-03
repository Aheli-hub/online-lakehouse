from delta.tables import DeltaTable

from gold_ingestion.config import (
    DELTA_FORMAT,
    WRITE_MODE,
    MERGE_SCHEMA,
    OVERWRITE_SCHEMA
)

from gold_ingestion.logger import get_logger

logger = get_logger()


# ==========================================================
# WRITE GOLD
# ==========================================================

def write_gold(

    spark,

    df,

    gold_path,

    load_type,

    merge_key

):

    logger.info(f"Writing Gold : {gold_path}")

    rows = df.count()

    # ======================================================
    # FIRST LOAD
    # ======================================================

    if not DeltaTable.isDeltaTable(
        spark,
        gold_path
    ):

        (

            df.write

            .format(DELTA_FORMAT)

            .mode(WRITE_MODE)

            .option(
                "mergeSchema",
                str(MERGE_SCHEMA).lower()
            )

            .save(gold_path)

        )

        logger.info("Gold Delta Created")

        return rows_written

    # ======================================================
    # FULL LOAD
    # ======================================================

    if load_type.upper() == "FULL":

        (

            df.write

            .format(DELTA_FORMAT)

            .mode(WRITE_MODE)

            .option(

                "overwriteSchema",

                str(OVERWRITE_SCHEMA).lower()

            )

            .option(
                "mergeSchema",
                str(MERGE_SCHEMA).lower()
            )

            .save(gold_path)

        )

        logger.info("Gold Delta Created")
        return rows
    # ======================================================
    # INCREMENTAL MERGE
    # ======================================================

    delta_table = DeltaTable.forPath(
        spark,
        gold_path
    )

    (

        delta_table.alias("target")

        .merge(

            df.alias("source"),

            f"target.{merge_key} = source.{merge_key}"

        )

        .whenMatchedUpdateAll()

        .whenNotMatchedInsertAll()

        .execute()

    )

    logger.info("Gold Merge Completed")

    return rows