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

    load_type

):

    logger.info(f"Writing Gold : {gold_path}")

    rows = df.count()

    if load_type.upper() == "FULL":

        (

            df.write

            .format(DELTA_FORMAT)

            .mode(WRITE_MODE)

            .option(

                "overwriteSchema",

                str(OVERWRITE_SCHEMA).lower()

            )

            .save(gold_path)

        )

        logger.info("Gold Delta Created")

    else:

        (

            df.write

            .format(DELTA_FORMAT)

            .mode("overwrite")

            .option(

                "mergeSchema",

                str(MERGE_SCHEMA).lower()

            )

            .save(gold_path)

        )

        logger.info("Gold Delta Updated")

    return rows