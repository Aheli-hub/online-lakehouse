from gold_ingestion.config import (
    GOLD_CONFIG_TABLE
)

from gold_ingestion.logger import get_logger

logger = get_logger()


# ==========================================================
# LOAD GOLD CONFIG
# ==========================================================

def load_gold_config(spark):

    logger.info("Loading Gold Configuration...")

    df = (

        spark.read

        .format("bigquery")

        .option(

            "table",

            GOLD_CONFIG_TABLE

        )

        .load()

        .filter("active = TRUE")

    )

    logger.info(

        f"Gold Config Loaded : {df.count()} tables"

    )

    return df