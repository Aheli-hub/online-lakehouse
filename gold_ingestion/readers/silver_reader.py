from gold_ingestion.config import (
    SILVER_PATH,
    DELTA_FORMAT
)

from gold_ingestion.logger import get_logger

logger = get_logger()


# ==========================================================
# READ SILVER DELTA TABLE
# ==========================================================

def read_silver_table(

        spark,

        table_name

):

    path = f"{SILVER_PATH}/{table_name}"

    logger.info(

        f"Reading Silver : {path}"

    )

    df = (

        spark.read

        .format(

            DELTA_FORMAT

        )

        .load(

            path

        )

    )

    logger.info(

        f"Rows Read : {df.count()}"

    )

    return df