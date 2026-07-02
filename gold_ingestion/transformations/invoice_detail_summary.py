from pyspark.sql.functions import (
    sum,
    count,
    round,
    current_timestamp
)

from gold_ingestion.readers.silver_reader import read_silver_table
from gold_ingestion.logger import get_logger

logger = get_logger()


def build_invoice_detail_summary(spark):

    logger.info("Reading tblinvdetail...")

    detail_df = read_silver_table(

        spark,

        "tblinvdetail"

    )

    logger.info("Creating Invoice Detail Summary...")

    df = (

        detail_df

        .groupBy(

            "invoiceid"

        )

        .agg(

            count("*").alias(

                "total_items"

            ),

            round(

                sum("amount"),

                2

            ).alias(

                "invoice_amount"

            )

        )

        .withColumn(

            "gold_load_ts",

            current_timestamp()

        )

    )

    logger.info(

        f"Rows Created : {df.count()}"

    )

    return df