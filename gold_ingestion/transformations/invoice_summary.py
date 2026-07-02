from pyspark.sql.functions import (
    sum,
    count,
    round,
    current_timestamp
)

from gold_ingestion.readers.silver_reader import read_silver_table
from gold_ingestion.logger import get_logger

logger = get_logger()


def build_invoice_summary(spark):

    logger.info("Reading tblinvoice...")

    invoice_df = read_silver_table(

        spark,

        "tblinvoice"

    )

    logger.info("Creating Invoice Summary...")

    df = (

        invoice_df

        .groupBy(

            "accountid"

        )

        .agg(

            count("*").alias(

                "invoice_count"

            ),

            round(

                sum("totalamt"),

                2

            ).alias(

                "total_invoice_amount"

            ),

            round(

                sum("paidamt"),

                2

            ).alias(

                "total_paid_amount"

            ),

            round(

                sum("discountamount"),

                2

            ).alias(

                "total_discount_amount"

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