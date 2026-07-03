from pyspark.sql.functions import (
    col,
    current_timestamp
)

from gold_ingestion.readers.silver_reader import read_silver_table
from gold_ingestion.logger import get_logger

logger = get_logger()


def customer_summary(spark):

    logger.info("Reading tbluser...")

    user_df = read_silver_table(
        spark,
        "tbluser"
    )

    logger.info("Reading tblgroup...")

    group_df = read_silver_table(
        spark,
        "tblgroup"
    )

    logger.info("Reading tblpolicy...")

    policy_df = read_silver_table(
        spark,
        "tblpolicy"
    )

    logger.info("Joining Customer Tables...")

    df = (

        user_df.alias("u")

        .join(
            group_df.alias("g"),
            col("u.groupid") == col("g.groupid"),
            "left"
        )

        .join(
            policy_df.alias("p"),
            col("g.policyid") == col("p.policyid"),
            "left"
        )

        .select(

            col("u.userid"),

            col("u.username"),

            col("u.firstname"),

            col("u.lastname"),

            col("u.emailid"),

            col("u.phoneno"),

            col("g.groupid"),

            col("g.groupname"),

            col("p.policyid"),

            col("p.policyname"),

            current_timestamp().alias(
                "gold_load_ts"
            )

        )

    )

    return df