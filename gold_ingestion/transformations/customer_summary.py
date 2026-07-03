from pyspark.sql.functions import (
    col,
    current_timestamp
)

from gold_ingestion.readers.silver_reader import read_silver_table
from gold_ingestion.logger import get_logger

logger = get_logger()


def customer_summary(user_df,group_df,policy_df):

 
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

            col("u.emailid"),
   
            col("g.groupname"),

            col("p.policyid"),

            col("p.policyname"),

            current_timestamp().alias(
                "gold_load_ts"
            )

        )

    )

    return df