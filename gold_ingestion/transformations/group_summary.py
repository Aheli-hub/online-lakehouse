from pyspark.sql.functions import (
    current_timestamp
)

from gold_ingestion.readers.silver_reader import read_silver_table
from gold_ingestion.logger import get_logger

logger = get_logger()


def group_summary(group_df):

    logger.info("Creating Group Summary...")

    df = (

        group_df

        .select(

            "groupid",

            "groupname",

            "policyid",

            "securitypolicyid",

            "accesspolicyid",

            "price",

            "creditlimit",

            "groupstatus",

            "connectiontype",

            "packagetype",

            "billingdate",

            "profileid"

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