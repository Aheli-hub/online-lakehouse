from pyspark.sql.functions import (
    current_timestamp
)

from gold_ingestion.readers.silver_reader import read_silver_table
from gold_ingestion.logger import get_logger

logger = get_logger()


def policy_summary(spark):

    logger.info("Reading tblpolicy...")

    policy_df = read_silver_table(

        spark,

        "tblpolicy"

    )

    logger.info("Creating Policy Summary...")

    df = (

        policy_df

        .select(

            "policyid",

            "policyname",

            "allotedminutes",

            "expiredays",

            "policydescription",

            "isperiodic",

            "periodallowedminutes",

            "cycle",

            "sessionpulse",

            "policytype",

            "cycletype",

            "cycleallottedtime",

            "allottedunit"

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