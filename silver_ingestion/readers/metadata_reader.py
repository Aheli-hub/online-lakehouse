from pyspark.sql.functions import col

from config import SILVER_CONFIG
from config import SCHEMA_CONFIG

# ==========================================================
# READ BIGQUERY
# ==========================================================

def read_bq(
    spark,
    table_name
):

    return (
        spark.read
        .format("bigquery")
        .option("table", table_name)
        .load()
    )


# ==========================================================
# LOAD SILVER CONFIG
# ==========================================================

def load_silver_config(
    spark
):

    return (

        read_bq(
            spark,
            SILVER_CONFIG
        )

        .filter(
            col("active") == True
        )

    )


# ==========================================================
# LOAD SCHEMA CONFIG
# ==========================================================

def load_schema_config(
    spark
):

    return (

        read_bq(
            spark,
            SCHEMA_CONFIG
        )

        .filter(
            col("active") == True
        )

    )