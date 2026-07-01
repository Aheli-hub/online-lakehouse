from pyspark.sql.functions import col

from silver_ingestion.config import (
    SILVER_CONFIG_TABLE,
    SCHEMA_CONFIG_TABLE,
    DQ_RULES_TABLE
)

# ==========================================================
# READ BIGQUERY TABLE
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
            SILVER_CONFIG_TABLE
        )

        .filter(
            col("active")
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
            SCHEMA_CONFIG_TABLE
        )

        .filter(
            col("active")
        )

    )


# ==========================================================
# LOAD DATA QUALITY RULES
# ==========================================================

def load_dq_rules(
    spark
):

    return (

        read_bq(
            spark,
            DQ_RULES_TABLE
        )

        .filter(
            col("active")
        )

    )