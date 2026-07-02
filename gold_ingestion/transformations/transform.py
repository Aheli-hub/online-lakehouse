from gold_ingestion.logger import get_logger

from gold_ingestion.readers.silver_reader import (
    read_silver_table
)

from gold_ingestion.transformations.customer_summary import (
    customer_summary
)

from gold_ingestion.transformations.invoice_summary import (
    invoice_summary
)

from gold_ingestion.transformations.invoice_detail_summary import (
    invoice_detail_summary
)

from gold_ingestion.transformations.group_summary import (
    group_summary
)

from gold_ingestion.transformations.policy_summary import (
    policy_summary
)

logger = get_logger()


# ==========================================================
# APPLY GOLD TRANSFORMATIONS
# ==========================================================

def apply_transformations(

    spark,

    table_name,

    config

):

    logger.info("=" * 60)
    logger.info(f"Gold Transformation : {table_name}")
    logger.info("=" * 60)

    source_tables = [

        x.strip()

        for x in config["source_tables"].split(",")

    ]

    dfs = {}

    for table in source_tables:

        dfs[table] = read_silver_table(

            spark,

            table

        )

    # ======================================================
    # CUSTOMER SUMMARY
    # ======================================================

    if table_name == "customer_summary":

        return customer_summary(

            dfs["tbluser"]

        )

    # ======================================================
    # INVOICE SUMMARY
    # ======================================================

    elif table_name == "invoice_summary":

        return invoice_summary(

            dfs["tblinvoice"]

        )

    # ======================================================
    # INVOICE DETAIL SUMMARY
    # ======================================================

    elif table_name == "invoice_detail_summary":

        return invoice_detail_summary(

            dfs["tblinvdetail"]

        )

    # ======================================================
    # GROUP SUMMARY
    # ======================================================

    elif table_name == "group_summary":

        return group_summary(

            dfs["tblgroup"]

        )

    # ======================================================
    # POLICY SUMMARY
    # ======================================================

    elif table_name == "policy_summary":

        return policy_summary(

            dfs["tblpolicy"]

        )

    else:

        raise Exception(

            f"Unknown Gold Table : {table_name}"

        )