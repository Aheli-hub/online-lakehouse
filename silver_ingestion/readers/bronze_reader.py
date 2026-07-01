from delta.tables import DeltaTable

from silver_ingestion.logger import get_logger

logger = get_logger()

# ==========================================================
# BRONZE DELTA READER
# ==========================================================

def read_bronze(
    spark,
    bronze_path
):

    logger.info(f"Reading Bronze : {bronze_path}")

    if not DeltaTable.isDeltaTable(
        spark,
        bronze_path
    ):

        raise FileNotFoundError(
            f"Bronze Delta table not found : {bronze_path}"
        )

    df = (
        spark.read
        .format("delta")
        .load(bronze_path)
    )

    logger.info(f"Rows Read : {df.count()}")

    return df