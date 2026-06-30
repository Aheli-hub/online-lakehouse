from config import JDBC_URL, JDBC_PROPERTIES
from retry import retry

# ==========================================================
# LOAD INGESTION METADATA
# ==========================================================

@retry()
def load_metadata(spark):

    query = """
    (
        SELECT
            table_name,
            load_type,
            watermark_column,
            watermark_type,
            primary_key,
            merge_enabled,
            partition_column,
            source_schema,
            incremental_strategy,
            table_priority,
            batch_size
        FROM ingestion_metadata
        WHERE active = TRUE
        ORDER BY table_priority, table_name
    ) metadata
    """

    df = (
        spark.read
        .jdbc(
            url=JDBC_URL,
            table=query,
            properties=JDBC_PROPERTIES
        )
    )

    return df