from datetime import datetime

from pyspark.sql import Row

from silver_ingestion.config import (
    PROJECT_ID,
    METADATA_DATASET
)

# ==========================================================
# WRITE AUDIT TO BIGQUERY
# ==========================================================

def update_audit(
    spark,
    run_id,
    table_name,
    row_count,
    status
):

    audit_df = spark.createDataFrame([

        Row(
            run_id=run_id,
            table_name=table_name,
            row_count=row_count,
            status=status,
            load_time=datetime.now()
        )

    ])

    (
        audit_df.write
        .format("bigquery")
        .mode("append")
        .option(
            "table",
            f"{PROJECT_ID}.{METADATA_DATASET}.silver_audit"
        )
        .save()
    )

    print(
        f"Audit Updated : {table_name} | "
        f"Rows={row_count} | Status={status}"
    )