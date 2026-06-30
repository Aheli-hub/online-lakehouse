
from datetime import datetime
import traceback

from pyspark.sql.functions import max as spark_max

from spark_session import get_spark
from metadata import load_metadata
from jdbc_reader import read_table
from bronze_writer import write_bronze
from audit import update_audit
from run_manager import start_pipeline, finish_pipeline
from error_logger import log_error
from logger import get_logger

logger = get_logger()

# ==========================================================
# START SPARK
# ==========================================================

spark = get_spark()

run_id = start_pipeline()

logger.info("=" * 60)
logger.info(f"Pipeline Run ID : {run_id}")
logger.info("Bronze Pipeline Started")

metadata = load_metadata(spark)

# ==========================================================
# PIPELINE SUMMARY VARIABLES
# ==========================================================

pipeline_start = datetime.now()

total_tables = metadata.count()

success_tables = 0

failed_tables = 0

total_rows = 0

pipeline_failed = False

# ==========================================================
# PROCESS TABLES
# ==========================================================

for row in metadata.collect():

    start_time = datetime.now()

    table_name = row["table_name"]
    load_type = row["load_type"]
    watermark = row["watermark_column"]
    watermark_type = row["watermark_type"]
    primary_key = row["primary_key"]
    merge_enabled = row["merge_enabled"]

    logger.info("=" * 60)
    logger.info(f"Processing : {table_name}")
    logger.info(f"Load Type  : {load_type}")

    try:

        # ==================================================
        # BUILD SQL
        # ==================================================

        if load_type.upper() == "FULL":

            sql = f"""
            SELECT *
            FROM {table_name}
            """

        else:

            if watermark_type.upper() == "TIMESTAMP":

                sql = f"""
                SELECT *
                FROM {table_name}
                WHERE {watermark} >
                (
                    SELECT
                        COALESCE(MAX(last_run_time), '1900-01-01')
                    FROM ingestion_audit
                    WHERE table_name = '{table_name}'
                )
                """

            elif watermark_type.upper() == "INTEGER":

                sql = f"""
                SELECT *
                FROM {table_name}
                WHERE {watermark} >
                (
                    SELECT
                        COALESCE(MAX(last_watermark), 0)
                    FROM ingestion_audit
                    WHERE table_name = '{table_name}'
                )
                """

            else:

                raise Exception(
                    f"Unsupported watermark type : {watermark_type}"
                )

        # ==================================================
        # READ SOURCE
        # ==================================================

        df = read_table(
            spark,
            sql
        )

        row_count = df.count()

        logger.info(f"Rows Read : {row_count}")

        if row_count == 0:

            logger.info("No new data.")

            continue

        # ==================================================
        # LAST WATERMARK
        # ==================================================

        last_watermark = None

        if (
            load_type.upper() == "INCREMENTAL"
            and watermark_type.upper() == "INTEGER"
        ):

            last_watermark = (
                df.agg(
                    spark_max(watermark).alias("max_value")
                )
                .collect()[0]["max_value"]
            )

        # ==================================================
        # WRITE BRONZE
        # ==================================================

        path = write_bronze(
            spark=spark,
            df=df,
            table_name=table_name,
            load_type=load_type,
            primary_key=primary_key,
            merge_enabled=merge_enabled
        )

        # ==================================================
        # AUDIT
        # ==================================================

        update_audit(
            run_id=run_id,
            table_name=table_name,
            row_count=row_count,
            status="SUCCESS",
            last_watermark=last_watermark
        )
        success_tables += 1
        total_rows += row_count
        end_time = datetime.now()

        logger.info(f"Completed : {table_name}")
        logger.info(f"Rows      : {row_count}")
        logger.info(f"Location  : {path}")
        logger.info(
            f"Time      : {(end_time - start_time).seconds} sec"
        )

    except Exception as e:

        pipeline_failed = True
        failed_tables += 1
        logger.error(f"{table_name} FAILED")
        logger.exception(e)

        # ==================================================
        # ERROR TABLE (DLQ)
        # ==================================================

        log_error(
            run_id=run_id,
            table_name=table_name,
            stage="PIPELINE",
            error_message=str(e),
            stack_trace=traceback.format_exc()
        )

        # ==================================================
        # AUDIT FAILED
        # ==================================================

        update_audit(
            run_id=run_id,
            table_name=table_name,
            row_count=0,
            status="FAILED",
            last_watermark=None
        )

# ==========================================================
# FINISH PIPELINE
# ==========================================================

if pipeline_failed:

    finish_pipeline(
        run_id,
        "FAILED"
    )

else:

    finish_pipeline(
        run_id,
        "SUCCESS"
    )

logger.info("=" * 60)
logger.info("Bronze Pipeline Completed")
# ==========================================================
# PIPELINE SUMMARY
# ==========================================================

pipeline_end = datetime.now()

logger.info("=" * 60)
logger.info("BRONZE PIPELINE SUMMARY")
logger.info("=" * 60)

logger.info(f"Run ID            : {run_id}")
logger.info(f"Tables Processed  : {total_tables}")
logger.info(f"Successful Tables : {success_tables}")
logger.info(f"Failed Tables     : {failed_tables}")
logger.info(f"Rows Loaded       : {total_rows}")
logger.info(
    f"Execution Time    : {(pipeline_end - pipeline_start).seconds} sec"
)

logger.info("=" * 60)
spark.stop()

