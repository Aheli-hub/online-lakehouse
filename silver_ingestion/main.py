from silver_ingestion.spark_session import get_spark
from silver_ingestion.logger import get_logger

from silver_ingestion.run_manager import (
    start_pipeline,
    finish_pipeline
)

from silver_ingestion.pipeline_summary import (
    pipeline_summary
)

from silver_ingestion.audit.error_logger import log_error

from silver_ingestion.readers.metadata_reader import (
    load_silver_config,
    load_schema_config,
    load_dq_rules
)

from silver_ingestion.readers.bronze_reader import read_bronze

from silver_ingestion.transformations.transform import (
    apply_transformations
)

from silver_ingestion.writers.silver_writer import (
    write_silver
)

logger = get_logger()


def main():

    spark = get_spark()

    run_id = start_pipeline(spark)

    logger.info("=" * 60)
    logger.info("Silver Pipeline Started")
    logger.info("=" * 60)

    success_tables = 0
    failed_tables = 0
    total_rows = 0

    try:

        silver_config = load_silver_config(spark)

        schema_config = load_schema_config(spark)

        dq_rules = load_dq_rules(spark)

        configs = silver_config.collect()

        logger.info(f"Tables to Process : {len(configs)}")

        for cfg in configs:

            table_name = cfg["table_name"]

            logger.info("=" * 60)
            logger.info(f"Processing Table : {table_name}")
            logger.info("=" * 60)

            try:

                # ----------------------------------------
                # Read Bronze
                # ----------------------------------------

                df = read_bronze(

                    spark,

                    cfg["bronze_path"]

                )

                # ----------------------------------------
                # Transform
                # ----------------------------------------

                df = apply_transformations(

                    df=df,

                    table_name=table_name,

                    primary_key=cfg["primary_key"],

                    schema_config=schema_config,

                    dq_rules=dq_rules

                )

                # ----------------------------------------
                # Write Silver
                # ----------------------------------------

                rows = write_silver(

                    spark=spark,

                    df=df,

                    silver_path=cfg["silver_path"],

                    load_type=cfg["load_type"],

                    primary_key=cfg["primary_key"]

                )

                total_rows += rows

                success_tables += 1

                logger.info(
                    f"{table_name} Completed Successfully"
                )

            except Exception as e:

                failed_tables += 1

                logger.exception(
                    f"{table_name} Failed"
                )

                log_error(

                    spark=spark,

                    run_id=run_id,

                    table_name=table_name,

                    error_message=str(e)

                )

        pipeline_summary(

            total_tables=success_tables + failed_tables,

            success_tables=success_tables,

            failed_tables=failed_tables,

            total_rows=total_rows

        )

        pipeline_status = (

            "SUCCESS"

            if failed_tables == 0

            else "PARTIAL_SUCCESS"

        )

        finish_pipeline(

            spark,

            run_id,

            pipeline_status

        )

    except Exception as e:

        logger.exception("Pipeline Failed")

        finish_pipeline(

            spark,

            run_id,

            "FAILED"

        )

        raise

    finally:

        logger.info("=" * 60)
        logger.info("Stopping Spark Session")
        logger.info("=" * 60)

        spark.stop()


if __name__ == "__main__":

    main()