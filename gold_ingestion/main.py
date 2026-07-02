from gold_ingestion.spark_session import get_spark
from gold_ingestion.logger import get_logger

from gold_ingestion.run_manager import (
    start_pipeline,
    finish_pipeline
)

from gold_ingestion.pipeline_summary import (
    pipeline_summary
)

from gold_ingestion.audit.error_logger import log_error

from gold_ingestion.readers.metadata_reader import (
    load_gold_config
)

from gold_ingestion.transformations.transform import (
    apply_transformations
)

from gold_ingestion.writers.gold_writer import (
    write_gold
)

logger = get_logger()


def main():

    spark = get_spark()

    run_id = start_pipeline(spark)
    logger.info("=" * 60)
    logger.info("Gold Pipeline Started")
    logger.info("=" * 60)
    success_tables = 0
    failed_tables = 0
    total_rows = 0

    try:

        configs = load_gold_config(spark).collect()

        logger.info(f"Gold Tables : {len(configs)}")

        for cfg in configs:

            table_name = cfg["table_name"]

            logger.info("=" * 60)
            logger.info(f"Processing : {table_name}")
            logger.info("=" * 60)

            try:

                df = apply_transformations(

                    spark=spark,

                    table_name=table_name,

                    config=cfg

                )

                rows = write_gold(

                    spark=spark,

                    df=df,

                    gold_path=cfg["gold_path"],

                    load_type=cfg["load_type"]

                )

                total_rows += rows

                success_tables += 1
                logger.info(f"{table_name} Completed Successfully")
            except Exception as e:

                failed_tables += 1

                logger.exception(f"{table_name} Failed")

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

        finish_pipeline(

            spark,

            run_id,

            "SUCCESS" if failed_tables == 0 else "PARTIAL_SUCCESS"

        )

    except Exception:

        logger.exception("Gold Pipeline Failed")

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