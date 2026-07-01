from pyspark.sql.functions import (
    col,
    trim,
    when,
    current_timestamp
)

from silver_ingestion.logger import get_logger

logger = get_logger()

# ==========================================================
# APPLY ALL SILVER TRANSFORMATIONS
# ==========================================================

def apply_transformations(

    df,
    table_name,
    schema_config,
    primary_key

):

    logger.info("=" * 60)
    logger.info(f"Applying Transformations : {table_name}")
    logger.info("=" * 60)

    # ======================================================
    # LOAD SCHEMA METADATA
    # ======================================================

    table_schema = (

        schema_config

        .filter(
            col("table_name") == table_name
        )

        .collect()

    )

    if not table_schema:

        logger.warning(
            f"No schema configuration found for {table_name}"
        )

        return df

    # ======================================================
    # APPLY DATA TYPES
    # ======================================================

    logger.info("Casting Columns...")

    for row in table_schema:

        column_name = row["column_name"]

        data_type = row["data_type"]

        if column_name in df.columns:

            df = df.withColumn(

                column_name,

                col(column_name).cast(data_type)

            )

    # ======================================================
    # TRIM STRING COLUMNS
    # ======================================================

    logger.info("Trimming String Columns...")

    for row in table_schema:

        column_name = row["column_name"]

        data_type = row["data_type"]

        if (

            column_name in df.columns

            and

            data_type.lower() == "string"

        ):

            df = df.withColumn(

                column_name,

                trim(col(column_name))

            )

    # ======================================================
    # EMPTY STRING -> NULL
    # ======================================================

    logger.info("Replacing Empty Strings with NULL...")

    for row in table_schema:

        column_name = row["column_name"]

        data_type = row["data_type"]

        if (

            column_name in df.columns

            and

            data_type.lower() == "string"

        ):

            df = df.withColumn(

                column_name,

                when(

                    trim(col(column_name)) == "",

                    None

                ).otherwise(

                    col(column_name)

                )

            )

    # ======================================================
    # REMOVE NULLS FROM MANDATORY COLUMNS
    # ======================================================

    logger.info("Validating Mandatory Columns...")

    mandatory_columns = [

        row["column_name"]

        for row in table_schema

        if (

            row["nullable"] == False

            and

            row["column_name"] in df.columns

        )

    ]

    if mandatory_columns:

        before = df.count()

        df = df.na.drop(

            subset=mandatory_columns

        )

        after = df.count()

        logger.info(

            f"Rows Removed (Mandatory NULLs): {before - after}"

        )

    # ======================================================
    # REMOVE DUPLICATES
    # ======================================================

    if primary_key in df.columns:

        before = df.count()

        df = df.dropDuplicates(

            [primary_key]

        )

        after = df.count()

        logger.info(

            f"Duplicate Rows Removed : {before - after}"

        )

    else:

        logger.warning(

            f"Primary Key '{primary_key}' not found."

        )

    # ======================================================
    # ADD AUDIT COLUMN
    # ======================================================

    logger.info("Adding Audit Columns...")

    df = df.withColumn(

        "silver_load_ts",

        current_timestamp()

    )

    # ======================================================
    # REORDER COLUMNS
    # ======================================================

    ordered_columns = [

        row["column_name"]

        for row in table_schema

        if row["column_name"] in df.columns

    ]

    for column in df.columns:

        if column not in ordered_columns:

            ordered_columns.append(column)

    df = df.select(

        *ordered_columns

    )

    logger.info("Transformation Completed.")

    return df