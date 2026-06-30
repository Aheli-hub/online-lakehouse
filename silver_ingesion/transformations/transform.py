from pyspark.sql.functions import (
    col,
    trim,
    when,
    current_timestamp
)

# ==========================================================
# APPLY ALL SILVER TRANSFORMATIONS
# ==========================================================

def apply_transformations(

    df,
    table_name,
    schema_config,
    primary_key

):

    print("=" * 60)
    print(f"Applying Transformations : {table_name}")
    print("=" * 60)

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

    # ======================================================
    # APPLY DATA TYPES
    # ======================================================

    print("Casting Columns...")

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

    print("Trimming String Columns...")

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

    print("Replacing Empty Strings with NULL...")

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

    print("Validating Mandatory Columns...")

    for row in table_schema:

        column_name = row["column_name"]

        nullable = row["nullable"]

        if (

            column_name in df.columns

            and

            nullable == False

        ):

            before = df.count()

            df = df.filter(

                col(column_name).isNotNull()

            )

            after = df.count()

            removed = before - after

            if removed > 0:

                print(

                    f"{column_name} : "

                    f"{removed} NULL rows removed"

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

        print(

            f"Duplicate Rows Removed : "

            f"{before-after}"

        )

    # ======================================================
    # ADD AUDIT COLUMN
    # ======================================================

    print("Adding Audit Columns...")

    df = df.withColumn(

        "silver_load_ts",

        current_timestamp()

    )

    # ======================================================
    # COLUMN ORDER
    # ======================================================

    ordered_columns = []

    for row in table_schema:

        column_name = row["column_name"]

        if column_name in df.columns:

            ordered_columns.append(

                column_name

            )

    # Add newly created columns

    for c in df.columns:

        if c not in ordered_columns:

            ordered_columns.append(c)

    df = df.select(

        *ordered_columns

    )

    print("Transformation Completed.")

    return df