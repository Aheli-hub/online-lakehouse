from delta.tables import DeltaTable

from config import MERGE_SCHEMA
from config import OVERWRITE_SCHEMA

# ==========================================================
# WRITE SILVER DELTA
# ==========================================================

def write_silver(

    spark,
    df,
    silver_path,
    load_type,
    primary_key

):

    print(f"Writing Silver : {silver_path}")

    # ======================================================
    # FIRST LOAD
    # ======================================================

    if not DeltaTable.isDeltaTable(
        spark,
        silver_path
    ):

        (

            df.write

            .format("delta")

            .mode("overwrite")

            .option(
                "mergeSchema",
                str(MERGE_SCHEMA).lower()
            )

            .save(silver_path)

        )

        print("Silver Delta Created")

        return

    # ======================================================
    # FULL LOAD
    # ======================================================

    if load_type.upper() == "FULL":

        (

            df.write

            .format("delta")

            .mode("overwrite")

            .option(
                "overwriteSchema",
                str(OVERWRITE_SCHEMA).lower()
            )

            .option(
                "mergeSchema",
                str(MERGE_SCHEMA).lower()
            )

            .save(silver_path)

        )

        print("Silver Overwrite Completed")

        return

    # ======================================================
    # INCREMENTAL MERGE
    # ======================================================

    delta_table = DeltaTable.forPath(
        spark,
        silver_path
    )

    (

        delta_table.alias("target")

        .merge(

            df.alias("source"),

            f"target.{primary_key}=source.{primary_key}"

        )

        .whenMatchedUpdateAll()

        .whenNotMatchedInsertAll()

        .execute()

    )

    print("Silver Merge Completed")