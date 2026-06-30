# ==========================================================
# BRONZE DELTA READER
# ==========================================================

from delta.tables import DeltaTable


def read_bronze(
    spark,
    bronze_path
):

    print(f"Reading Bronze : {bronze_path}")

    if not DeltaTable.isDeltaTable(
        spark,
        bronze_path
    ):

        raise Exception(
            f"Bronze Delta table not found : {bronze_path}"
        )

    df = (
        spark.read
        .format("delta")
        .load(bronze_path)
    )

    print(f"Rows Read : {df.count()}")

    return df