from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

# =====================================================
# SPARK SESSION
# =====================================================
spark = (
    SparkSession.builder
    .appName("Silver_Delta_Engine")
    .getOrCreate()
)

# Fix for Parquet TIMESTAMP(NANOS) issue
spark.conf.set("spark.sql.legacy.parquet.nanosAsLong", "true")

# =====================================================
# BIGQUERY READER
# =====================================================
def read_bq(table_name):
    return (
        spark.read
        .format("bigquery")
        .option("table", table_name)
        .load()
    )

# =====================================================
# LOAD METADATA TABLES
# =====================================================
print("Loading metadata tables...")

silver_config = (
    read_bq("lakehouse_metadata.silver_config")
    .filter(col("active") == True)
)

schema_config = (
    read_bq("lakehouse_metadata.schema_config")
    .filter(col("active") == True)
)

print("Metadata loaded successfully")

# =====================================================
# PROCESS TABLES
# =====================================================
for cfg in silver_config.collect():
    try:
        table_name = cfg["table_name"]
        bronze_path = cfg["bronze_path"]
        silver_path = cfg["silver_path"]
        primary_key = cfg["primary_key"]

        print("\n====================================")
        print(f"Processing Table : {table_name}")
        print("====================================")

        print(f"Reading Bronze: {bronze_path}")

        # Read Bronze
        df = spark.read.parquet(bronze_path)

        print(f"Rows Read: {df.count()}")

        # Apply Schema Metadata
        table_schema = (
            schema_config
            .filter(col("table_name") == table_name)
            .collect()
        )

        for row in table_schema:
            column_name = row["column_name"]
            data_type = row["data_type"]
            nullable = row["nullable"]

            if column_name in df.columns:
                df = df.withColumn(
                    column_name,
                    col(column_name).cast(data_type)
                )

                if nullable == False:
                    df = df.filter(
                        col(column_name).isNotNull()
                    )

        # Deduplicate
        if primary_key in df.columns:
            before_count = df.count()
            df = df.dropDuplicates([primary_key])
            after_count = df.count()
            print(
                f"Duplicates Removed: "
                f"{before_count - after_count}"
            )

        # Audit Column
        df = df.withColumn(
            "silver_load_ts",
            current_timestamp()
        )

        # Write Delta
        print(f"Writing Silver Delta: {silver_path}")

        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(silver_path)
        )

        print(f"SUCCESS : {table_name}")

    except Exception as e:
        print(f"FAILED : {table_name}")
        print(str(e))

# =====================================================
# COMPLETE
# =====================================================
print("\n====================================")
print("SILVER DELTA PROCESS COMPLETED")
print("====================================")

spark.stop()