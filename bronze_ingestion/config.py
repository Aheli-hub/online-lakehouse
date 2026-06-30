import os

# ==========================================================
# GCP CONFIGURATION
# ==========================================================

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = \
    "/opt/gcp/online-lakehouse-2fddd5b53bb5.json"

BUCKET_NAME = "24online-datalake"

BRONZE_PATH = f"gs://{BUCKET_NAME}/24online-bronze"

# ==========================================================
# POSTGRESQL JDBC
# ==========================================================

JDBC_URL = "jdbc:postgresql://127.0.0.1:5432/24online"

JDBC_PROPERTIES = {
    "user": "cyberoam",
    "password": "cyberoam",
    "driver": "org.postgresql.Driver"
}

# ==========================================================
# PIPELINE SETTINGS
# ==========================================================

APP_NAME = "24Online Bronze Ingestion"

LOG_FOLDER = "logs"

MAX_RETRY = 3

RETRY_WAIT = 5

DEFAULT_START_DATE = "1900-01-01 00:00:00"

WRITE_MODE = "delta"

MERGE_SCHEMA = True

# ==========================================================
# PARALLELISM
# ==========================================================

MAX_THREADS = 4

ENABLE_PARALLEL = False

# ==========================================================
# JDBC PERFORMANCE
# ==========================================================

FETCH_SIZE = 50000

NUM_PARTITIONS = 4

# ==========================================================
# LOGGING
# ==========================================================

LOG_LEVEL = "INFO"