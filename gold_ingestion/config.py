# ==========================================================
# GCP PROJECT
# ==========================================================

PROJECT_ID = "online-lakehouse"

BUCKET_NAME = "24online-datalake"

# ==========================================================
# GCS PATHS
# ==========================================================

SILVER_PATH = f"gs://{BUCKET_NAME}/24online-silver"

GOLD_PATH = f"gs://{BUCKET_NAME}/24online-gold"

# ==========================================================
# BIGQUERY DATASETS
# ==========================================================

METADATA_DATASET = "lakehouse_metadata"

AUDIT_DATASET = "lakehouse_audit"

# ==========================================================
# BIGQUERY TABLES
# ==========================================================

GOLD_CONFIG_TABLE = (
    f"{PROJECT_ID}.{METADATA_DATASET}.gold_config"
)

PIPELINE_RUN_TABLE = (
    f"{PROJECT_ID}.{AUDIT_DATASET}.pipeline_runs"
)

PIPELINE_ERROR_TABLE = (
    f"{PROJECT_ID}.{AUDIT_DATASET}.pipeline_errors"
)

# ==========================================================
# DELTA CONFIGURATION
# ==========================================================

DELTA_FORMAT = "delta"

WRITE_MODE = "overwrite"

MERGE_SCHEMA = True

OVERWRITE_SCHEMA = True

# ==========================================================
# PIPELINE
# ==========================================================

PIPELINE_NAME = "Gold Pipeline"

TEMP_GCS_BUCKET = BUCKET_NAME