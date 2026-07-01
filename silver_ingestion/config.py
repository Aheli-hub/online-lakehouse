# ==========================================================
# GCP PROJECT
# ==========================================================

PROJECT_ID = "online-lakehouse"

BUCKET_NAME = "24online-datalake"

# ==========================================================
# GCS PATHS
# ==========================================================

BRONZE_PATH = f"gs://{BUCKET_NAME}/24online-bronze"

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

SILVER_CONFIG_TABLE = (
    f"{PROJECT_ID}.{METADATA_DATASET}.silver_config"
)

SCHEMA_CONFIG_TABLE = (
    f"{PROJECT_ID}.{METADATA_DATASET}.schema_config"
)

DQ_RULES_TABLE = (
    f"{PROJECT_ID}.{METADATA_DATASET}.dq_rules"
)

PIPELINE_RUN_TABLE = (
    f"{PROJECT_ID}.{AUDIT_DATASET}.pipeline_runs"
)

PIPELINE_ERROR_TABLE = (
    f"{PROJECT_ID}.{AUDIT_DATASET}.pipeline_errors"
)

SILVER_AUDIT_TABLE = (
    f"{PROJECT_ID}.{AUDIT_DATASET}.silver_audit"
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

PIPELINE_NAME = "Silver Pipeline"