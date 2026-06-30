# ==========================================================
# CONFIGURATION
# ==========================================================

import os

# ==========================================================
# GCP
# ==========================================================

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = \
"/opt/gcp/online-lakehouse-2fddd5b53bb5.json"

PROJECT_ID = "online-lakehouse"

BUCKET_NAME = "24online-datalake"

# ==========================================================
# BRONZE / SILVER PATH
# ==========================================================

BRONZE_PATH = "gs://24online-datalake/24online-bronze"

SILVER_PATH = "gs://24online-datalake/24online-silver"

# ==========================================================
# BIGQUERY METADATA
# ==========================================================

METADATA_DATASET = "lakehouse_metadata"

SILVER_CONFIG = f"{PROJECT_ID}.{METADATA_DATASET}.silver_config"

SCHEMA_CONFIG = f"{PROJECT_ID}.{METADATA_DATASET}.schema_config"

# ==========================================================
# DELTA OPTIONS
# ==========================================================

MERGE_SCHEMA = True

OVERWRITE_SCHEMA = True