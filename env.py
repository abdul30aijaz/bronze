# Databricks notebook source
# DBTITLE 1,Core configuration constants
"""Environment configuration for Mars Pet Care MDIF pipeline. Defines core constants, paths, and connection settings."""
import sys
import json

CATALOG   = "workspace"
SCHEMA    = "metadata"
TABLE     = "metadata_control_table_2"

VOLUME_NAME = "mdif_volume"
VOLUME_BASE = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_NAME}"

# Data layer base paths (Unity Catalog Volumes)
LANDING_BASE = "/Volumes/workspace/default/staging"
RAW_BASE     = "/Volumes/workspace/default/raw"

# COMMAND ----------

# DBTITLE 1,Databricks CLI/SDK auth (for cd_create_jobs)
SECRET_SCOPE = "mdif-cicd"

DATABRICKS_HOST     = dbutils.secrets.get(scope=SECRET_SCOPE, key="DATABRICKS_HOST")
DATABRICKS_TOKEN    = dbutils.secrets.get(scope=SECRET_SCOPE, key="DATABRICKS_TOKEN")
DATABRICKS_REPO_ID  = dbutils.secrets.get(scope=SECRET_SCOPE, key="DATABRICKS_REPO_ID")

# COMMAND ----------

# DBTITLE 1,Workspace configuration
WORKSPACE_PATH = "/Workspace/Shared/bronze"

if WORKSPACE_PATH not in sys.path:
    sys.path.insert(0, WORKSPACE_PATH)

# COMMAND ----------

# DBTITLE 1,Single job widget
# Read single JOB_NAME widget parameter (for single table processing)
try:
    _raw_job_name = dbutils.widgets.get("JOB_NAME").strip()
    JOB_NAME      = _raw_job_name if _raw_job_name else None
except Exception:
    JOB_NAME = None

# COMMAND ----------

# DBTITLE 1,Multiple jobs widget
# Read JOB_NAMES widget parameter (supports JSON array or comma-separated list)
try:
    _raw = dbutils.widgets.get("JOB_NAMES").strip()
    if not _raw:
        raise ValueError("empty")
    if _raw.startswith("["):
        JOB_NAMES = [j.strip() for j in json.loads(_raw) if j.strip()]
    else:
        JOB_NAMES = [j.strip() for j in _raw.split(",") if j.strip()]
    if not JOB_NAMES:
        raise ValueError("empty after parsing")
except Exception:
    JOB_NAMES = []

# COMMAND ----------

# DBTITLE 1,Validation helper functions
def require_job_name():
    """Validates that JOB_NAME widget parameter is provided, raises ValueError if missing."""
    if not JOB_NAME:
        raise ValueError(
            "JOB_NAME is required but was not provided. "
            "Set the JOB_NAME widget before running this notebook."
        )

def require_job_names():
    """Validates that JOB_NAMES widget parameter is provided, raises ValueError if missing."""
    if not JOB_NAMES:
        raise ValueError(
            "JOB_NAMES is required but was not provided. "
            "Set the JOB_NAMES widget before running this notebook."
        )

# COMMAND ----------

# DBTITLE 1,Derived paths setup
# Construct derived paths based on JOB_NAME and configuration
METADATA_BASE_DIR     = f"{VOLUME_BASE}/metadata/configs"
METADATA_DIR          = f"{METADATA_BASE_DIR}/{JOB_NAME}" if JOB_NAME else None
REQUIREMENTS          = f"{WORKSPACE_PATH}/requirements.txt"
DEFAULT_CONTROL_TABLE = f"{CATALOG}.{SCHEMA}.{TABLE}"

# COMMAND ----------

# DBTITLE 1,Environment confirmation output
# Print configuration summary for verification
print("ENVIRONMENT loaded")
print(f"  CATALOG              = {CATALOG}")
print(f"  SCHEMA                = {SCHEMA}")
print(f"  CONTROL TABLE         = {DEFAULT_CONTROL_TABLE}")
print(f"  VOLUME_BASE            = {VOLUME_BASE}")
print(f"  LANDING_BASE           = {LANDING_BASE}")
print(f"  RAW_BASE               = {RAW_BASE}")
print(f"  WORKSPACE_PATH         = {WORKSPACE_PATH}")
print(f"  METADATA_BASE_DIR      = {METADATA_BASE_DIR}")
print(f"  DATABRICKS_HOST        = {DATABRICKS_HOST}")