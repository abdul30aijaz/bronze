# Databricks notebook source
# DBTITLE 1,Core configuration constants
"""Environment configuration for Mars Pet Care MDIF pipeline. Defines core constants, paths, and connection settings."""

import sys
import json

TABLE     = "metadata_control_table_2"
SCOPE     = "marspcmdifcinkv"
POLICY_ID = "001256943B153715"
CATALOG   = "cdap_mars_pc_mdif"
SCHEMA    = "metadata"
ADLS_BASE = "abfss://marspcmdif@cdapshareddesacin.dfs.core.windows.net"

# COMMAND ----------

# DBTITLE 1,Workspace and secrets
# Workspace configuration and Logic App URL from Key Vault
DEV_PATH= "/Workspace/Shared/Mars_AZURE_Pet_Care_MDIF"
WORKSPACE_PATH = "/Workspace/Shared/Mars_AZURE_Pet_Care_MDIF/bronze"
WORKSPACE_URL  = "adb-7405616865282613.13.azuredatabricks.net"
LOGIC_APP_URL  = dbutils.secrets.get(scope=SCOPE, key="logic-app-url")

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
METADATA_BASE_DIR     = "dbfs:/mdif/metadata/configs"
METADATA_DIR          = f"{METADATA_BASE_DIR}/{JOB_NAME}" if JOB_NAME else None
SCRIPTS_PATH          = f"/Workspace/Shared/Mars_AZURE_Pet_Care_MDIF/metadata/configs"
REQUIREMENTS          = f"{WORKSPACE_PATH}/requirements.txt"
DEFAULT_CONTROL_TABLE = f"{CATALOG}.{SCHEMA}.{TABLE}"

# COMMAND ----------

# DBTITLE 1,Environment confirmation output
# Print configuration summary for verification
print("ENVINRONMENT loaded")