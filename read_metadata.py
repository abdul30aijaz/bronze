# Databricks notebook source
# MAGIC %md
# MAGIC MDIF Metadata Reader
# MAGIC
# MAGIC Reads active table configurations from the Unity Catalog control table
# MAGIC and pushes them to Databricks task values for downstream pipeline tasks.

# COMMAND ----------

# DBTITLE 1,Load Environment
# MAGIC %run /Workspace/Shared/bronze/env

# COMMAND ----------

# DBTITLE 1,Import Dependencies
import json
import datetime
from pyspark.sql import Row
from utils.logging_utils import log_event, get_run_id
from utils.metadata_utils import json_safe, row_to_dict

# COMMAND ----------

# DBTITLE 1,Widget Parameters
dbutils.widgets.text("JOB_NAME", "", "Job Name")
dbutils.widgets.text("TABLE_FILTER", "", "Table Filter (comma-separated, leave empty for all)")

job_name = dbutils.widgets.get("JOB_NAME").strip()
table_filter = dbutils.widgets.get("TABLE_FILTER").strip()

if not job_name:
    raise ValueError("JOB_NAME is required")

filter_tables = [t.strip() for t in table_filter.split(",") if t.strip()] if table_filter else []

# COMMAND ----------

# DBTITLE 1,Initialize Logging
run_id = get_run_id(spark)

log_event(
    spark,
    "INFO",
    f"read_metadata started -- job_name='{job_name}'"
    + (f", table_filter={filter_tables}" if filter_tables else ", table_filter=None (all tables)"),
    run_id,
    function_name="read_metadata",
    status="running",
    additional_data={
        "job_name": job_name,
        "control_table": DEFAULT_CONTROL_TABLE,
        "table_filter": ", ".join(filter_tables) if filter_tables else "none",
    },
)

# COMMAND ----------

# DBTITLE 1,Build Config
def build_config(row: dict) -> dict:
    """Transform a control table row into a pipeline configuration."""
    EXCLUDE_KEYS = {"ingestion_steps", "sync_timestamp"}
    ingestion_steps = row.get("ingestion_steps")

    flat = {k: v for k, v in row.items() if k not in EXCLUDE_KEYS}

    schema_raw = flat.get("schema")
    if isinstance(schema_raw, str):
        try:
            flat["schema"] = json.loads(schema_raw)
        except (json.JSONDecodeError, TypeError):
            flat["schema"] = []
    elif not isinstance(schema_raw, list):
        flat["schema"] = []

    return {
        **flat,
        "ingestion_steps": row_to_dict(ingestion_steps) if ingestion_steps else {},
    }

# COMMAND ----------

# DBTITLE 1,Query Control
try:
    df = spark.sql(f"""
        SELECT * FROM {DEFAULT_CONTROL_TABLE}
        WHERE is_active = true
        AND   job_name  = '{job_name}'
    """)
    configs = [build_config(row.asDict()) for row in df.collect()]

except Exception as exc:
    log_event(
        spark,
        "ERROR",
        f"Failed to query control table -- {type(exc).__name__}: {exc}",
        run_id,
        function_name="read_metadata",
        status="failed",
        additional_data={"job_name": job_name, "error": str(exc)[:300]},
    )
    raise

if not configs:
    log_event(
        spark,
        "ERROR",
        f"No active configs found for job_name='{job_name}'",
        run_id,
        function_name="read_metadata",
        status="failed",
        additional_data={"job_name": job_name},
    )
    raise ValueError(f"No active configs found for job_name='{job_name}'")

# COMMAND ----------

# DBTITLE 1,Filter Tables
if filter_tables:
    all_tables = [c.get("table_name") for c in configs]
    configs = [c for c in configs if c.get("table_name") in filter_tables]
    missing_tables = [t for t in filter_tables if t not in all_tables]

    if missing_tables:
        log_event(
            spark,
            "WARNING",
            f"TABLE_FILTER contains tables not found in control table: {missing_tables}",
            run_id,
            function_name="read_metadata",
            status="info",
            additional_data={"missing_tables": ", ".join(missing_tables)},
        )

    if not configs:
        log_event(
            spark,
            "ERROR",
            f"TABLE_FILTER '{table_filter}' matched no active configs for job_name='{job_name}'",
            run_id,
            function_name="read_metadata",
            status="failed",
            additional_data={"job_name": job_name, "table_filter": table_filter},
        )
        raise ValueError(f"TABLE_FILTER '{table_filter}' matched no active configs")

    log_event(
        spark,
        "INFO",
        f"Table filter applied -- running {len(configs)} of {len(all_tables)} table(s)",
        run_id,
        function_name="read_metadata",
        status="info",
        additional_data={"filtered_tables": ", ".join([c.get("table_name") for c in configs])},
    )

# COMMAND ----------

# DBTITLE 1,Log Configs
for cfg in configs:
    ingestion_steps = cfg.get("ingestion_steps", {})
    layers = list(ingestion_steps.keys()) if ingestion_steps else []
    table_name = cfg.get("table_name", "UNKNOWN")

    log_event(
        spark,
        "INFO",
        f"Config found: {table_name} | layers={layers}",
        run_id,
        function_name="read_metadata",
        status="success",
        table_name=table_name,
        additional_data={"layers": ", ".join(layers), "job_name": job_name},
    )

# COMMAND ----------

# DBTITLE 1,Push Values
try:
    filtered_table_names = [c.get("table_name") for c in configs]
    pipeline_name = configs[0].get("source_name", job_name) if configs else job_name

    dbutils.jobs.taskValues.set(
        key="table_configs",
        value=json.dumps(configs, default=json_safe),
    )
    dbutils.jobs.taskValues.set(
        key="table_names",
        value=json.dumps(filtered_table_names),
    )
    dbutils.jobs.taskValues.set(
        key="pipeline_name",
        value=pipeline_name,
    )

    print("Task values pushed:")
    print(f"  table_configs -> {json.dumps(configs, default=json_safe)}")
    print(f"  table_names   -> {json.dumps(filtered_table_names)}")
    print(f"  pipeline_name -> {pipeline_name}")

    log_event(
        spark,
        "INFO",
        f"read_metadata complete -- {len(configs)} config(s) pushed to task values",
        run_id,
        function_name="read_metadata",
        status="success",
        additional_data={
            "config_count": str(len(configs)),
            "table_names": ", ".join(filtered_table_names),
            "job_name": job_name,
        },
    )

except Exception as e:
    log_event(
        spark,
        "WARNING",
        f"Task values not available (local run) -- {e}",
        run_id,
        function_name="read_metadata",
        status="info",
        additional_data={
            "job_name": job_name,
            "note": "running outside job context",
        },
    )