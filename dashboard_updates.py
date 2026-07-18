# Databricks notebook source
# DBTITLE 1, Notebook Description
"""Dashboard Update — runs at the end of each job to upsert monitoring records.

One row is written per (job_id, table_name) pair, since a single job can drive
multiple tables through the ingestor_iteration For Each task. Each table's row
gets its OWN status, matched via pipeline_logs' pipeline_summary entries for
this run's job_run_id -- the durable source of truth for per-table status,
written on both success and failure paths regardless of Jobs API quirks.
"""

# COMMAND ----------
# DBTITLE 1, Dashboard update
import requests
import json
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, lower
from delta.tables import DeltaTable
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

spark = SparkSession.builder.getOrCreate()

workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
token = ctx.apiToken().get()

current_job_id = ctx.jobId().get()
current_job_name = ctx.tags().get("jobName").get()

normalized_name = current_job_name.upper().replace("MDIF_", "").lower()

print(f"Running update for: {current_job_name} (normalized: {normalized_name}, id={current_job_id})")

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

TARGET_TABLE = "cdap_mars_pc_mdif.job_monitor.job_monitoring_dashboard_data"
LOG_TABLE = "cdap_mars_pc_mdif.logging.pipeline_logs"

CADENCE_THRESHOLDS = {
    "DAILY": 26,
    "WEEKLY": 170,
    "MONTHLY": 745,
    "QUARTERLY": 2208,
    "YEARLY": 8760,
}



def extract_cadence(job_name: str) -> str:
    """Extract job cadence (DAILY/WEEKLY/MONTHLY/QUARTERLY/YEARLY) from job name suffix."""
    last = job_name.upper().split("_")[-1]
    return last if last in CADENCE_THRESHOLDS else "UNKNOWN"


def extract_schema_from_job_name(job_name: str) -> str:
    """Derive schema name from job name by removing prefixes and cadence tags."""
    name = job_name.upper().replace("MDIF_", "")

    for cadence in ["_DAILY", "_WEEKLY", "_MONTHLY", "_QUARTERLY", "_YEARLY"]:
        if name.endswith(cadence):
            name = name[:-len(cadence)]
            break

    return name.replace("_NA_", "_").replace("_EU_", "_").replace("_APAC_", "_").lower()


def get_table_last_modified(schema_name: str, table_name: str):
    """
    Return last modified timestamp of a Delta table if it exists.

    Args:
        schema_name: Target schema name.
        table_name: Target table name.

    Returns:
        Timestamp or None if table/schema not found.
    """
    if not schema_name or not table_name:
        return None

    try:
        schemas = spark.sql("SHOW SCHEMAS IN cdap_mars_pc_mdif").collect()
        schema_names = [r.databaseName for r in schemas]

        if schema_name not in schema_names:
            return None
    except Exception:
        return None

    try:
        detail = spark.sql(f"DESCRIBE DETAIL cdap_mars_pc_mdif.{schema_name}.{table_name}")
        return detail.select("lastModified").first()[0]
    except Exception:
        return None


def get_latest_run(job_id: str):
    """
    Fetch latest Databricks job run using Jobs API.

    Args:
        job_id: Databricks job ID.

    Returns:
        Latest run dictionary or None.
    """
    url = f"https://{workspace_url}/api/2.1/jobs/runs/list"
    start_time_ms = int((datetime.now() - timedelta(days=1)).timestamp() * 1000)

    response = requests.get(
        url,
        headers=headers,
        params={"job_id": job_id, "start_time_from": start_time_ms, "limit": 1}
    )
    response.raise_for_status()

    runs = response.json().get("runs", [])
    return runs[0] if runs else None


def get_run_details(run_id):
    """Fetch full run details (used only as a fallback for the overall status
    when nothing has been logged to pipeline_logs yet — e.g. run just started)."""
    url = f"https://{workspace_url}/api/2.1/jobs/runs/get"
    response = requests.get(url, headers=headers, params={"run_id": run_id})
    response.raise_for_status()
    return response.json()


def build_table_status_map(job_run_id: str):
    """
    Map table_name -> (status, error_message) by reading this run's
    pipeline_summary log rows from pipeline_logs, keyed by job_run_id.

    This is the reliable source of truth for per-table status: it's written
    durably by run() in the ingestor_iteration notebook on BOTH success and
    failure paths (before any exception propagates), with the table's real
    resolved name -- unlike the Jobs API, whose base_parameters only expose
    the unresolved {{input}} template and whose task values aren't
    retrievable via REST at all.
    """
    rows = (
        spark.table(LOG_TABLE)
        .filter(col("function_name") == lit("pipeline_summary"))
        .withColumn("log_job_run_id", col("additional_data")["job_run_id"])
        .filter(col("log_job_run_id") == lit(str(job_run_id)))
        .withColumn("failure_reason", col("additional_data")["failure_reason"])
        .withColumn("dq_message", col("additional_data")["dq_message"])
        .orderBy(col("timestamp").desc())
        .select("table_name", "status", "failure_reason", "dq_message")
        .collect()
    )

    mapping = {}
    for r in rows:
        if not r["table_name"] or r["table_name"] in mapping:
            continue  # keep only the most recent entry per table (already ordered desc)
        status = (r["status"] or "UNKNOWN").upper()
        error_message = r["dq_message"] or r["failure_reason"] or ""
        mapping[r["table_name"]] = (status, error_message)

    return mapping


# Fetch ALL control table rows for current job (a job can drive multiple tables)
control_rows = (
    spark.table("cdap_mars_pc_mdif.metadata.metadata_control_table")
    .filter(lower(col("job_name")) == lit(normalized_name))
    .withColumn("source_path", col("ingestion_steps.landing.source_path"))
    .select("table_name", "source_path", "region", "market", "domain", "source_name", "sync_timestamp")
    .collect()
)

if not control_rows:
    dbutils.notebook.exit("Not in control table")

# Fetch latest job run
run = get_latest_run(current_job_id)

if run is None:
    dbutils.notebook.exit("No run found")

run_id = run.get("run_id")

# Build table_name -> (status, error_message) from pipeline_logs' pipeline_summary
# rows for this job_run_id -- the reliable source of truth, since it's written
# durably (before any exception can propagate) on both success and failure,
# with the table's real resolved name.
table_status_map = build_table_status_map(run_id)

if table_status_map:
    statuses = [s for s, _ in table_status_map.values()]
    if any(s == "FAILED" for s in statuses):
        overall_status = "FAILED"
    elif all(s == "SUCCESS" for s in statuses):
        overall_status = "SUCCESS"
    else:
        overall_status = "UNKNOWN"
else:
    # Nothing logged yet for this run (e.g. it just started, or the log table
    # write hasn't landed) -> fall back to the run-level Jobs API state so we
    # at least report RUNNING accurately rather than guessing.
    run_data = get_run_details(run_id)
    state = run_data.get("state", {})
    overall_status = state.get("result_state") or state.get("life_cycle_state", "RUNNING")

now = datetime.now()
cadence = extract_cadence(current_job_name)
threshold = CADENCE_THRESHOLDS.get(cadence)
schema_name = extract_schema_from_job_name(current_job_name)

final_schema = StructType([
    StructField("job_id", StringType(), True),
    StructField("job_name", StringType(), True),
    StructField("run_id", StringType(), True),
    StructField("status", StringType(), True),
    StructField("error_message", StringType(), True),
    StructField("trigger_type", StringType(), True),
    StructField("run_url", StringType(), True),
    StructField("table_name", StringType(), True),
    StructField("region", StringType(), True),
    StructField("market", StringType(), True),
    StructField("domain", StringType(), True),
    StructField("source_name", StringType(), True),
    StructField("sync_timestamp", TimestampType(), True),
    StructField("cadence", StringType(), True),
    StructField("last_table_update_time", TimestampType(), True),
    StructField("data_age_hours", DoubleType(), True),
    StructField("freshness_status", StringType(), True),
    StructField("refresh_timestamp", TimestampType(), True),
])

rows_to_write = []

for ctrl in control_rows:
    if ctrl["table_name"] not in table_status_map:
        # This table wasn't part of this run's iterations at all (e.g. a
        # partial/test run, or it's excluded from this run's input list).
        # Don't touch its row — writing a guessed status here would falsely
        # imply this table was processed when it wasn't.
        print(f"Skipping {ctrl['table_name']}: no matching pipeline_summary log entry for this run — leaving existing row untouched")
        continue

    table_status, table_error = table_status_map[ctrl["table_name"]]

    last_modified = get_table_last_modified(schema_name, ctrl["table_name"])

    if last_modified is None:
        data_age_hours = None
        freshness_status = "MISSING"
    else:
        data_age_hours = round((now - last_modified).total_seconds() / 3600, 2)
        freshness_status = (
            "UNKNOWN" if threshold is None else
            "FRESH" if data_age_hours <= threshold else "STALE"
        )

    rows_to_write.append({
        "job_id": current_job_id,
        "job_name": current_job_name,
        "run_id": str(run_id),
        "status": table_status,
        "error_message": table_error,
        "trigger_type": run.get("trigger", "MANUAL"),
        "run_url": f"https://{workspace_url}/jobs/{current_job_id}/runs/{run_id}",
        "table_name": ctrl["table_name"],
        "region": ctrl["region"],
        "market": ctrl["market"],
        "domain": ctrl["domain"],
        "source_name": ctrl["source_name"],
        "sync_timestamp": ctrl["sync_timestamp"],
        "cadence": cadence,
        "last_table_update_time": last_modified,
        "data_age_hours": data_age_hours,
        "freshness_status": freshness_status,
        "refresh_timestamp": now,
    })

if not rows_to_write:
    print(f"No tables matched to a completed iteration for {current_job_name} this run — nothing to update.")
    dbutils.notebook.exit("No matched iterations")

update_df = spark.createDataFrame(rows_to_write, schema=final_schema)

# Composite key: a job can now write multiple rows (one per table), so the
# merge key must be (job_id, table_name), not job_id alone.
DeltaTable.forName(spark, TARGET_TABLE).alias("t").merge(
    update_df.alias("s"),
    "t.job_id = s.job_id AND t.table_name = s.table_name"
).whenMatchedUpdateAll(
).whenNotMatchedInsertAll(
).execute()

print(f"Updated {len(rows_to_write)} monitoring row(s) for {current_job_name} — overall={overall_status}")
