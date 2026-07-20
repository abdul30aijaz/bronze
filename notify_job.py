# Databricks notebook source
# MAGIC %run /Workspace/Shared/bronze/env

# COMMAND ----------

# DBTITLE 1, Imports
import json
from utils.spark_utils   import get_or_create_spark
from utils.logging_utils import get_run_id, log_event
from utils.notify_utils  import notify_pipeline_run

# COMMAND ----------

# DBTITLE 1, Helper — Normalize Job Name
def _normalize_job_name(job_name: str) -> str:
    """Strip the MDIF_ prefix and lowercase for control table lookup.
    e.g. MDIF_WALMART_NA_US_MONTHLY → walmart_na_us_monthly
    """
    name = job_name.strip()
    if name.upper().startswith("MDIF_"):
        name = name[5:]
    return name.lower()

# COMMAND ----------

# DBTITLE 1, Helper — Collect Per-Table Results from Log Table
def _collect_results_from_log(spark, job_run_id, table_names, log_fn=None):
    """Query pipeline_logs to build a per-table result dict for notification.

    Fetches pipeline_summary and rows_ingested per table, and computes
    dq_score as the row-completeness ratio: rows surviving empty-row and
    duplicate-row cleanup in process_raw (layer='dq_completeness'), i.e.
    final_row_count / original_row_count.

    This is logged in process_raw, before structural/GE validation runs in
    process_raw_trusted, so dq_score is available even for tables that later
    fail validation (e.g. a primary key check) -- it's never blank just
    because the table failed downstream.

    Returns all tables as FAILED if the log table is unavailable.
    """
    log_table = f"{CATALOG}.logging.pipeline_logs"

    try:
        #  Step 1: Latest pipeline_summary row per table for this job run 
        summary_df = spark.sql(f"""
            SELECT
                table_name,
                status,
                run_id,
                additional_data
            FROM {log_table}
            WHERE function_name                 = 'pipeline_summary'
              AND additional_data['job_run_id'] = '{job_run_id}'
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY table_name
                ORDER BY run_id DESC
            ) = 1
        """)
        summary_rows = {r["table_name"]: r for r in summary_df.collect()}

        # Step 2: Map each table to its own run_id
        table_run_id_map = {
            name: row["run_id"]
            for name, row in summary_rows.items()
        }

        # Step 3: Per-table rows_ingested and dq_score (completeness ratio)
        rows_map = {}
        dq_map   = {}

        for table_name, table_run_id in table_run_id_map.items():

            # rows_ingested from raw_trusted process log
            try:
                rows_row = spark.sql(f"""
                    SELECT additional_data['record_count'] AS record_count
                    FROM {log_table}
                    WHERE function_name            = 'process_raw_trusted'
                      AND status                   = 'success'
                      AND additional_data['layer'] = 'raw_trusted'
                      AND run_id                   = '{table_run_id}'
                      AND table_name               = '{table_name}'
                    ORDER BY run_id DESC
                    LIMIT 1
                """).first()
                if rows_row and rows_row["record_count"]:
                    rows_map[table_name] = int(rows_row["record_count"])
            except Exception:
                pass

            # dq_score = completeness ratio from process_raw
            try:
                completeness_row = spark.sql(f"""
                    SELECT
                        additional_data['passed_count'] AS passed_count,
                        additional_data['total_count']  AS total_count
                    FROM {log_table}
                    WHERE function_name            = 'process_raw'
                      AND additional_data['layer'] = 'dq_completeness'
                      AND run_id                   = '{table_run_id}'
                      AND table_name               = '{table_name}'
                    ORDER BY run_id DESC
                    LIMIT 1
                """).first()
                if completeness_row:
                    passed = int(completeness_row["passed_count"] or 0)
                    total  = int(completeness_row["total_count"]  or 0)
                    if total > 0:
                        dq_map[table_name] = round(passed / total, 4)
            except Exception:
                pass

    except Exception as exc:
        msg = f"Could not query {log_table}: {exc} — marking all tables FAILED"
        (log_fn(msg) if log_fn else print(msg))
        return {
            t: {
                "table_name":     t,
                "status":         "FAILED",
                "rows_ingested":  None,
                "dq_score":       None,
                "failure_reason": f"Log table unavailable: {exc}",
                "dq_message":     None,
            }
            for t in table_names
        }

    collected = {}
    for name, row in summary_rows.items():
        ad = row["additional_data"] or {}
        collected[name] = {
            "table_name":     name,
            "status":         "SUCCESS" if row["status"] == "success" else "FAILED",
            "rows_ingested":  rows_map.get(name),
            "dq_score":       dq_map.get(name),
            "failure_reason": ad.get("failure_reason") or None,
            "dq_message":     ad.get("dq_message")     or None,
        }

    return collected

# COMMAND ----------

# DBTITLE 1, Helper — Resolve Notify Config
def _resolve_notify_config(spark, job_name, log_fn=None):
    """Fetch the notify block for this job from the control table."""
    ct_job_name = _normalize_job_name(job_name)
    try:
        row = spark.sql(f"""
            SELECT notify
            FROM   {DEFAULT_CONTROL_TABLE}
            WHERE  job_name  = '{ct_job_name}'
              AND  is_active = true
            LIMIT 1
        """).first()

        if not row or not row["notify"]:
            raise ValueError(f"No notify config found in control table for job_name='{ct_job_name}'")

        notify_raw = row["notify"]

        # Handle Row, dict, or raw JSON string
        if hasattr(notify_raw, "asDict"):
            notify_dict = notify_raw.asDict()
            return {
                k: v.asDict() if hasattr(v, "asDict") else v
                for k, v in notify_dict.items()
            }
        if isinstance(notify_raw, dict):
            return notify_raw
        if isinstance(notify_raw, str):
            return json.loads(notify_raw)

        raise ValueError(f"Unexpected notify type: {type(notify_raw)}")

    except Exception as exc:
        msg = f"[notify] Failed to resolve notify config from control table: {exc}"
        (log_fn(msg) if log_fn else print(msg))
        raise

# COMMAND ----------

# DBTITLE 1, Helper — Resolve Job Name from Logs
def _resolve_job_name(spark, job_run_id, log_fn=None):
    """Look up job_name from pipeline_summary logs using job_run_id."""
    try:
        row = spark.sql(f"""
            SELECT job_name
            FROM   {CATALOG}.logging.pipeline_logs
            WHERE  additional_data['job_run_id'] = '{job_run_id}'
              AND  function_name                 = 'pipeline_summary'
              AND  job_name IS NOT NULL
            LIMIT 1
        """).first()

        if not row or not row["job_name"]:
            raise ValueError(f"Could not resolve job_name from logs for job_run_id='{job_run_id}'")

        return row["job_name"]

    except Exception as exc:
        msg = f"[notify] Failed to resolve job_name from logs: {exc}"
        (log_fn(msg) if log_fn else print(msg))
        raise

# COMMAND ----------

# DBTITLE 1, Main — Send Pipeline Notification
def main():
    """Collect per-table results from pipeline_logs and send the email notification.

    Resolves job context (job_run_id, run_link), reads table_names from task
    values, collects results from the log table, and dispatches via
    notify_pipeline_run. Any table missing a log entry is marked FAILED.
    """
    spark  = get_or_create_spark(app_name="MDIF-Notify")
    run_id = get_run_id(spark)

    # Resolve Databricks job context
    try:
        ctx        = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        job_run_id = str(ctx.jobRunId().get()).strip()
        job_id     = str(ctx.jobId().get()).strip()
        run_link   = f"https://{WORKSPACE_URL}/#job/{job_id}/run/{job_run_id}"
    except Exception as exc:
        raise RuntimeError(f"Could not resolve job context: {exc}")

    # Read table_names and pipeline_name from read_metadata task values 
    table_names_json = dbutils.jobs.taskValues.get(taskKey="read_metadata", key="table_names",   default="[]")
    pipeline_name    = dbutils.jobs.taskValues.get(taskKey="read_metadata", key="pipeline_name", default="MDIF Pipeline")
    table_names      = json.loads(table_names_json) if table_names_json else []

    if not table_names:
        raise ValueError("table_names task value is missing — nothing to report")

    log_event(spark, "INFO",
              f"notify_job started — job_run_id={job_run_id}, tables={table_names}",
              run_id, function_name="notify_job", status="running",
              additional_data={"tables": ", ".join(table_names), "job_run_id": job_run_id})

    # Convenience wrapper so helpers can log warnings without importing log_event
    def notify_log(msg):
        log_event(spark, "WARNING", msg, run_id,
                  function_name="notify_job", status="warning",
                  additional_data={"layer": "notification"})

    #  Resolve job_name and notify config 
    job_name      = _resolve_job_name(spark, job_run_id, log_fn=notify_log)
    notify_config = _resolve_notify_config(spark, job_name, log_fn=notify_log)

    #  Collect per-table results from log table 
    collected = _collect_results_from_log(
        spark       = spark,
        job_run_id  = job_run_id,
        table_names = table_names,
        log_fn      = notify_log,
    )

    # Ensure every expected table has a result entry 
    table_results = []
    for table_name in table_names:
        if table_name in collected:
            table_results.append(collected[table_name])
        else:
            notify_log(f"No pipeline_summary entry for '{table_name}' — marking FAILED")
            table_results.append({
                "table_name":       table_name,
                "status":           "FAILED",
                "rows_ingested":    None,
                "dq_score":         None,
                "completeness_pct": None,
                "failure_reason":   "No log entry found — task may have crashed before logging",
                "dq_message":       None,
            })

    overall       = "SUCCESS" if all(r["status"] == "SUCCESS" for r in table_results) else "PARTIAL/FAILED"
    success_count = sum(r["status"] == "SUCCESS" for r in table_results)

    log_event(spark, "INFO",
              f"Job-level status: {overall} — {success_count}/{len(table_results)} tables succeeded",
              run_id, function_name="notify_job", status="info",
              additional_data={
                  "job_run_id":     job_run_id,
                  "overall_status": overall,
                  "success_count":  str(success_count),
                  "total_count":    str(len(table_results)),
              })

    log_event(spark, "INFO",
              f"Results collected: { {r['table_name']: r['status'] for r in table_results} }",
              run_id, function_name="notify_job", status="info",
              additional_data={"job_run_id": job_run_id})

    # Dispatch email notification via Logic App 
    notify_pipeline_run(
        pipeline_name = pipeline_name,
        run_id        = job_run_id,
        run_link      = run_link,
        table_results = table_results,
        notify_config = notify_config,
        logic_app_url = LOGIC_APP_URL,
        log_fn        = notify_log,
    )

    log_event(spark, "INFO",
              f"Notification sent — {len(table_results)} table(s) reported",
              run_id, function_name="notify_job", status="success",
              additional_data={"table_count": str(len(table_results))})

# COMMAND ----------

# DBTITLE 1, Execute
# DISABLED: Notifications temporarily disabled during migration
# Uncomment after SMTP is configured
# main()

print("[NOTIFY] Notification task skipped — notifications disabled for now")