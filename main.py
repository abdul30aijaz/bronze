# Databricks notebook source
# DBTITLE 1,MDIF Pipeline - Main Orchestrator
"""MDIF Pipeline - Main Orchestrator

Orchestrates ingestion pipeline for a single table through three layers:
Landing → Raw → Raw Trusted. Reads table metadata from upstream task values,
processes layers sequentially, archives source files, and logs to Unity Catalog.

Parameters:
    table_name (widget): Table to process (must match control table entry)
"""

# COMMAND ----------

# DBTITLE 1,Load Environment
# MAGIC %run /Workspace/Shared/bronze/env

# COMMAND ----------

# DBTITLE 1,Import Dependencies
import json
from datetime import datetime

from ingestion_steps.landing import process_landing
from ingestion_steps.raw import process_raw
from ingestion_steps.raw_trusted import process_raw_trusted
from utils.path_utils import resolve_paths
from utils.archive_utils import move_to_archive
from utils.logging_utils import log_event, get_run_id, ensure_log_table
from utils.spark_utils import get_or_create_spark
from validation.validations import ValidationError

# COMMAND ----------

# DBTITLE 1,Layer Registry
# Maps layer names to processor functions for sequential execution
LAYER_HANDLERS = {
    "landing":     process_landing,
    "raw":         process_raw,
    "raw_trusted": process_raw_trusted
}

LAYER_ORDER = ["landing", "raw", "raw_trusted"]

# COMMAND ----------

# DBTITLE 1,Layer Enabled Helper
def _layer_enabled(layer_config):
    """Determines whether a layer config should be treated as active.

    Struct-derived configs pulled from the Unity Catalog control table always
    have every sub-field present as a key, even when the layer is meant to be
    skipped -- in that case every value is null, e.g.:
        {"source_path": None, "notebook_path": None, "file_format": None, ...}"""
    if not layer_config or not isinstance(layer_config, dict):
        return False
    return any(v is not None for v in layer_config.values())

# COMMAND ----------

# DBTITLE 1,Process Table
def _process_table(spark, table_config, ingestion_ts, run_id):
    """Orchestrates ingestion pipeline for a single table through all configured layers.

    Processes data sequentially through landing, raw, and raw_trusted layers,
    then archives source files. Each layer transforms and validates data before
    passing to the next layer.

    Supports partial runs:
        - raw + raw_trusted only → reads df from latest landing CSV in ADLS
        - raw_trusted only       → reads df from latest raw Parquet in ADLS

    A layer is considered "disabled" if its config is missing, not a dict, or
    a dict whose values are all null -- see _layer_enabled(). This handles
    control table rows where ingestion_steps.landing/raw/raw_trusted are
    present as struct sub-fields but hold NULL values for skipped layers.

    Args:
        spark: SparkSession instance
        table_config: Configuration dict containing table metadata and layer configs
        ingestion_ts: Timestamp string for this ingestion run
        run_id: Unique identifier for logging and tracking
    """
    table_name  = table_config.get("table_name", "")
    region      = table_config.get("region", "")
    market      = table_config.get("market", "")
    domain      = table_config.get("domain", "")
    source_name = table_config.get("source_name", "")
    job_name    = table_config.get("job_name", "")
    ingestion_steps  = table_config.get("ingestion_steps", {})

    # Layers that are actually enabled (non-null, usable config), used for
    # logging and for the archive decision
    active_layers = [k for k in ingestion_steps if _layer_enabled(ingestion_steps.get(k))]

    paths = resolve_paths(
        region=region, market=market, domain=domain,
        source_name=source_name, table_name=table_name,
        ingestion_ts=ingestion_ts, job_name=job_name,
        landing_base=LANDING_BASE,
        raw_base=RAW_BASE,
    )

    log_event(
        spark, "INFO",
        f"Processing table: {table_name} | layers={active_layers}", run_id,
        function_name="_process_table", status="running", table_name=table_name,
        additional_data={"layers": ", ".join(active_layers)},
    )

    df = None

    landing_enabled     = _layer_enabled(ingestion_steps.get("landing"))
    raw_enabled         = _layer_enabled(ingestion_steps.get("raw"))
    raw_trusted_enabled = _layer_enabled(ingestion_steps.get("raw_trusted"))

    # ── Partial run: only raw_trusted → read from latest RAW parquet ──────────
    if not landing_enabled and not raw_enabled and raw_trusted_enabled:
        raw_base  = f"{RAW_BASE}/{region}/{market}/{domain}/{source_name}/{table_name}"
        latest_ts = sorted([f.name.rstrip("/") for f in dbutils.fs.ls(raw_base)])[-1]
        raw_path  = f"{raw_base}/{latest_ts}"

        log_event(
            spark, "INFO",
            f"Partial run (raw_trusted only): {table_name} -- reading latest RAW parquet from {raw_path}",
            run_id, function_name="_process_table", status="running", table_name=table_name,
            additional_data={"layer": "raw_trusted", "source_path": raw_path, "latest_ts": latest_ts},
        )

        df = spark.read.format("parquet").option("mergeSchema", "true").load(raw_path)

    # ── Partial run: raw + raw_trusted → read from latest LANDING csv ─────────
    elif not landing_enabled and raw_enabled:
        landing_base_path = f"{LANDING_BASE}/{region}/{market}/{domain}/{source_name}/{table_name}"
        latest_ts         = sorted([f.name.rstrip("/") for f in dbutils.fs.ls(landing_base_path)])[-1]
        landing_path      = f"{landing_base_path}/{latest_ts}"

        log_event(
            spark, "INFO",
            f"Partial run (raw + raw_trusted): {table_name} -- reading latest LANDING csv from {landing_path}",
            run_id, function_name="_process_table", status="running", table_name=table_name,
            additional_data={"layer": "raw", "source_path": landing_path, "latest_ts": latest_ts},
        )

        df = spark.read.option("header", "true").csv(landing_path)

    # ── Full run: df starts as None, landing populates it ─────────────────────
    # No action needed here — normal flow

    # ── Process each layer in sequence ────────────────────────────────────────
    for layer_name in LAYER_ORDER:
        layer_config = ingestion_steps.get(layer_name)
        if not _layer_enabled(layer_config):
            continue

        handler = LAYER_HANDLERS[layer_name]

        log_event(
            spark, "INFO",
            f"Layer started: {table_name} -- {layer_name}", run_id,
            function_name="_process_table", status="running", table_name=table_name,
            additional_data={"layer": layer_name},
        )

        df = handler(spark, dbutils, df, layer_config, table_config, paths, run_id)

        log_event(
            spark, "INFO",
            f"Layer complete: {table_name} -- {layer_name}", run_id,
            function_name="_process_table", status="success", table_name=table_name,
            additional_data={"layer": layer_name},
        )

    # ── Archive source files ───────────────────────────────────────────────────
    try:
        landing_config = ingestion_steps.get("landing") or {}
        source_path    = landing_config.get("source_path") if _layer_enabled(landing_config) else ""
        source_path    = source_path or ""

        if source_path:
            archived_files = move_to_archive(
                dbutils, source_path, table_name,
                log_fn=lambda msg: log_event(
                    spark, "INFO", msg, run_id,
                    function_name="_process_table", status="success",
                    table_name=table_name, additional_data={"layer": "archive"},
                ),
            )
            log_event(
                spark, "INFO",
                f"Archive complete: {table_name} -- {len(archived_files)} file(s) archived", run_id,
                function_name="_process_table", status="success", table_name=table_name,
                additional_data={"layer": "archive", "archived_files": str(len(archived_files))},
            )
        else:
            log_event(
                spark, "INFO",
                f"Archive skipped: {table_name} -- no source_path (custom/partial ingestion)", run_id,
                function_name="_process_table", status="info", table_name=table_name,
                additional_data={"layer": "archive"},
            )
    except Exception as exc:
        log_event(
            spark, "ERROR",
            f"Archive FAILED: {table_name} -- {exc}", run_id,
            function_name="_process_table", status="failed", table_name=table_name,
            additional_data={"layer": "archive", "error": str(exc)},
        )
        raise

    log_event(
        spark, "INFO",
        f"Table fully ingested: {table_name} -- all layers complete", run_id,
        function_name="_process_table", status="success", table_name=table_name,
        additional_data={"layers_processed": ", ".join(active_layers)},
    )


# COMMAND ----------

# DBTITLE 1,Run Pipeline
def run(spark, table_name):
    """Manages the complete pipeline execution lifecycle.

    Initializes Spark session, retrieves table config from upstream task values,
    executes the layer pipeline, handles errors, and logs pipeline summary.

    Args:
        spark: SparkSession instance (or None to create new)
        table_name: Name of the table to process
    """
    spark        = get_or_create_spark(spark=spark, app_name=f"MDIF-Pipeline-{table_name}")
    ingestion_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_id       = get_run_id(spark)

    try:
        ctx        = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        job_run_id = str(ctx.jobRunId().get()).strip()
    except Exception:
        job_run_id = run_id

    ensure_log_table(spark)

    log_event(
        spark, "INFO",
        f"Job started: {table_name}", run_id,
        function_name="run", status="running", table_name=table_name,
        additional_data={"ingestion_ts": ingestion_ts, "job_run_id": job_run_id},
    )

    try:
        table_configs_json = dbutils.jobs.taskValues.get(taskKey="read_metadata", key="table_configs")
        table_configs      = json.loads(table_configs_json)
    except Exception as e:
        log_event(
            spark, "ERROR",
            f"Failed to read table_configs from task values: {e}", run_id,
            function_name="run", status="failed", table_name=table_name,
        )
        raise RuntimeError(f"Could not read table_configs: {e}")

    table_config = next(
        (c for c in table_configs if c.get("table_name") == table_name), None
    )

    if not table_config:
        log_event(
            spark, "ERROR",
            f"No config found for table_name='{table_name}'", run_id,
            function_name="run", status="failed", table_name=table_name,
            additional_data={
                "available_tables": ", ".join([c.get("table_name", "?") for c in table_configs])
            },
        )
        raise ValueError(
            f"No config found for table_name='{table_name}'. "
            f"Available tables: {[c.get('table_name') for c in table_configs]}"
        )

    try:
        _process_table(spark, table_config, ingestion_ts, run_id)

        log_event(spark, "INFO", f"Job complete: {table_name} successfully ingested", run_id,
                  function_name="run", status="success", table_name=table_name)
        log_event(
            spark, "INFO",
            f"Pipeline summary: {table_name} -- success", run_id,
            function_name="pipeline_summary", status="success", table_name=table_name,
            additional_data={"job_run_id": job_run_id},
        )

    except ValidationError as ve:
        log_event(
            spark, "ERROR",
            f"Table FAILED at validation: {table_name} -- "
            f"{len(ve.failed_checks)} check(s) failed -- "
            f"details: {' | '.join(ve.failed_checks[:5])}"
            f"{'...' if len(ve.failed_checks) > 5 else ''}",
            run_id, function_name="run", status="failed", table_name=table_name,
            additional_data={"failed_checks": " | ".join(ve.failed_checks)},
        )
        log_event(
            spark, "INFO",
            f"Pipeline summary: {table_name} -- failed", run_id,
            function_name="pipeline_summary", status="failed", table_name=table_name,
            additional_data={
                "job_run_id":     job_run_id,
                "failure_reason": "Validation failed",
                "dq_message":     " | ".join(ve.failed_checks[:3]),
            },
        )
        raise

    except Exception as exc:
        log_event(
            spark, "ERROR",
            f"Table FAILED: {table_name} -- {type(exc).__name__}: {str(exc)[:200]}", run_id,
            function_name="run", status="failed", table_name=table_name,
            additional_data={"error_type": type(exc).__name__, "error": str(exc)[:200]},
        )
        log_event(
            spark, "INFO",
            f"Pipeline summary: {table_name} -- failed", run_id,
            function_name="pipeline_summary", status="failed", table_name=table_name,
            additional_data={
                "job_run_id":     job_run_id,
                "failure_reason": f"{type(exc).__name__}: {str(exc)[:150]}",
            },
        )
        raise


# COMMAND ----------

# DBTITLE 1,Main Entry
def main():
    """Notebook entry point for Databricks job execution.

    Creates widget for table_name parameter, validates it, and calls run function.
    Enables parameterized execution where each job task processes a different table.
    """
    spark = get_or_create_spark(app_name="MDIF-Main")

    dbutils.widgets.text("table_name", "", "Table Name")
    table_name = dbutils.widgets.get("table_name").strip()

    if not table_name:
        raise ValueError(
            "table_name widget is required. "
            "Please provide a table name that matches a control table entry."
        )

    run(spark, table_name=table_name)


# COMMAND ----------

# DBTITLE 1,Execute
main()