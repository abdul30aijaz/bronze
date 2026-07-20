# Databricks notebook source
# DBTITLE 1, Notebook Description
"""Metadata validation framework for MDIF ingestion configs."""

# COMMAND ----------

# DBTITLE 1, Magic Command
# MAGIC %run /Workspace/Shared/bronze/env

# COMMAND ----------

# DBTITLE 1, Imports
import json
from pyspark.sql import SparkSession
from utils.metadata_utils import read_json_files


# COMMAND ----------

# DBTITLE 1, Constants — Validation Rules
# Required top-level metadata fields
REQUIRED_FIELDS = ["source_name", "region", "market", "domain", "ingestion_steps"]

# Required fields per ingestion layer
LAYER_REQUIRED_FIELDS = {
    "raw": ["schema_config"],
    "raw_trusted": ["load_type"],
}

# Supported ingestion formats and constraints
VALID_FORMATS = {"excel", "csv", "parquet", "json", "delta"}
FORMATS_REQUIRING_OPTIONS = {"excel", "csv", "json"}
VALID_TRIGGER_TYPES = {"schedule", "file_arrival"}


# COMMAND ----------

# DBTITLE 1, Helper — Detect API Landing
def _is_api_landing(landing_cfg: dict) -> bool:
    """Identify API-based ingestion (presence of 'parameters')."""
    return "parameters" in landing_cfg


# COMMAND ----------

# DBTITLE 1, Validator — Top-Level Fields
def _validate_top_level(data: dict, errors: list) -> None:
    """Validate required metadata fields at root level."""
    for field in REQUIRED_FIELDS:
        if field not in data or data[field] is None:
            errors.append(f"Missing required field: '{field}'")

    if "is_active" in data and not isinstance(data["is_active"], bool):
        errors.append(
            f"'is_active' must be boolean, got {type(data['is_active']).__name__}"
        )


# COMMAND ----------

# DBTITLE 1, Validator — Landing Layer
def _validate_landing(landing_cfg: dict, errors: list) -> None:
    """Validate landing configuration (file or API ingestion)."""

    # API-based ingestion validation
    if _is_api_landing(landing_cfg):
        parameters = landing_cfg.get("parameters")

        if not isinstance(parameters, dict):
            errors.append("'parameters' must be a dict (api source)")
        elif not parameters.get("notebook_path"):
            errors.append("Missing 'notebook_path' in landing.parameters")

    # File-based ingestion validation
    else:
        for field in ["source_path", "file_format"]:
            if not landing_cfg.get(field):
                errors.append(f"Missing '{field}' in landing")

        file_format = (landing_cfg.get("file_format") or "").lower()

        if file_format and file_format not in VALID_FORMATS:
            errors.append(f"Invalid file_format '{file_format}'")

        if file_format in FORMATS_REQUIRING_OPTIONS:
            opts_key = next((k for k in landing_cfg if k.endswith("_options")), None)

            if not opts_key:
                errors.append(
                    f"Missing '{file_format}_options' block for format '{file_format}'"
                )
            elif not isinstance(landing_cfg[opts_key], dict):
                errors.append(f"'{opts_key}' must be a dict")

        pii_columns = landing_cfg.get("pii_columns")

        if pii_columns is None:
            errors.append("'pii_columns' is required (use empty list if none)")
        elif not isinstance(pii_columns, list):
            errors.append("'pii_columns' must be a list")
        elif len(pii_columns) != len(set(pii_columns)):
            errors.append("'pii_columns' contains duplicates")

# COMMAND ----------

# DBTITLE 1, Validator — Raw Layer
def _validate_raw(raw_cfg: dict, errors: list) -> None:
    """Validate raw ingestion layer configuration."""

    for field in LAYER_REQUIRED_FIELDS["raw"]:
        if not raw_cfg.get(field):
            errors.append(f"Missing '{field}' in 'raw'")

    dedup = raw_cfg.get("deduplication")
    if dedup is not None and not isinstance(dedup, bool):
        errors.append(f"'deduplication' must be boolean, got {type(dedup).__name__}")


# COMMAND ----------

# DBTITLE 1, Validator — Raw Trusted Layer
def _validate_raw_trusted(rt_cfg: dict, errors: list) -> None:
    """Validate raw_trusted ingestion layer configuration."""

    for field in LAYER_REQUIRED_FIELDS["raw_trusted"]:
        if not rt_cfg.get(field):
            errors.append(f"Missing '{field}' in 'raw_trusted'")

    schema_evo = rt_cfg.get("schema_evolution")
    if schema_evo is not None and not isinstance(schema_evo, bool):
        errors.append(f"'schema_evolution' must be boolean, got {type(schema_evo).__name__}")


# COMMAND ----------

# DBTITLE 1, Validator — Notification Config
def _validate_notify(notify_cfg: dict, errors: list) -> None:
    """Validate notification email configuration."""

    if not isinstance(notify_cfg, dict):
        errors.append(f"'notify' must be a dict, got {type(notify_cfg).__name__}")
        return

    emails = notify_cfg.get("emails")

    if emails is None:
        errors.append("'notify.emails' is required")
    elif not isinstance(emails, list):
        errors.append("'notify.emails' must be a list")
    elif not emails:
        errors.append("'notify.emails' cannot be empty")
    elif not all(isinstance(e, str) for e in emails):
        errors.append("'notify.emails' must contain only strings")


# COMMAND ----------

# DBTITLE 1, Validator — Trigger Config
def _validate_trigger(trigger_cfg: dict, errors: list) -> None:
    """Validate ingestion trigger configuration."""

    if not isinstance(trigger_cfg, dict):
        errors.append(f"'trigger' must be a dict, got {type(trigger_cfg).__name__}")
        return

    trigger_type = trigger_cfg.get("type")

    # manual trigger → no validation needed
    if not trigger_type:
        return

    if trigger_type not in VALID_TRIGGER_TYPES:
        errors.append(f"Invalid trigger.type '{trigger_type}'")
        return

    if trigger_type == "schedule":
        if not trigger_cfg.get("cron"):
            errors.append("'trigger.cron' required for schedule trigger")
        if not trigger_cfg.get("timezone"):
            errors.append("'trigger.timezone' required for schedule trigger")

# COMMAND ----------

# DBTITLE 1, Validator — Schema Entries
def _validate_schema(data: dict, errors: list) -> None:
    """Validate schema definition consistency."""

    for i, col in enumerate(data.get("schema", [])):
        if "column_name" not in col or "data_type" not in col:
            errors.append(f"Schema entry {i} missing required fields")


# COMMAND ----------

# DBTITLE 1, Entry Point — Validate Single Metadata Entry
def validate_entry(job_name: str, table_name: str, data: dict) -> list:
    """Run full validation suite for a single metadata entry."""

    errors: list = []

    _validate_top_level(data, errors)

    ingestion_steps = data.get("ingestion_steps")
    if not ingestion_steps:
        errors.append("Missing 'ingestion_steps'")
        return errors

    landing_cfg = ingestion_steps.get("landing")
    if landing_cfg:
        _validate_landing(landing_cfg, errors)

    raw_cfg = ingestion_steps.get("raw")
    if raw_cfg:
        _validate_raw(raw_cfg, errors)

    rt_cfg = ingestion_steps.get("raw_trusted")
    if rt_cfg:
        _validate_raw_trusted(rt_cfg, errors)

    if data.get("notify") is not None:
        _validate_notify(data["notify"], errors)

    if data.get("trigger") is not None:
        _validate_trigger(data["trigger"], errors)

    _validate_schema(data, errors)

    return errors


# COMMAND ----------

# DBTITLE 1, Main — Run Validation Pipeline
def run() -> None:
    """
    Execute metadata validation for all configs in the job.

    Fails fast if any invalid metadata is found.
    """

    spark = SparkSession.builder.appName("MDIF-ValidateMetadata").getOrCreate()

    current_user = spark.sql("SELECT current_user()").collect()[0][0]

    print(
        f"Task 2 — Validate Metadata | user={current_user} | "
        f"job={JOB_NAME} | dir={METADATA_DIR}"
    )

    entries = read_json_files(dbutils, METADATA_DIR)
    all_errors = {}

    for job_name, table_name, data in entries:
        errors = validate_entry(job_name, table_name, data)

        if errors:
            all_errors[f"{job_name}/{table_name}"] = errors
            print(f"FAIL {job_name}/{table_name}")
            for err in errors:
                print(f"  {err}")
        else:
            landing = data.get("ingestion_steps", {}).get("landing", {})
            source_kind = "api" if _is_api_landing(landing) else "file"

            detail = (
                landing.get("parameters", {}).get("notebook_path")
                if source_kind == "api"
                else landing.get("file_format")
            )

            trigger_log = (data.get("trigger") or {}).get("type") or "manual"

            print(
                f"ok {job_name}/{table_name} "
                f"| source={source_kind} | detail={detail} | trigger={trigger_log}"
            )

    if all_errors:
        raise ValueError(
            f"Validation failed for {len(all_errors)} source(s): "
            f"{list(all_errors.keys())}"
        )

    print(f"Validation complete | {len(entries)} source(s) passed")


# COMMAND ----------

# DBTITLE 1, Execute
run()