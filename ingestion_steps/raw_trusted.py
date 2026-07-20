"""Raw trusted layer processor for the MDIF pipeline.
Runs structural validation (the 4 checks in validations.py), schema evolution,
and writes the result to Delta. On structural validation failure, failed checks
are logged to the DQ error table before the error is re-raised.
"""

# Define CATALOG directly (cannot import from notebook)
CATALOG = "workspace"

from connectors.delta_connector import (
    write_delta, _is_uc_table, _table_exists, _read_delta_df,
)
from validation.validations import run_detailed, ValidationError
from utils.logging_utils import log_event
from utils.dq_logging_utils import DQValidator


def _log_validation_results(spark, results, table_name, run_id, row_count):
    """Log each structural validation result and summarize pass/fail counts.

    Args:
        spark: Active SparkSession used for logging.
        results: List of validation result dicts with 'name', 'passed', 'details'.
        table_name: Name of the table being validated.
        run_id: Identifier for the current pipeline run.
        row_count: Number of rows in the DataFrame being validated.

    Returns:
        Tuple of (passed_names, failed_names, score_str) summarizing the results.
    """
    total        = len(results)
    passed_names = []
    failed_names = []

    for v_result in results:
        v_name    = v_result["name"]
        v_passed  = v_result["passed"]
        v_details = v_result["details"]

        if v_passed:
            passed_names.append(v_name)
            log_event(
                spark, "INFO",
                f"[{v_name}] PASSED: {table_name} -- {v_details}",
                run_id, function_name="process_raw_trusted",
                record_count=row_count, status="success", table_name=table_name,
                additional_data={"validation_name": v_name, "layer": "validation"},
            )
        else:
            failed_names.append(v_name)
            log_event(
                spark, "ERROR",
                f"[{v_name}] FAILED: {table_name} -- {v_details}",
                run_id, function_name="process_raw_trusted",
                record_count=row_count, status="failed", table_name=table_name,
                additional_data={
                    "validation_name": v_name,
                    "layer":           "validation",
                    "error":           v_details,
                },
            )

    score_str = f"{len(passed_names)}/{total}"
    return passed_names, failed_names, score_str


def _apply_schema_evolution(spark, incoming_df, path):
    """Add any new columns from incoming_df into the existing Delta table at path.

    Uses ALTER TABLE for Unity Catalog tables; sets autoMerge for path-based tables.
    Returns (changes_dict, altered_columns). If table does not exist yet, no-ops.

    Args:
        spark: Active SparkSession used to inspect and alter the Delta table.
        incoming_df: Incoming DataFrame whose schema is compared against the existing table.
        path: Table path or fully-qualified UC table name to evolve.

    Returns:
        Tuple of (changes dict with new/removed columns and has_changes flag, list of altered column names).
    """
    if not _table_exists(spark, path):
        return {"new_columns": [], "removed_columns": [], "has_changes": False}, []

    existing_cols = set(_read_delta_df(spark, path).columns)
    incoming_cols = set(incoming_df.columns)
    new_columns   = sorted(incoming_cols - existing_cols)
    changes       = {
        "new_columns":     new_columns,
        "removed_columns": sorted(existing_cols - incoming_cols),
        "has_changes":     bool(new_columns),
    }

    if not changes["has_changes"]:
        return changes, []

    altered = []
    if _is_uc_table(path):
        for col_name in new_columns:
            col_type = incoming_df.schema[col_name].dataType.simpleString()
            try:
                spark.sql(f"ALTER TABLE {path} ADD COLUMNS ({col_name} {col_type})")
                altered.append(col_name)
            except Exception as exc:
                raise RuntimeError(
                    f"Schema evolution failed: could not add column '{col_name}' "
                    f"({col_type}) to {path} -- {exc}"
                ) from exc
    else:
        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
        altered = new_columns

    return changes, altered


def process_raw_trusted(spark, dbutils, df, layer_config, table_config, paths, run_id):
    """Run structural validation, schema evolution, and write the DataFrame to the raw trusted Delta table.

    Args:
        spark: Active SparkSession used for processing and logging.
        dbutils: Databricks utilities object (passed through, not directly used here).
        df: Input DataFrame to validate and write.
        layer_config: Configuration dict for this layer (load_type, primary_key, enable_expiration, schema_evolution).
        table_config: Configuration dict describing the table (table_name, source_name, market, job_name, schema).
        paths: Dict of paths for the pipeline, including the "raw_trusted" output path.
        run_id: Identifier for the current pipeline run.

    Returns:
        The processed DataFrame after validation (post-write).
    """
    table_name        = table_config.get("table_name")
    source_name       = table_config.get("source_name", "")
    market            = table_config.get("market", "")
    job_name          = table_config.get("job_name", "")
    load_type         = layer_config.get("load_type", "append")
    primary_key       = layer_config.get("primary_key") or []
    enable_expiration = layer_config.get("enable_expiration", False)
    schema_evolution  = layer_config.get("schema_evolution", False)
    row_count         = df.count()

    # Target catalog/schema for the DQ error table — computed up front so it's
    # available whether validation passes or fails.
    source_lower   = source_name.lower().strip().replace(" ", "_")
    market_lower   = market.lower().strip().replace(" ", "_")
    target_schema  = f"{source_lower}_{market_lower}"
    target_catalog = CATALOG

    # Structural validation — the 4 checks defined in validation/validations.py:
    # duplicate_row_validation, row_count_validation, primary_key_validation, schema_validation
    log_event(
        spark, "INFO",
        f"Validation started: {table_name}", run_id,
        function_name="process_raw_trusted", record_count=row_count,
        status="running", table_name=table_name,
        additional_data={"layer": "validation"},
    )

    try:
        validation_results = run_detailed(df, table_config)
    except ValidationError as ve:
        passed_names, failed_names, score = _log_validation_results(
            spark, ve.results, table_name, run_id, row_count
        )
        log_event(
            spark, "ERROR",
            f"Validation score: {table_name} -- {score} passed | "
            f"PASSED: [{', '.join(passed_names) if passed_names else 'none'}] | "
            f"FAILED: [{', '.join(failed_names) if failed_names else 'none'}] -- "
            f"fix failed validations before data can be promoted to trusted layer",
            run_id, function_name="process_raw_trusted",
            record_count=row_count, status="failed", table_name=table_name,
            additional_data={
                "layer":              "validation",
                "validation_score":   score,
                "passed_validations": ", ".join(passed_names),
                "failed_validations": ", ".join(failed_names),
                "failed_checks":      " | ".join(ve.failed_checks),
            },
        )

        # Persist the failed checks to the DQ error table (same table/schema as before).
        try:
            validator = DQValidator(
                spark=spark,
                job_name=job_name or "mdif_pipeline",
                catalog=target_catalog,
                schema=target_schema,
                table_name=table_name,
            )
            n_logged = validator.log_validation_failures(
                ve.results, df=df, primary_key=primary_key, severity="CRITICAL"
            )
            if n_logged:
                log_event(
                    spark, "INFO",
                    f"Logged {n_logged} failed validation record(s) → {validator.error_table}",
                    run_id, function_name="process_raw_trusted",
                    record_count=row_count, status="info", table_name=table_name,
                    additional_data={"layer": "validation", "error_table": validator.error_table},
                )
        except Exception as log_exc:
            log_event(
                spark, "ERROR",
                f"Failed to log DQ error records: {table_name} -- {log_exc}", run_id,
                function_name="process_raw_trusted", status="failed", table_name=table_name,
                additional_data={"layer": "validation", "error": str(log_exc)},
            )

        raise

    passed_names, failed_names, score = _log_validation_results(
        spark, validation_results, table_name, run_id, row_count
    )
    log_event(
        spark, "INFO",
        f"Validation score: {table_name} -- {score} passed | all validations successful",
        run_id, function_name="process_raw_trusted",
        record_count=row_count, status="success", table_name=table_name,
        additional_data={
            "validation_score": score,
            "validations_run":  str(len(validation_results)),
            "layer":            "validation",
        },
    )

    # Schema evolution
    if schema_evolution:
        try:
            changes, altered_columns = _apply_schema_evolution(
                spark=spark, incoming_df=df, path=paths["raw_trusted"],
            )
            if changes["has_changes"]:
                log_event(
                    spark, "WARNING",
                    f"Schema evolution detected new column(s): {table_name} -- "
                    f"added {len(altered_columns)} column(s): {altered_columns}",
                    run_id, function_name="process_raw_trusted",
                    record_count=row_count, status="warning", table_name=table_name,
                    additional_data={
                        "layer":           "raw_trusted",
                        "new_columns":     str(changes["new_columns"]),
                        "removed_columns": str(changes["removed_columns"]),
                        "altered_columns": str(altered_columns),
                    },
                )
            else:
                log_event(
                    spark, "INFO",
                    f"Schema evolution complete: {table_name} -- no new columns detected",
                    run_id, function_name="process_raw_trusted",
                    record_count=row_count, status="success", table_name=table_name,
                    additional_data={
                        "layer":           "raw_trusted",
                        "new_columns":     "none",
                        "removed_columns": str(changes["removed_columns"]),
                    },
                )
        except Exception as exc:
            log_event(
                spark, "ERROR",
                f"Schema evolution FAILED: {table_name} -- {exc}", run_id,
                function_name="process_raw_trusted", status="failed", table_name=table_name,
                additional_data={"layer": "raw_trusted", "error": str(exc)},
            )
            raise
    else:
        log_event(
            spark, "INFO",
            f"Schema evolution skipped: {table_name} -- not enabled in config", run_id,
            function_name="process_raw_trusted", status="info", table_name=table_name,
            additional_data={"layer": "raw_trusted"},
        )

    # ── Step: Write delta ─────────────────────────────────────────────────────
    try:
        if enable_expiration and load_type.lower() in ("scd_type2", "scd2"):
            log_event(
                spark, "INFO",
                f"Expiration enabled for {table_name} using {load_type}", run_id,
                function_name="process_raw_trusted", status="info", table_name=table_name,
                additional_data={
                    "load_type":         load_type,
                    "enable_expiration": str(enable_expiration),
                    "layer":             "raw_trusted",
                },
            )

        write_delta(
            spark=spark, df=df, path=paths["raw_trusted"],
            load_type=load_type, primary_key=primary_key,
            enable_expiration=enable_expiration,
        )

        expiration_note = (
            " with expiration"
            if enable_expiration and load_type.lower() in ("scd_type2", "scd2")
            else ""
        )
        log_event(
            spark, "INFO",
            f"Raw trusted write complete: {table_name} -- {row_count} rows as Delta "
            f"({load_type}{expiration_note})",
            run_id, function_name="process_raw_trusted", record_count=row_count,
            status="success", table_name=table_name,
            additional_data={
                "path":              paths["raw_trusted"],
                "load_type":         load_type,
                "format":            "delta",
                "layer":             "raw_trusted",
                "primary_key":       str(primary_key),
                "enable_expiration": str(enable_expiration),
            },
        )
    except Exception as exc:
        log_event(
            spark, "ERROR",
            f"Raw trusted write FAILED: {table_name} -- load_type='{load_type}' -- {exc}", run_id,
            function_name="process_raw_trusted", status="failed", table_name=table_name,
            additional_data={
                "path":      paths["raw_trusted"],
                "load_type": load_type,
                "layer":     "raw_trusted",
                "error":     str(exc),
            },
        )
        raise

    return df
