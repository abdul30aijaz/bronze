"""
dq_logging_utils.py — Delta-backed error logging for structural data quality validations.

This module does NOT define or run any expectations itself. It only captures the
results produced by validations.run_detailed() (the 4 structural checks:
duplicate_row_validation, row_count_validation, primary_key_validation,
schema_validation) and, when any of them fail, writes one error record per
failed check into a Unity Catalog Delta error table.
"""
import json
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, StringType, StructField, StructType, TimestampType

from connectors.delta_connector import _ensure_schema_exists

UTC = timezone.utc

_ERROR_SCHEMA = StructType([
    StructField("error_timestamp",  TimestampType(), True),
    StructField("error_date",       DateType(),      True),
    StructField("job_id",           StringType(),    True),
    StructField("run_id",           StringType(),    True),
    StructField("job_name",         StringType(),    True),
    StructField("source_table",     StringType(),    True),
    StructField("expectation_type", StringType(),    True),
    StructField("column_name",      StringType(),    True),
    StructField("failed_value",     StringType(),    True),
    StructField("full_record",      StringType(),    True),
    StructField("severity",         StringType(),    True),
])


def _resolve_job_run_ids(spark: SparkSession):
    """Resolve Databricks job_id and run_id.

    Args:
        spark: Active SparkSession.

    Returns:
        Tuple (job_id, run_id). Falls back to notebook context if API unavailable.
    """
    try:
        from databricks.sdk import WorkspaceClient
        run = next(iter(WorkspaceClient().jobs.list_runs(active_only=True)), None)
        if run:
            job_id, run_id = str(run.job_id), str(run.run_id)
            print(f"[DQ] job_id={job_id}, run_id={run_id} (WorkspaceClient)")
            return job_id, run_id
    except Exception:
        pass

    job_id = "unknown_job"
    try:
        from pyspark.dbutils import DBUtils
        job_id = str(DBUtils(spark).notebook.entry_point.getDbutils().notebook().getContext().jobId().get())
        print(f"[DQ] job_id={job_id} (notebook context fallback)")
    except Exception:
        pass

    ts = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    print(f"[DQ WARNING] WorkspaceClient unavailable — run_id fallback: notebook_{ts}")
    return job_id, f"notebook_{ts}"


def _extract_column_from_message(msg: str) -> str:
    """Best-effort extraction of a column name from a failed-check message.

    Structural validation messages typically embed the offending column as
    "column 'name'" (see validations.py). Falls back to "N/A" when no column
    can be identified (e.g. row_count_validation, which is table-level).

    Args:
        msg: A single failed-check message string.

    Returns:
        The extracted column name, or "N/A" if none is present.
    """
    match = re.search(r"column '([^']+)'", msg)
    if match:
        return match.group(1)
    return "N/A"


class DQValidator:
    """Delta-backed error logger for structural data quality validation failures.

    This class does not define or execute any expectations. It consumes the
    result list produced by validations.run_detailed() / ValidationError.results
    (duplicate_row_validation, row_count_validation, primary_key_validation,
    schema_validation) and persists failed checks to a Delta error table.

    Args:
        spark: SparkSession
        job_name: Name of the pipeline/job
        catalog: Unity Catalog name
        schema: Schema name
        table_name: Target table name

    Returns:
        Instance of DQValidator
    """

    def __init__(self, spark: SparkSession, job_name: str, catalog: str, schema: str, table_name: str):
        self.spark        = spark
        self.job_name     = job_name
        self.catalog      = catalog
        self.schema       = schema
        self.table_name   = table_name
        self.error_table  = f"{catalog}.{schema}.{table_name}_errors"
        self.source_table = f"{schema}.{table_name}"
        self.job_id, self.run_id = _resolve_job_run_ids(spark)
        self.validation_run_id   = f"{self.run_id}_{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}"

    def _ensure_error_schema(self):
        """Ensure the Unity Catalog schema exists before writing the error table."""
        try:
            _ensure_schema_exists(self.spark, self.error_table)
            print(f"[DQ] Schema ready for: {self.error_table}")
        except Exception as e:
            print(f"[DQ WARNING] Could not ensure schema: {e}")

    def log_failed_records(self, failed_records: List[Dict[str, Any]], severity: str = "CRITICAL") -> None:
        """Write failed records to the Delta error table, creating it first if needed.

        Args:
            failed_records: List of dicts with keys expectation_type, column_name,
                             failed_value, full_record.
            severity: Severity label attached to every record in this batch.

        Returns:
            None
        """
        if not failed_records:
            print("[DQ] No failed records to log")
            return
        self._ensure_error_schema()
        now = datetime.now(UTC)
        rows = [
            (now, now.date(), self.job_id, self.run_id, self.job_name, self.source_table,
             r["expectation_type"], r["column_name"], r["failed_value"], r["full_record"], severity)
            for r in failed_records
        ]
        (
            self.spark.createDataFrame(rows, schema=_ERROR_SCHEMA)
            .write.format("delta").mode("append")
            .partitionBy("error_date")
            .saveAsTable(self.error_table)
        )
        print(f"[DQ] Logged {len(failed_records)} failed records → {self.error_table}")

    def _get_duplicate_rows(self, df: DataFrame, max_rows: int) -> List[Dict[str, Any]]:
        """Independently recompute the distinct duplicate rows in df.

        Not sourced from GX — this queries the same DataFrame directly so the
        actual duplicate row content can be logged, without validations.py
        needing to expose anything.

        Args:
            df: The DataFrame that duplicate_row_validation was run against.
            max_rows: Cap on number of distinct duplicate rows collected, to
                      protect the driver from an unbounded .collect().

        Returns:
            List of row dicts (each a distinct row value that occurs more than once).
        """
        dup_df = (
            df.groupBy(df.columns)
              .count()
              .filter(F.col("count") > 1)
              .drop("count")
              .limit(max_rows)
        )
        return [row.asDict() for row in dup_df.collect()]

    def _get_primary_key_failure_rows(self, df: DataFrame, primary_key: List[str],
                                       max_rows: int) -> List[Dict[str, Any]]:
        """Independently recompute the rows that violate the primary key constraint.

        Covers both null primary-key values and duplicate primary-key values.
        Queried directly against df — not sourced from GX/validations.py.

        Args:
            df: The DataFrame that primary_key_validation was run against.
            primary_key: List of primary key column names.
            max_rows: Cap on number of offending rows collected.

        Returns:
            List of row dicts for rows with a null or duplicate primary key.
        """
        null_cond = None
        for c in primary_key:
            cond = F.col(c).isNull()
            null_cond = cond if null_cond is None else (null_cond | cond)
        null_rows = df.filter(null_cond) if null_cond is not None else df.limit(0)

        dup_keys = (
            df.groupBy(*primary_key).count()
              .filter(F.col("count") > 1)
              .select(*primary_key)
        )
        dup_rows = df.join(dup_keys, on=primary_key, how="inner")

        combined = null_rows.unionByName(dup_rows, allowMissingColumns=True).distinct().limit(max_rows)
        return [row.asDict() for row in combined.collect()]

    def log_validation_failures(self, results: List[Dict[str, Any]],
                                 df: Optional[DataFrame] = None,
                                 primary_key: Optional[List[str]] = None,
                                 severity: str = "CRITICAL",
                                 max_rows_per_check: int = 5000) -> int:
        """Log failed structural validation checks to the Delta error table.

        Consumes the result list from validations.run_detailed() (or
        ValidationError.results) — one entry per check (duplicate_row_validation,
        row_count_validation, primary_key_validation, schema_validation).

        For the two checks that are inherently row-level — duplicate_row_validation
        and primary_key_validation — this independently re-queries `df` (the same
        DataFrame validations.py was given) to find and log the actual offending
        rows, one error record per row, with the full row content as JSON in
        full_record. This is done entirely in this module; validations.py is
        never modified or queried for row-level detail.

        row_count_validation and schema_validation have no row-level concept
        (they're table/schema-level), so those are logged as a single message
        record, same as before.

        If `df` (and, for primary_key_validation, `primary_key`) are not
        supplied, every failing check falls back to message-only logging.

        Args:
            results: List of validation result dicts, each with keys
                     'name', 'passed', 'details', 'failed_checks'.
            df: The DataFrame that was validated — required for row-level capture.
            primary_key: Primary key columns — required for row-level capture
                         of primary_key_validation.
            severity: Severity label to attach to each logged record.
            max_rows_per_check: Cap on number of offending rows collected per
                                 failed check, to protect the driver.

        Returns:
            Number of error records logged.
        """
        failed_records = []
        for r in results:
            if r.get("passed", True):
                continue
            check_name    = r.get("name", "unknown_validation")
            failed_checks = r.get("failed_checks") or [r.get("details", "validation failed")]

            row_level_done = False
            try:
                if check_name == "duplicate_row_validation" and df is not None:
                    dup_rows = self._get_duplicate_rows(df, max_rows_per_check)
                    for row in dup_rows:
                        failed_records.append({
                            "expectation_type": check_name,
                            "column_name":      "ALL",
                            "failed_value":     "N/A",
                            "full_record":      json.dumps(row, default=str),
                        })
                    row_level_done = True

                elif check_name == "primary_key_validation" and df is not None and primary_key:
                    pk_fail_rows = self._get_primary_key_failure_rows(df, primary_key, max_rows_per_check)
                    for row in pk_fail_rows:
                        failed_records.append({
                            "expectation_type": check_name,
                            "column_name":      ", ".join(primary_key),
                            "failed_value":     json.dumps({k: row.get(k) for k in primary_key}, default=str),
                            "full_record":      json.dumps(row, default=str),
                        })
                    row_level_done = True
            except Exception as exc:
                print(f"[DQ WARNING] Row-level capture failed for '{check_name}': {exc} -- falling back to message-only")
                row_level_done = False

            if not row_level_done:
                for msg in failed_checks:
                    failed_records.append({
                        "expectation_type": check_name,
                        "column_name":      _extract_column_from_message(msg),
                        "failed_value":     "N/A",
                        "full_record":      msg,
                    })

        if failed_records:
            self.log_failed_records(failed_records, severity)
        return len(failed_records)
