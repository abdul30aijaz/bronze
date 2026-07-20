"""Delta-based logging framework."""

import logging
import os
import sys
from datetime import datetime

sys.path.insert(0, "/Workspace/Shared/bronze")
from env import CATALOG

os.environ["TQDM_DISABLE"] = "1"
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"
os.environ["GX_ANALYTICS_ENABLED"] = "false"

# Suppress noisy third-party loggers
for logger_name in [
    "great_expectations", "great_expectations.data_context",
    "great_expectations.validator", "great_expectations.core",
    "great_expectations.execution_engine", "py4j", "py4j.java_gateway",
    "pyspark", "pyspark.sql", "pyspark.sql.dataframe",
    "posthog", "databricks", "urllib3", "requests"
]:
    logging.getLogger(logger_name).setLevel(logging.ERROR)

logging.getLogger().setLevel(logging.WARNING)

_console_logger = logging.getLogger(__name__)
_console_logger.setLevel(logging.DEBUG)
_console_logger.propagate = False

if not _console_logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(
        logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
    )
    _console_logger.addHandler(_handler)

# Module-level cache so the SDK call only happens once per process
_cached_job_name: str | None = None


def _get_job_name(spark) -> str:
    """Resolve the actual Databricks job name for the currently running job.

    Resolution order:
      1. Databricks SDK  – looks up the active run's job name via the Jobs API.
      2. SparkContext app name – set to f"MDIF-Pipeline-{table_name}" in run(),
         which at least gives the table name when the SDK call fails.
      3. Hard fallback    – "MDIF_unknown_job" so the column is never NULL.

    The result is cached at module level so the SDK round-trip only happens
    once per Spark process (i.e. once per notebook / task).
    """
    global _cached_job_name
    if _cached_job_name is not None:
        return _cached_job_name

    # --- 1. Databricks SDK ---
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        active_runs = list(w.jobs.list_runs(active_only=True))
        if active_runs:
            active_runs.sort(key=lambda r: r.start_time or 0, reverse=True)
            latest_run = active_runs[0]
            # list_runs returns Run objects; the job settings are on run.run_name
            # or we can look up the job by job_id for its canonical name.
            run_name = getattr(latest_run, "run_name", None)
            job_id   = getattr(latest_run, "job_id", None)
            if job_id:
                try:
                    job_details = w.jobs.get(job_id=job_id)
                    resolved = job_details.settings.name  # canonical job name
                except Exception:
                    resolved = run_name  # fall back to run name
            else:
                resolved = run_name
            if resolved:
                _cached_job_name = resolved
                return _cached_job_name
    except Exception:
        pass

    # --- 2. SparkContext app name ---
    try:
        app_name = spark.sparkContext.appName
        if app_name and app_name not in ("SparkContext", "PySparkShell", ""):
            _cached_job_name = app_name
            return _cached_job_name
    except Exception:
        pass

    # --- 3. Fallback ---
    _cached_job_name = "MDIF_unknown_job"
    return _cached_job_name


def get_run_id(spark):
    """Fetch current job run ID."""
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        active_runs = list(w.jobs.list_runs(active_only=True))
        if active_runs:
            active_runs.sort(key=lambda r: r.start_time or 0, reverse=True)
            return str(active_runs[0].run_id)
    except Exception:
        pass
    return f"notebook_{datetime.now().strftime('%Y%m%d_%H%M%S')}"


def get_log_table() -> str:
    """Return the single Unity Catalog logging table name."""
    return f"{CATALOG}.logging.pipeline_logs"


def ensure_log_table(spark):
    """Create Unity Catalog Delta log table if it doesn't exist."""
    log_table = get_log_table()

    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.logging")

        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {log_table} (
                timestamp       TIMESTAMP,
                log_level       STRING,
                message         STRING,
                job_name        STRING,
                table_name      STRING,
                function_name   STRING,
                status          STRING,
                additional_data MAP<STRING, STRING>,
                run_id          STRING
            ) USING DELTA
        """)

        _console_logger.info(f"[Logging] Ensured log table: {log_table}")
    except Exception as e:
        _console_logger.warning(f"[Logging] Could not ensure log table: {e}")

    return log_table


def log_event(spark, level, message, run_id, function_name=None,
              record_count=None, status=None, additional_data=None,
              table_name=None, job_name=None):
    """Write structured log entry to console and Unity Catalog Delta table.

    Parameters
    ----------
    job_name : str, optional
        Override the resolved job name. When omitted (the common case),
        ``_get_job_name(spark)`` is called so every row receives the actual
        Databricks job name rather than a hardcoded placeholder.
    """
    level_upper = level.upper()
    status_tag  = f"[{status.upper()}]" if status else ""
    func_tag    = f"[{function_name}]"  if function_name else ""
    record_tag  = f"  rows={record_count}" if record_count is not None else ""

    log_message = f"{func_tag} {status_tag}  {message}{record_tag}"

    # Console logging
    log_func = {
        "DEBUG":    _console_logger.debug,
        "INFO":     _console_logger.info,
        "WARNING":  _console_logger.warning,
        "WARN":     _console_logger.warning,
        "ERROR":    _console_logger.error,
        "CRITICAL": _console_logger.critical,
    }.get(level_upper, _console_logger.info)

    log_func(log_message)

    # Delta table logging
    try:
        log_table = get_log_table()
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Resolve job name dynamically – never hardcode
        resolved_job_name = job_name or _get_job_name(spark)

        extra = {}
        if record_count is not None:
            extra["record_count"] = str(record_count)
        if additional_data and isinstance(additional_data, dict):
            extra.update({k: str(v) for k, v in additional_data.items()})

        final_additional_data = extra if extra else None
        safe_message          = message.replace("'", "''") if message else ""
        safe_job_name         = resolved_job_name.replace("'", "''")

        if final_additional_data:
            pairs = [f"'{k}', '{v.replace(chr(39), chr(39)*2)}'"
                     for k, v in final_additional_data.items()]
            additional_data_sql = f"MAP({', '.join(pairs)})"
        else:
            additional_data_sql = "NULL"

        func_sql   = f"'{function_name}'" if function_name else "NULL"
        status_sql = f"'{status}'"        if status        else "NULL"
        table_sql  = f"'{table_name}'"    if table_name    else "NULL"

        spark.sql(f"""
            INSERT INTO {log_table}
            (timestamp, log_level, message, job_name, table_name,
             function_name, status, additional_data, run_id)
            VALUES (
                TIMESTAMP '{ts}',
                '{level_upper}',
                '{safe_message}',
                '{safe_job_name}',
                {table_sql},
                {func_sql},
                {status_sql},
                {additional_data_sql},
                '{run_id}'
            )
        """)
    except Exception as e:
        _console_logger.warning(f"[Logging] Failed to write to Delta table: {e}")


def log_source_files(spark, file_names: list, row_count: int, table_name: str, run_id: str):
    """Log the list of source files read during landing ingestion.

    Writes one INFO log entry listing every file that was picked up from the
    source directory, so the audit trail shows exactly which files contributed
    to each ingestion run.

    Parameters
    ----------
    spark      : active SparkSession
    file_names : list of file name strings (just the names, not full paths)
    row_count  : total rows read across all files combined
    table_name : control-table table name (for the log row)
    run_id     : current pipeline run ID
    """
    file_list_str = ", ".join(file_names) if file_names else "none"
    file_count    = len(file_names)

    log_event(
        spark, "INFO",
        f"Source files detected: {table_name} -- {file_count} file(s): [{file_list_str}]",
        run_id,
        function_name="log_source_files",
        record_count=row_count,
        status="success",
        table_name=table_name,
        additional_data={
            "file_count": str(file_count),
            "file_names": file_list_str,
            "layer":      "landing",
        },
    )