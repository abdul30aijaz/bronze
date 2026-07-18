"""Landing layer ingestion framework for file and notebook-based sources.

Handles dynamic routing of ingestion logic based on configuration.
"""

import json
from functools import reduce

from connectors.connector_registry import get_connector
from connectors.csv_connector import write_csv
from utils.masking_utils import encrypt_pii_columns
from utils.logging_utils import log_event, log_source_files
from pyspark.sql import functions as F  


def _process_file_landing(spark, dbutils, df, layer_config, table_config, paths, run_id):
    """Ingest files from a landing path, apply optional transformations, and write to CSV.
    Supports multiple file formats via connector registry and optional PII encryption.

    Args:
        spark: Spark session.
        dbutils: Databricks utilities instance.
        df: Input DataFrame (unused but kept for interface consistency).
        layer_config: Configuration for landing layer ingestion.
        table_config: Table-level configuration (contains table name).
        paths: Dictionary containing output paths.
        run_id: Execution run identifier.

    Returns:
        pyspark.sql.DataFrame: Combined DataFrame after ingestion and transformations.

    Raises:
        Exception: If file reading or writing fails.
    """

    table_name    = table_config.get("table_name")
    source_path   = layer_config.get("source_path", "")
    file_format   = layer_config.get("file_format", "")

    # Reserved config keys that should not be passed to connectors
    reserved_keys = {
        "source_path", "file_format", "format_options_key", "format_options",
        "excel_options", "notebook_path", "parameters", "pii_columns",
    }

    pii_columns = layer_config.get("pii_columns")

    # Build connector options dynamically from config
    connector_opts = {}
    for k, v in layer_config.items():
        if k in reserved_keys or v is None:
            continue
        if isinstance(v, str):
            try:
                parsed = json.loads(v)
                connector_opts[k] = parsed if isinstance(parsed, dict) else v
            except (json.JSONDecodeError, TypeError):
                connector_opts[k] = v
        else:
            connector_opts[k] = v

    # Parse and normalize format-specific options
    format_options_raw = layer_config.get("format_options")
    if format_options_raw:
        if isinstance(format_options_raw, str):
            try:
                format_options = json.loads(format_options_raw)
            except (json.JSONDecodeError, TypeError):
                format_options = {}
        elif isinstance(format_options_raw, dict):
            format_options = format_options_raw
        else:
            format_options = {}

        if format_options:
            fmt = file_format.lower().strip()

            # Format-specific handling
            if fmt in ("xlsx", "xls", "excel"):
                connector_opts["excel_options"] = format_options
            elif fmt == "csv":
                if "delimiter" in format_options:
                    format_options["sep"] = format_options.pop("delimiter")
                connector_opts.update(format_options)
            else:
                connector_opts.update(format_options)

    # Resolve file paths for ingestion
    full_path  = f"{source_path.rstrip('/')}/{table_name.upper()}"
    file_list  = [f for f in dbutils.fs.ls(full_path) if not f.name.endswith("/")]
    file_names = [f.name for f in file_list]
    file_paths = [f.path for f in file_list]

    try:
        connector = get_connector(file_format)

        # Read all files and attach source filename column
        dfs = [
            connector(spark, path=fp, **connector_opts)
            .withColumn("source_file_name", F.lit(fn))
            for fp, fn in zip(file_paths, file_names)
        ]

        df        = dfs[0] if len(dfs) == 1 else reduce(lambda a, b: a.union(b), dfs)
        row_count = df.count()

        log_source_files(spark, file_names, row_count, table_name, run_id)

        log_event(
            spark, "INFO",
            f"File landing read complete: {table_name} -- {row_count} rows",
            run_id,
            function_name="_process_file_landing",
            record_count=row_count,
            status="success",
            table_name=table_name,
            additional_data={
                "source_path": full_path,
                "file_format": file_format,
                "layer": "landing",
            },
        )

    except Exception as exc:
        log_event(
            spark, "ERROR",
            f"File landing read FAILED: {table_name} -- {exc}",
            run_id,
            function_name="_process_file_landing",
            status="failed",
            table_name=table_name,
            additional_data={
                "source_path": full_path,
                "file_format": file_format,
                "error": str(exc),
            },
        )
        raise

    # Optional PII encryption step
    if pii_columns:
        df = encrypt_pii_columns(df, pii_columns)

        log_event(
            spark, "INFO",
            f"PII columns encrypted: {table_name} -- {pii_columns}",
            run_id,
            function_name="_process_file_landing",
            record_count=row_count,
            status="success",
            table_name=table_name,
            additional_data={
                "pii_columns": str(pii_columns),
                "layer": "landing",
            },
        )

    # Write final landing dataset
    try:
        write_csv(df, paths["landing"])

        encryption_note = f" (with {len(pii_columns)} PII columns encrypted)" if pii_columns else ""

        log_event(
            spark, "INFO",
            f"File landing write complete: {table_name}{encryption_note}",
            run_id,
            function_name="_process_file_landing",
            record_count=row_count,
            status="success",
            table_name=table_name,
            additional_data={
                "path": paths["landing"],
                "format": "csv",
                "layer": "landing",
                "pii_encrypted": bool(pii_columns),
                "pii_columns": str(pii_columns or []),
            },
        )

    except Exception as exc:
        log_event(
            spark, "ERROR",
            f"File landing write FAILED: {table_name} -- {exc}",
            run_id,
            function_name="_process_file_landing",
            status="failed",
            table_name=table_name,
            additional_data={
                "path": paths["landing"],
                "layer": "landing",
                "error": str(exc),
            },
        )
        raise

    return df


def _run_notebook(spark, dbutils, df, layer_config, table_config, paths, run_id):
    """Execute a Databricks notebook as part of landing ingestion.
    The notebook is executed with system + metadata parameters, and output
    is reloaded from landing storage after execution.

    Args:
        spark: Spark session.
        dbutils: Databricks utilities instance.
        df: Input DataFrame (not directly used).
        layer_config: Landing layer configuration.
        table_config: Table configuration.
        paths: Output path dictionary.
        run_id: Execution run identifier.

    Returns:
        pyspark.sql.DataFrame: DataFrame loaded from notebook output.

    Raises:
        Exception: If notebook execution or downstream read fails.
    """

    table_name      = table_config.get("table_name")
    notebook_path   = layer_config.get("notebook_path")
    metadata_params = layer_config.get("parameters") or {}

    # Normalize metadata parameters
    if isinstance(metadata_params, str):
        try:
            metadata_params = json.loads(metadata_params)
        except (json.JSONDecodeError, TypeError):
            metadata_params = {}
    elif hasattr(metadata_params, "asDict"):
        metadata_params = metadata_params.asDict()
    elif not isinstance(metadata_params, dict):
        metadata_params = {}

    metadata_params = {k: str(v) for k, v in metadata_params.items()}

    # System-generated parameters passed to notebook
    system_params = {
        "table_name":  table_name,
        "run_id":      run_id,
        "output_path": paths["landing"],
    }

    arguments = {**system_params, **metadata_params}

    try:
        log_event(
            spark, "INFO",
            f"Notebook run started: {table_name} -- {notebook_path}",
            run_id,
            function_name="_run_notebook",
            status="running",
            table_name=table_name,
            additional_data={
                "notebook_path": notebook_path,
                "metadata_params": str(metadata_params),
                "layer": "landing",
            },
        )

        dbutils.notebook.run(notebook_path, timeout_seconds=3600, arguments=arguments)

        # Reload output written by notebook
        df        = spark.read.option("header", "true").csv(paths["landing"])
        df        = df.withColumn("source_file_name", F.lit("notebook"))
        row_count = df.count()

        log_event(
            spark, "INFO",
            f"Notebook run complete: {table_name} -- {row_count} rows",
            run_id,
            function_name="_run_notebook",
            record_count=row_count,
            status="success",
            table_name=table_name,
            additional_data={
                "notebook_path": notebook_path,
                "row_count": str(row_count),
                "layer": "landing",
            },
        )

    except Exception as exc:
        log_event(
            spark, "ERROR",
            f"Notebook run FAILED: {table_name} -- {notebook_path} -- {exc}",
            run_id,
            function_name="_run_notebook",
            status="failed",
            table_name=table_name,
            additional_data={
                "notebook_path": notebook_path,
                "layer": "landing",
                "error": str(exc),
            },
        )
        raise

    return df


# Landing field → handler registry 

_LANDING_FIELD_REGISTRY = {
    "source_path":   _process_file_landing,
    "notebook_path": _run_notebook,
}


def process_landing(spark, dbutils, df, layer_config, table_config, paths, run_id):
    """Route landing ingestion to the correct handler based on configuration.

    Args:
        spark: Spark session.
        dbutils: Databricks utilities instance.
        df: Input DataFrame.
        layer_config: Landing layer configuration.
        table_config: Table configuration.
        paths: Output path dictionary.
        run_id: Execution run identifier.

    Returns:
        pyspark.sql.DataFrame: Processed DataFrame from selected handler.

    Raises:
        ValueError: If no valid ingestion type is found in configuration.
    """
    table_name = table_config.get("table_name")

    for field, handler in _LANDING_FIELD_REGISTRY.items():
        if layer_config.get(field):
            log_event(
                spark, "INFO",
                f"Landing ingestion type detected: {table_name} -- '{field}' present → {handler.__name__}",
                run_id,
                function_name="process_landing",
                status="running",
                table_name=table_name,
                additional_data={"detected_field": field, "handler": handler.__name__},
            )
            return handler(spark, dbutils, df, layer_config, table_config, paths, run_id)

    raise ValueError(
        f"Cannot determine ingestion type for {table_name} -- "
        f"no known fields found in landing config. "
        f"Present keys: {list(layer_config.keys())} | "
        f"Known fields: {list(_LANDING_FIELD_REGISTRY.keys())}"
    )