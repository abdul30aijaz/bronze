"""Raw layer processor for ingestion pipelines.
Cleans, standardizes, applies schema, deduplicates, and writes the raw DataFrame as Parquet.
"""

import re
from pyspark.sql import DataFrame, functions as F
from connectors.parquet_connector import write_parquet
from transform.header_standardization import standardize_headers
from utils.logging_utils import log_event
from utils.schema_config import enforce_schema, infer_schema


def _clean_dataframe(df: DataFrame, table_name: str) -> tuple:
    """
    Drop fully null rows and handle empty-header columns.
    Covers all ingestion paths — file, excel, notebook.

    - Fully null rows          → dropped silently, counted
    - Empty-header col + all null data  → dropped silently
    - Empty-header col + any non-null   → ValueError, pipeline fails

    Args:
        df: Input DataFrame to clean.
        table_name: Name of the table, used for error messages.

    Returns:
        Tuple of (cleaned DataFrame, original_row_count, empty_rows_dropped).
    """
    # Step 0: Capture original count before any cleaning
    original_count = df.count()

    # Step A: Drop fully null rows
    df = df.dropna(how="all")
    after_dropna_count = df.count()
    empty_rows_dropped = original_count - after_dropna_count

    # Step B: Identify empty-header columns
    # _c0, _c1 ...   → Spark auto-names from CSV with blank headers
    # Unnamed: 0 ... → openpyxl passes through if _strip_empty is removed
    empty_header_cols = [
        c for c in df.columns
        if re.fullmatch(r"_c\d+", c) or re.fullmatch(r"Unnamed:\s*\d+", c)
    ]

    if not empty_header_cols:
        return df, original_count, empty_rows_dropped

    # Single Spark job — count non-nulls across all suspect columns at once
    agg_exprs = [
        F.count(F.when(F.col(c).isNotNull(), 1)).alias(c)
        for c in empty_header_cols
    ]
    non_null_counts = df.agg(*agg_exprs).collect()[0].asDict()

    cols_with_data = [c for c in empty_header_cols if non_null_counts[c] > 0]
    cols_to_drop   = [c for c in empty_header_cols if non_null_counts[c] == 0]

    if cols_with_data:
        raise ValueError(
            f"Empty-header columns with non-null data detected in '{table_name}': "
            f"{cols_with_data}. Fix the source file headers before re-ingesting."
        )

    return df.drop(*cols_to_drop), original_count, empty_rows_dropped
# Deduplication helper (inlined — only used in the raw layer)

def _deduplicate_df(df: DataFrame, layer_config: dict) -> tuple | None:
    """Deduplicate df if enabled in layer_config, else return None.

    Reads the ``deduplication`` key from *layer_config*.
    Returns (deduplicated_df, before_count, after_count, removed_count)
    if enabled, or None if disabled/absent.

    Args:
        df: Input DataFrame to deduplicate.
        layer_config: Layer configuration dict, expected to contain a "deduplication" key.

    Returns:
        A tuple of (deduplicated_df, before_count, after_count, removed_count) if
        deduplication is enabled, otherwise None.
    """
    value = layer_config.get("deduplication")

    if value is None or value is False or (isinstance(value, str) and value.strip().lower() != "true"):
        return None

    if not isinstance(value, (bool, str)):
        raise TypeError(
            f"'deduplication' config must be true or false, got {type(value)}: {value!r}"
        )

    before_count: int  = df.count()
    deduped_df         = df.distinct()
    after_count: int   = deduped_df.count()
    removed_count: int = before_count - after_count

    return deduped_df, before_count, after_count, removed_count


# Raw layer processor

def process_raw(spark, dbutils, df, layer_config, table_config, paths, run_id):
    """Clean, standardize, apply schema, deduplicate, and write the raw DataFrame to Parquet.
    Args:
        spark: Active SparkSession used for caching and processing.
        dbutils: Databricks utilities object (passed through, not directly used here).
        df: Input DataFrame to process.
        layer_config: Configuration dict for this layer (schema_config, deduplication).
        table_config: Configuration dict describing the table (table_name, schema).
        paths: Dict of paths for the pipeline, including the "raw" output path.
        run_id: Identifier for the current pipeline run.

    Returns:
        The processed DataFrame after cleaning, schema application, and deduplication.
    """
    table_name    = table_config.get("table_name")
    schema_config = layer_config.get("schema_config", "infer")
    schema_cfg    = table_config.get("schema") or []

    # Step 0: Drop null rows + validate/drop empty-header columns
    try:
        df, original_count, empty_rows_dropped = _clean_dataframe(df, table_name)
        log_event(
            spark, "INFO",
            f"Empty row/column cleanup complete: {table_name} -- "
            f"{empty_rows_dropped} empty row(s) dropped "
            f"({original_count} → {original_count - empty_rows_dropped})",
            run_id, function_name="process_raw", status="success", table_name=table_name,
            additional_data={
                "layer":               "raw",
                "original_row_count":  str(original_count),
                "empty_rows_dropped":  str(empty_rows_dropped),
            },
        )
    except ValueError as exc:
        log_event(
            spark, "ERROR",
            f"Empty-header column with data detected: {table_name} -- {exc}", run_id,
            function_name="process_raw", status="failed", table_name=table_name,
            additional_data={"layer": "raw", "error": str(exc)},
        )
        raise

    # Step 1: Header standardization
    try:
        df = standardize_headers(df)
        log_event(
            spark, "INFO",
            f"Header standardization complete: {table_name}", run_id,
            function_name="process_raw", status="success", table_name=table_name,
            additional_data={"column_count": str(len(df.columns)), "layer": "raw"},
        )
    except Exception as exc:
        log_event(
            spark, "ERROR",
            f"Header standardization FAILED: {table_name} -- {exc}", run_id,
            function_name="process_raw", status="failed", table_name=table_name,
            additional_data={"layer": "raw", "error": str(exc)},
        )
        raise

    # Step 2: Schema enforcement / inference
    try:
        # Skip caching on serverless (not supported)
        # df.cache()
        # df.count()

        if schema_config == "enforce":
            augmented_schema = list(schema_cfg) + (
                [{"column_name": "source_file_name", "data_type": "string"}]
                if "source_file_name" in df.columns else []
            )
            df = enforce_schema(df, augmented_schema)
        else:
            df = infer_schema(df)

        # Unpersist removed since we're not caching

        row_count = df.count()
        log_event(
            spark, "INFO",
            f"Schema applied ({schema_config}): {table_name} -- {row_count} rows", run_id,
            function_name="process_raw", record_count=row_count, status="success",
            table_name=table_name,
            additional_data={"schema_config": schema_config, "layer": "raw"},
        )
    except Exception as exc:
        # Unpersist removed since we're not caching
        log_event(
            spark, "ERROR",
            f"Schema apply FAILED: {table_name} -- {exc}", run_id,
            function_name="process_raw", status="failed", table_name=table_name,
            additional_data={"schema_config": schema_config, "layer": "raw", "error": str(exc)},
        )
        raise

    # Step 3: Deduplication
    removed_count = 0  # default so it's defined even when dedup is skipped/disabled
    try:
        dedup_result = _deduplicate_df(df, layer_config)
        if dedup_result:
            df, before_count, after_count, removed_count = dedup_result
            row_count = after_count

            if removed_count > 0:
                log_event(
                    spark, "WARNING",
                    f"Deduplication removed {removed_count} duplicate row(s): "
                    f"{table_name} -- before={before_count}, after={after_count}",
                    run_id, function_name="process_raw",
                    record_count=after_count, status="warning", table_name=table_name,
                    additional_data={
                        "layer":         "raw",
                        "before_count":  str(before_count),
                        "after_count":   str(after_count),
                        "removed_count": str(removed_count),
                    },
                )
            else:
                log_event(
                    spark, "INFO",
                    f"Deduplication complete: {table_name} -- no duplicates found ({after_count} rows)",
                    run_id, function_name="process_raw",
                    record_count=after_count, status="success", table_name=table_name,
                    additional_data={
                        "layer":         "raw",
                        "before_count":  str(before_count),
                        "after_count":   str(after_count),
                        "removed_count": "0",
                    },
                )
        else:
            log_event(
                spark, "INFO",
                f"Deduplication skipped: {table_name} -- not enabled in config", run_id,
                function_name="process_raw", status="info", table_name=table_name,
                additional_data={"layer": "raw"},
            )
    except Exception as exc:
        log_event(
            spark, "ERROR",
            f"Deduplication FAILED: {table_name} -- {exc}", run_id,
            function_name="process_raw", status="failed", table_name=table_name,
            additional_data={"layer": "raw", "error": str(exc)},
        )
        raise

    # Step 4: Write parquet
    try:
        write_parquet(df, paths["raw"])
        log_event(
            spark, "INFO",
            f"Raw write complete: {table_name} -- {row_count} rows as Parquet", run_id,
            function_name="process_raw", record_count=row_count, status="success",
            table_name=table_name,
            additional_data={"path": paths["raw"], "format": "parquet", "layer": "raw"},
        )
    except Exception as exc:
        log_event(
            spark, "ERROR",
            f"Raw write FAILED: {table_name} -- {exc}", run_id,
            function_name="process_raw", status="failed", table_name=table_name,
            additional_data={"path": paths["raw"], "layer": "raw", "error": str(exc)},
        )
        raise

    # Step 5: Row-completeness DQ score — survives even if raw_trusted later fails
    final_row_count   = row_count
    completeness_frac = round(final_row_count / original_count, 4) if original_count > 0 else None

    log_event(
        spark,
        "INFO" if completeness_frac == 1.0 else "WARNING",
        f"Row completeness: {table_name} -- {final_row_count}/{original_count} rows retained "
        f"({round((completeness_frac or 0) * 100, 1)}%) after empty-row/duplicate cleanup "
        f"(empty_dropped={empty_rows_dropped}, duplicates_dropped={removed_count})",
        run_id, function_name="process_raw",
        status="success" if completeness_frac == 1.0 else "warning",
        table_name=table_name,
        additional_data={
            "layer":                  "dq_completeness",
            "passed_count":           str(final_row_count),
            "total_count":            str(original_count),
            "empty_rows_dropped":     str(empty_rows_dropped),
            "duplicate_rows_dropped": str(removed_count),
        },
    )

    return df