"""Great Expectations validation framework.

Provides configurable and mandatory data quality validations for Spark ingestion pipelines.
"""

import sys
from unittest.mock import MagicMock

sys.modules["tqdm"] = MagicMock()
sys.modules["tqdm.auto"] = MagicMock()
sys.modules["tqdm.notebook"] = MagicMock()
sys.modules["posthog"] = MagicMock()

from typing import Any, Dict, List, Optional
import great_expectations as gx
from pyspark.sql import DataFrame


class ValidationError(Exception):
    """Raised when data validation fails across one or more checks."""

    def __init__(
        self,
        source_id: str,
        failed_checks: Optional[List[str]] = None,
        results: Optional[List[Dict[str, Any]]] = None,
        cause: Optional[Exception] = None,
    ):
        """Initialize ValidationError.

        Args:
            source_id: Identifier of the data source that failed validation.
            failed_checks: List of failed validation messages.
            results: Detailed results from validation execution.
            cause: Underlying exception that triggered this error (if any).

        Returns:
            None
        """
        self.source_id = source_id
        self.failed_checks = failed_checks or []
        self.results = results or []
        self.cause = cause
        msg = "; ".join(self.failed_checks) if self.failed_checks else ""
        super().__init__(
            f"Validation error for source '{source_id}': {msg}"
            if msg
            else f"Validation error for source '{source_id}'"
        )


def _build_context():
    """Create an ephemeral Great Expectations context.

    Returns:
        gx.DataContext: Ephemeral GE context instance.
    """
    return gx.get_context(mode="ephemeral")


def _get_batch_definition(context, df: DataFrame, source_id: str):
    """Create a batch definition for a Spark DataFrame.

    Args:
        context: Great Expectations context.
        df: Spark DataFrame being validated.
        source_id: Identifier for the data source.

    Returns:
        BatchDefinition: GE batch definition bound to the DataFrame.
    """
    data_source = context.data_sources.add_spark(name=f"spark_{source_id}")
    data_asset = data_source.add_dataframe_asset(name=source_id)
    return data_asset.add_batch_definition_whole_dataframe(f"batch_{source_id}")


def _run_suite(context, batch_definition, suite, df: DataFrame) -> Any:
    """Run a Great Expectations validation suite.

    Args:
        context: GE context.
        batch_definition: Batch definition for the dataset.
        suite: Expectation suite to execute.
        df: Spark DataFrame being validated.

    Returns:
        Any: Validation result object returned by GE.
    """
    context.suites.add(suite)
    validation_def = gx.ValidationDefinition(
        data=batch_definition,
        suite=suite,
        name=f"val_{suite.name}",
    )
    context.validation_definitions.add(validation_def)
    return validation_def.run(batch_parameters={"dataframe": df})


def _get_enabled_validations(config: Dict[str, Any]) -> List[str]:
    """Get list of enabled validation checks from config.

    Args:
        config: Pipeline configuration dictionary.

    Returns:
        List[str]: Enabled validation names or defaults if not provided.
    """
    raw_trusted = config.get("ingestion_steps", {}).get("raw_trusted", {})
    validations = raw_trusted.get("validations") or []
    if not validations:
        return ["row_count_validation", "primary_key_validation", "schema_validation"]
    return validations


def _get_primary_keys(config: Dict[str, Any]) -> List[str]:
    """Extract primary key columns from config.

    Args:
        config: Pipeline configuration dictionary.

    Returns:
        List[str]: Primary key column names.
    """
    raw_trusted = config.get("ingestion_steps", {}).get("raw_trusted", {})
    return raw_trusted.get("primary_key") or []


def _get_expected_columns(config: Dict[str, Any]) -> List[str]:
    """Extract expected schema columns from config.

    Args:
        config: Pipeline configuration dictionary.

    Returns:
        List[str]: Expected column names in schema.
    """
    columns = []
    for item in config.get("schema") or []:
        col = item.get("column_name") if isinstance(item, dict) else getattr(item, "column_name", None)
        if col:
            columns.append(col)
    return columns
def _get_exp_type(res) -> str:
    """Extract expectation type from a GE validation result.

    Args:
        res: Great Expectations result object.

    Returns:
        str: Normalized expectation type string.
    """
    cfg = res.expectation_config
    exp_type = getattr(cfg, "expectation_type", None)
    if exp_type and isinstance(exp_type, str):
        return exp_type
    exp_type = getattr(cfg, "type", None)
    if exp_type and isinstance(exp_type, str):
        return exp_type
    import re
    cls_name = type(cfg).__name__
    return re.sub(r"(?<!^)(?=[A-Z])", "_", cls_name).lower()


def _extract_column(res) -> Optional[str]:
    """Extract a single column name from an expectation result.

    Args:
        res: GE expectation result object.

    Returns:
        Optional[str]: Column name if available, otherwise None.
    """
    cfg = res.expectation_config
    col = getattr(cfg, "column", None)
    if col and isinstance(col, str):
        return col
    kwargs = getattr(cfg, "kwargs", None)
    if isinstance(kwargs, dict):
        col = kwargs.get("column")
        if col and isinstance(col, str):
            return col
    return None


def _extract_column_list(res) -> Optional[List[str]]:
    """Extract a list of columns from a compound expectation.

    Args:
        res: GE expectation result object.

    Returns:
        Optional[List[str]]: List of column names if present.
    """
    cfg = res.expectation_config
    col_list = getattr(cfg, "column_list", None)
    if col_list and isinstance(col_list, list):
        return col_list
    kwargs = getattr(cfg, "kwargs", None)
    if isinstance(kwargs, dict):
        col_list = kwargs.get("column_list")
        if col_list and isinstance(col_list, list):
            return col_list
    return None


def _all_passed(result) -> bool:
    """Check whether all expectations in a validation result passed.

    Args:
        result: GE validation result.

    Returns:
        bool: True if all checks passed or no results exist.
    """
    if not result.results:
        return True
    return all(r.success for r in result.results)


# Mandatory check (always runs, not configurable) 

def _run_duplicate_row_validation(context, batch_definition, df: DataFrame, source_id: str) -> Dict[str, Any]:
    """Validate that no duplicate rows exist in the dataset.

    This is a mandatory post-deduplication sanity check ensuring that the
    dataset remains fully deduplicated after ingestion steps.

    Args:
        context: GE context (unused - bypass GX to avoid serverless caching issues).
        batch_definition: Batch definition (unused).
        df: Spark DataFrame being validated.
        source_id: Identifier for dataset.

    Returns:
        Dict[str, Any]: Validation result with pass/fail status and details.
    """
    # Bypass Great Expectations entirely to avoid DataFrame caching (not supported on serverless)
    # Compute counts directly using SQL
    temp_view = f"_dup_check_{source_id.replace('.', '_')}"
    df.createOrReplaceTempView(temp_view)
    
    spark = df.sparkSession
    result_row = spark.sql(f"""
        SELECT 
            COUNT(*) as total_count,
            COUNT(DISTINCT *) as distinct_count
        FROM {temp_view}
    """).collect()[0]
    
    total_count = result_row["total_count"]
    distinct_count = result_row["distinct_count"]
    spark.catalog.dropTempView(temp_view)

    if total_count == distinct_count:
        return {
            "name": "duplicate_row_validation",
            "passed": True,
            "details": f"Duplicate row check passed -- all {total_count} rows are unique",
            "failed_checks": [],
        }

    duplicate_count = total_count - distinct_count
    msg = (
        f"found {duplicate_count} duplicate row(s) after deduplication "
        f"(total: {total_count}, distinct: {distinct_count}) -- "
        f"verify raw.deduplication config or widen subset_columns"
    )
    return {
        "name": "duplicate_row_validation",
        "passed": False,
        "details": f"Duplicate row check failed -- {msg}",
        "failed_checks": [f"DUPLICATE ROWS FAILED: {msg}"],
    }


# Configurable checks 

def _run_row_count_validation(context, batch_definition, df, source_id):
    """Validate that dataset has at least one row.

    Args:
        context: GE context (unused - bypass GX to avoid serverless caching issues).
        batch_definition: Batch definition (unused).
        df: Spark DataFrame.
        source_id: Dataset identifier.

    Returns:
        Dict[str, Any]: Validation result.
    """
    # Bypass Great Expectations to avoid DataFrame caching (not supported on serverless)
    min_value = 1
    observed = df.count()
    
    if observed >= min_value:
        return {"name": "row_count_validation", "passed": True,
                "details": f"Row count check passed -- {observed} rows found (min: {min_value})",
                "failed_checks": []}

    diff = min_value - observed
    diff_str = f"short by {diff} row{'s' if diff != 1 else ''}"

    msg = (
        f"ROW COUNT FAILED: found {observed} row(s) but expected at least "
        f"{min_value} row(s) -- {diff_str}"
    )
    return {"name": "row_count_validation", "passed": False, "details": msg, "failed_checks": [msg]}


def _run_primary_key_validation(context, batch_definition, df, source_id, primary_keys):
    """Validate primary key constraints (nulls + uniqueness).

    Args:
        context: GE context (unused - bypass GX to avoid serverless caching issues).
        batch_definition: Batch definition (unused).
        df: Spark DataFrame.
        source_id: Dataset identifier.
        primary_keys: List of primary key columns.

    Returns:
        Dict[str, Any]: Validation result.
    """
    if not primary_keys:
        return {"name": "primary_key_validation", "passed": True,
                "details": "Primary key check skipped -- no primary keys configured",
                "failed_checks": []}

    # Bypass Great Expectations to avoid DataFrame caching (not supported on serverless)
    temp_view = f"_pk_check_{source_id.replace('.', '_')}"
    df.createOrReplaceTempView(temp_view)
    spark = df.sparkSession
    
    total_rows = df.count()
    failed_msgs = []

    # Check for NULLs in primary key columns
    for col in primary_keys:
        null_count = spark.sql(f"""
            SELECT COUNT(*) as cnt 
            FROM {temp_view} 
            WHERE `{col}` IS NULL
        """).collect()[0]["cnt"]
        
        if null_count > 0:
            failed_msgs.append(f"column '{col}' has {null_count} NULLs")

    # Check uniqueness
    pk_cols = ", ".join([f"`{col}`" for col in primary_keys])
    result = spark.sql(f"""
        SELECT COUNT(*) as total_count, COUNT(DISTINCT {pk_cols}) as distinct_count
        FROM {temp_view}
    """).collect()[0]
    
    distinct_count = result["distinct_count"]
    spark.catalog.dropTempView(temp_view)
    
    if distinct_count < total_rows:
        dup_count = total_rows - distinct_count
        if len(primary_keys) == 1:
            failed_msgs.append(
                f"column '{primary_keys[0]}' has {dup_count} duplicate rows "
                f"(unique: {distinct_count}, total: {total_rows})"
            )
        else:
            failed_msgs.append(
                f"composite key {primary_keys} has {dup_count} duplicate rows"
            )

    if not failed_msgs:
        return {"name": "primary_key_validation", "passed": True,
                "details": f"Primary key check passed -- columns {primary_keys} are unique and non-null "
                           f"({total_rows} rows, all distinct)",
                "failed_checks": []}
    return {"name": "primary_key_validation", "passed": False,
            "details": f"Primary key check failed -- {'; '.join(failed_msgs)}",
            "failed_checks": [f"PRIMARY KEY FAILED: {m}" for m in failed_msgs]}
def _run_schema_validation(context, batch_definition, df, source_id, expected_columns):
    """Validate that DataFrame schema matches expected configuration.

    Args:
        context: GE context (not directly used but kept for consistency).
        batch_definition: Batch definition for dataset.
        df: Spark DataFrame being validated.
        source_id: Dataset identifier.
        expected_columns: List of expected column names.

    Returns:
        Dict[str, Any]: Validation result with schema mismatch details if any.
    """
    if not expected_columns:
        return {"name": "schema_validation", "passed": True,
                "details": "Schema check skipped -- no expected columns configured",
                "failed_checks": []}

    actual_columns = set(df.columns)
    expected_set = set(expected_columns)
    missing = expected_set - actual_columns
    extra = actual_columns - expected_set

    if not missing and not extra:
        return {"name": "schema_validation", "passed": True,
                "details": f"Schema check passed -- all {len(expected_columns)} expected columns present",
                "failed_checks": []}

    # One explicit, itemized message per offending column -- makes it clear
    # exactly which column(s) caused the mismatch and why, rather than a
    # single combined list.
    failed_msgs = []
    for col in sorted(missing):
        failed_msgs.append(
            f"SCHEMA FAILED: missing column '{col}' -- expected per schema config but not found in DataFrame"
        )
    for col in sorted(extra):
        failed_msgs.append(
            f"SCHEMA FAILED: unexpected column '{col}' -- present in DataFrame but not defined in schema config"
        )

    summary_parts = []
    if missing:
        summary_parts.append(f"missing columns: {sorted(missing)}")
    if extra:
        summary_parts.append(f"unexpected columns: {sorted(extra)}")

    return {"name": "schema_validation", "passed": False,
            "details": f"Schema check failed -- {'; '.join(summary_parts)}",
            "failed_checks": failed_msgs}


def run_detailed(df: DataFrame, config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Run all enabled validations and return detailed results per check.

    Args:
        df: Spark DataFrame to validate.
        config: Pipeline configuration dictionary containing validation rules.

    Returns:
        List[Dict[str, Any]]: List of validation results for each executed check.

    Raises:
        ValidationError: If one or more validations fail.
    """
    # All validations now use Spark SQL directly (no Great Expectations caching)
    source_id = config.get("table_name", "unknown")
    enabled = _get_enabled_validations(config)
    primary_keys = _get_primary_keys(config)
    expected_columns = _get_expected_columns(config)

    results = []

    # Mandatory: always runs
    results.append(_run_duplicate_row_validation(None, None, df, source_id))

    #  Configurable: controlled by validations list in config 
    if "row_count_validation" in enabled:
        results.append(_run_row_count_validation(None, None, df, source_id))

    if "primary_key_validation" in enabled:
        results.append(_run_primary_key_validation(None, None, df, source_id, primary_keys))

    if "schema_validation" in enabled:
        results.append(_run_schema_validation(None, None, df, source_id, expected_columns))

    # Collect all failures and raise if any 
    all_failed_checks = []
    for r in results:
        all_failed_checks.extend(r.get("failed_checks", []))

    if all_failed_checks:
        raise ValidationError(
            source_id=source_id,
            failed_checks=all_failed_checks,
            results=results,
        )

    return results
