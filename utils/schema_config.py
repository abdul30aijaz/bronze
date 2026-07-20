"""Schema enforcement and schema inference utilities."""

import uuid

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)

_TYPE_MAP = {
    "int": IntegerType(),
    "integer": IntegerType(),
    "long": LongType(),
    "bigint": LongType(),
    "string": StringType(),
    "float": FloatType(),
    "double": DoubleType(),
    "decimal": DecimalType(38, 18),
    "date": DateType(),
    "timestamp": TimestampType(),
}

_NUMERIC_TYPES = {
    "float",
    "double",
    "int",
    "integer",
    "long",
    "bigint",
    "decimal",
}

_DATE_FMTS = [
    "yyyy-MM-dd",
    "MM/dd/yyyy",
    "dd/MM/yyyy",
    "MM-dd-yyyy",
    "dd-MM-yyyy",
    "yyyy/MM/dd",
    "dd-MMM-yy",
    "dd-MMM-yyyy",
]

_DATE_WITH_TIME_FMTS = [
    "yyyy-MM-dd HH:mm:ss",
    "MM/dd/yyyy HH:mm:ss",
    "dd/MM/yyyy HH:mm:ss",
    "MM-dd-yyyy HH:mm:ss",
    "dd-MM-yyyy HH:mm:ss",
    "yyyy/MM/dd HH:mm:ss",
    "dd-MMM-yy HH:mm:ss",
    "dd-MMM-yyyy HH:mm:ss",
]

_TS_FMTS = [
    "yyyy-MM-dd HH:mm:ss",
    "MM/dd/yyyy HH:mm:ss",
    "dd/MM/yyyy HH:mm:ss",
    "MM-dd-yyyy HH:mm:ss",
    "yyyy/MM/dd HH:mm:ss",
    "dd-MM-yyyy HH:mm:ss",
]

_NULL_LIKE_SQL = "('null','none','na','n/a','nan','-','','nat')"


def _escape_sql_identifier(identifier):
    """Escape SQL identifiers.

    Args:
        identifier: Column or object name.

    Returns:
        Escaped identifier.
    """
    return identifier.replace("`", "``")


def _escape_sql_string(s):
    """Escape SQL string literals.

    Args:
        s: String value.

    Returns:
        Escaped string.
    """
    return s.replace("'", "''")


def _null_check_sql(escaped_col):
    """Build SQL condition for non-null values.

    Args:
        escaped_col: Escaped column name.

    Returns:
        SQL expression.
    """
    return (
        f"`{escaped_col}` IS NOT NULL "
        f"AND lower(trim(`{escaped_col}`)) NOT IN {_NULL_LIKE_SQL}"
    )


def _drop_temp_view(spark, temp_view):
    """Drop a temporary view if it exists.

    Args:
        spark: Active SparkSession.
        temp_view: Temporary view name.

    Returns:
        None.
    """
    try:
        spark.catalog.dropTempView(temp_view)
    except Exception:
        pass


def _build_nested_case(try_exprs):
    """Build a CASE expression from candidate expressions.

    Args:
        try_exprs: List of SQL expressions.

    Returns:
        SQL CASE expression.
    """
    if not try_exprs:
        return "NULL"

    if len(try_exprs) == 1:
        return try_exprs[0]

    when_clauses = " ".join(
        f"WHEN ({e}) IS NOT NULL THEN ({e})"
        for e in try_exprs[:-1]
    )

    return f"CASE {when_clauses} ELSE ({try_exprs[-1]}) END"


def _build_date_case(escaped_col):
    """Build a SQL expression for parsing dates.

    Args:
        escaped_col: Escaped column name.

    Returns:
        SQL expression.
    """
    return _build_nested_case(
        [f"try_to_date(`{escaped_col}`, '{_escape_sql_string(f)}')" for f in _DATE_FMTS]
        + [f"try_cast(try_cast(`{escaped_col}` as timestamp) as date)"]
        + [
            f"try_cast(try_to_timestamp(`{escaped_col}`, '{_escape_sql_string(f)}') as date)"
            for f in _DATE_WITH_TIME_FMTS
        ]
    )


def _build_timestamp_case(escaped_col):
    """Build a SQL expression for parsing timestamps.

    Args:
        escaped_col: Escaped column name.

    Returns:
        SQL expression.
    """
    return _build_nested_case(
        [f"try_cast(`{escaped_col}` as timestamp)"]
        + [
            f"try_to_timestamp(`{escaped_col}`, '{_escape_sql_string(f)}')"
            for f in _TS_FMTS
        ]
    )


def _normalize_schema_config(schema_config):
    """Normalize schema configuration entries.

    Args:
        schema_config: Schema configuration as dicts, lists, or objects.

    Returns:
        List of normalized dictionaries.
    """
    result = []

    for col_def in schema_config:
        if isinstance(col_def, dict):
            result.append(col_def)
        elif isinstance(col_def, list) and len(col_def) >= 2:
            result.append(
                {
                    "column_name": col_def[0],
                    "data_type": col_def[1],
                }
            )
        elif hasattr(col_def, "column_name"):
            result.append(
                {
                    "column_name": col_def.column_name,
                    "data_type": col_def.data_type,
                }
            )

    return result


def enforce_schema(df, schema_config):
    """Cast DataFrame columns to configured data types.

    Args:
        df: Input Spark DataFrame.
        schema_config: Column definitions with target data types.

    Returns:
        DataFrame with enforced schema.

    Raises:
        ValueError: If one or more values cannot be cast to the target type.
    """
    if not schema_config:
        return df

    spark = df.sparkSession
    df_cols = df.columns
    df_cols_set = set(df_cols)

    normalized = _normalize_schema_config(schema_config)
    schema_col_names = {c["column_name"] for c in normalized}

    select_exprs = []
    orig_exprs = []
    validate_cols = {}

    for col_def in normalized:
        col_name = col_def["column_name"]
        dtype = col_def["data_type"].lower().strip()

        if col_name not in df_cols_set:
            continue

        if dtype not in _TYPE_MAP:
            select_exprs.append(F.col(col_name))
            continue

        escaped = _escape_sql_identifier(col_name)

        if dtype == "date":
            select_exprs.append(
                F.expr(_build_date_case(escaped)).alias(col_name)
            )

        elif dtype == "timestamp":
            select_exprs.append(
                F.expr(_build_timestamp_case(escaped)).alias(col_name)
            )

        elif dtype in _NUMERIC_TYPES:
            select_exprs.append(
                F.expr(
                    f"try_cast(regexp_replace(trim(`{escaped}`), '\\s*%\\s*$', '') as {dtype})"
                ).alias(col_name)
            )

        else:
            select_exprs.append(
                F.expr(f"try_cast(`{escaped}` as {dtype})").alias(col_name)
            )

        orig_exprs.append(F.col(col_name).alias(f"__orig_{col_name}"))
        validate_cols[col_name] = dtype

    for col_name in df_cols:
        if col_name not in schema_col_names:
            select_exprs.append(F.col(col_name))

    df_cast = df.select(select_exprs + orig_exprs)
    # Validate cast results
    if validate_cols:
        col_list = list(validate_cols.items())
        val_exprs = []

        for i, (col_name, _) in enumerate(col_list):
            escaped = _escape_sql_identifier(col_name)
            escaped_orig = _escape_sql_identifier(f"__orig_{col_name}")

            val_exprs.append(
                f"SUM(CASE WHEN `{escaped}` IS NULL "
                f"AND `{escaped_orig}` IS NOT NULL "
                f"AND lower(trim(`{escaped_orig}`)) NOT IN {_NULL_LIKE_SQL} "
                f"THEN 1 ELSE 0 END) AS chk_{i}"
            )

        temp_view = f"schema_enforce_val_{uuid.uuid4().hex[:8]}"

        try:
            df_cast.createOrReplaceTempView(temp_view)

            result = spark.sql(
                f"SELECT {', '.join(val_exprs)} FROM {temp_view}"
            ).collect()[0]

            for i, (col_name, dtype) in enumerate(col_list):
                fail_count = result[f"chk_{i}"]

                if fail_count and fail_count > 0:
                    if dtype == "date":
                        detail = (
                            f"Tried {len(_DATE_FMTS)} pure formats, "
                            f"ISO 8601, {len(_DATE_WITH_TIME_FMTS)} date+time formats."
                        )
                    elif dtype == "timestamp":
                        detail = (
                            f"Tried native ISO 8601 + "
                            f"{len(_TS_FMTS)} explicit formats."
                        )
                    else:
                        detail = f"Could not cast to '{dtype}'."

                    raise ValueError(
                        f"Column '{col_name}': {fail_count} value(s) failed "
                        f"to parse as {dtype}. {detail}"
                    )

        finally:
            _drop_temp_view(spark, temp_view)

        df_cast = df_cast.drop(*[f"__orig_{c}" for c in validate_cols])

    return df_cast


def infer_schema(df):
    """Infer data types from string columns.
    Args:
        df: Input Spark DataFrame.

    Returns:
        DataFrame with inferred column types applied.
    """
    spark = df.sparkSession

    string_cols = [
        f.name
        for f in df.schema.fields
        if isinstance(f.dataType, StringType)
    ]

    if not string_cols:
        return df

    sample_df = df.limit(1000)
    # Skip caching on serverless (not supported)
    # sample_df = sample_df.cache()
    sample_df.count()

    detected_types = {}
    temp_view = f"infer_schema_sample_{uuid.uuid4().hex[:8]}"

    try:
        sample_df.createOrReplaceTempView(temp_view)

        all_exprs = []

        for i, col_name in enumerate(string_cols):
            escaped = _escape_sql_identifier(col_name)
            pct_stripped = (
                f"regexp_replace(trim(`{escaped}`), '\\s*%\\s*$', '')"
            )
            date_expr = _build_date_case(escaped)
            ts_expr = _build_timestamp_case(escaped)
            null_chk = _null_check_sql(escaped)

            all_exprs += [
                f"SUM(CASE WHEN {null_chk} THEN 1 ELSE 0 END) AS c{i}_total",
                f"SUM(CASE WHEN try_cast({pct_stripped} AS int) IS NOT NULL "
                f"AND {null_chk} THEN 1 ELSE 0 END) AS c{i}_int",
                f"SUM(CASE WHEN try_cast({pct_stripped} AS bigint) IS NOT NULL "
                f"AND {null_chk} THEN 1 ELSE 0 END) AS c{i}_bigint",
                f"SUM(CASE WHEN try_cast({pct_stripped} AS double) IS NOT NULL "
                f"AND {null_chk} THEN 1 ELSE 0 END) AS c{i}_double",
                f"SUM(CASE WHEN ({date_expr}) IS NOT NULL "
                f"AND {null_chk} THEN 1 ELSE 0 END) AS c{i}_date",
                f"SUM(CASE WHEN ({ts_expr}) IS NOT NULL "
                f"AND {null_chk} THEN 1 ELSE 0 END) AS c{i}_ts",
                f"SUM(CASE WHEN ({ts_expr}) IS NOT NULL "
                f"AND (hour({ts_expr}) != 0 "
                f"OR minute({ts_expr}) != 0 "
                f"OR second({ts_expr}) != 0) "
                f"THEN 1 ELSE 0 END) AS c{i}_non_midnight",
            ]

        result = spark.sql(
            f"SELECT {', '.join(all_exprs)} FROM {temp_view}"
        ).collect()[0]
        for i, col_name in enumerate(string_cols):
            total = result[f"c{i}_total"]

            if not total:
                continue

            if result[f"c{i}_int"] == total:
                detected_types[col_name] = "int"

            elif result[f"c{i}_bigint"] == total:
                detected_types[col_name] = "bigint"

            elif result[f"c{i}_double"] == total:
                detected_types[col_name] = "double"

            elif result[f"c{i}_date"] == total:
                detected_types[col_name] = "date"

            elif result[f"c{i}_ts"] == total:
                detected_types[col_name] = (
                    "date"
                    if result[f"c{i}_non_midnight"] == 0
                    else "timestamp"
                )
    finally:
        # Unpersist removed since we're not caching
        _drop_temp_view(spark, temp_view)

    if not detected_types:
        return df

    select_exprs = []

    for col_name in df.columns:
        dtype = detected_types.get(col_name)

        if not dtype:
            select_exprs.append(F.col(col_name))
            continue

        escaped = _escape_sql_identifier(col_name)

        if dtype in ("int", "bigint", "double"):
            select_exprs.append(
                F.expr(
                    f"try_cast(regexp_replace(trim(`{escaped}`), '\\s*%\\s*$', '') AS {dtype})"
                ).alias(col_name)
            )

        elif dtype == "date":
            select_exprs.append(
                F.expr(_build_date_case(escaped)).alias(col_name)
            )

        elif dtype == "timestamp":
            select_exprs.append(
                F.expr(_build_timestamp_case(escaped)).alias(col_name)
            )

        else:
            select_exprs.append(F.col(col_name))

    return df.select(select_exprs)