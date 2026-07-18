"""Delta connector with Unity Catalog support and SCD handling."""

from datetime import datetime, timezone
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def read_delta(
    spark: SparkSession,
    path: str = None,
    versionAsOf: int = None,
    timestampAsOf: str = None,
    readChangeFeed: bool = False,
    **kwargs,
):
    """
    Read Delta table into a Spark DataFrame.
    Args:
        spark: Spark session.
        path: Delta table path or UC table name.
    Returns:
        DataFrame
    """
    options = {"readChangeFeed": readChangeFeed, **kwargs}
    if versionAsOf is not None:
        options["versionAsOf"] = versionAsOf
    if timestampAsOf is not None:
        options["timestampAsOf"] = timestampAsOf

    return spark.read.format("delta").options(**options).load(path)


def _add_row_hash(df: DataFrame, exclude_columns: list = None) -> DataFrame:
    """
    Add SHA256 hash column based on business columns.
    Args:
        df: Input DataFrame.
    Returns:
        DataFrame
    """
    default_exclude = {
        'created_at', 'updated_at', 'ingestion_time', 'ingestion_ts',
        '__effective_from', '__effective_to', '__is_current', '__is_expired',
        '__hash', 'run_id', 'load_timestamp', 'row_hash', '_rescued_data'
    }

    if exclude_columns:
        default_exclude.update(c.lower() for c in exclude_columns)

    hash_cols = sorted([c for c in df.columns if c.lower() not in default_exclude])

    if not hash_cols:
        return df.withColumn('row_hash', F.lit(None).cast('string'))

    return df.withColumn(
        'row_hash',
        F.sha2(
            F.concat_ws('|', *[
                F.coalesce(F.col(c).cast('string'), F.lit('NULL'))
                for c in hash_cols
            ]),
            256
        )
    )


def _is_uc_table(path: str) -> bool:
    """Check if path is a Unity Catalog table name."""
    return "." in path and not path.startswith("/") and not path.startswith("abfss://")


def _ensure_schema_exists(spark: SparkSession, table_name: str):
    """Ensure Unity Catalog schema exists."""
    parts = table_name.split(".")
    if len(parts) != 3:
        raise ValueError(f"Expected catalog.schema.table, got {table_name}")

    catalog, schema, _ = parts
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")


def _get_delta_table(spark: SparkSession, path: str) -> DeltaTable:
    """Get DeltaTable object."""
    return DeltaTable.forName(spark, path) if _is_uc_table(path) else DeltaTable.forPath(spark, path)


def _read_delta_df(spark: SparkSession, path: str) -> DataFrame:
    """Read Delta table as DataFrame."""
    return spark.read.table(path) if _is_uc_table(path) else spark.read.format("delta").load(path)


def _write_delta_df(df: DataFrame, path: str, mode: str = "append"):
    """Write DataFrame to Delta."""
    if _is_uc_table(path):
        df.write.format("delta").mode(mode).saveAsTable(path)
    else:
        df.write.format("delta").mode(mode).save(path)


def _table_exists(spark: SparkSession, path: str) -> bool:
    """Check if Delta table exists."""
    if _is_uc_table(path):
        return spark.catalog.tableExists(path)
    return DeltaTable.isDeltaTable(spark, path)

def _load_overwrite(spark, df, path):
    """Full overwrite load."""
    _write_delta_df(df, path, mode="overwrite")

def _load_append(spark, df, path):
    """Append load."""
    if _table_exists(spark, path):
        existing_schema = _read_delta_df(spark, path).schema
        df = df.select(*existing_schema.fieldNames())

    _write_delta_df(df, path, mode="append")


def _load_scd_type1(spark, df, path, primary_key):
    """SCD Type 1 (upsert)."""
    if not _table_exists(spark, path):
        _write_delta_df(df, path, mode="overwrite")
        return

    delta_table = _get_delta_table(spark, path)
    merge_cond = " AND ".join(f"tgt.{c} = src.{c}" for c in primary_key)
    update_cols = {c: f"src.{c}" for c in df.columns if c not in primary_key}

    (
        delta_table.alias("tgt")
        .merge(df.alias("src"), merge_cond)
        .whenMatchedUpdate(set=update_cols)
        .whenNotMatchedInsertAll()
        .execute()
    )


def _load_scd_type2(spark, df, path, primary_key, enable_expiration=False):
    """SCD Type 2 versioning."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    df_new = df.withColumn("__effective_from", F.lit(now).cast("timestamp")) \
               .withColumn("__effective_to", F.lit(None).cast("timestamp")) \
               .withColumn("__is_current", F.lit(True))

    if not _table_exists(spark, path):
        _write_delta_df(df_new, path, mode="overwrite")
        return

    delta_table = _get_delta_table(spark, path)
    merge_cond = " AND ".join(f"tgt.{c} = src.{c}" for c in primary_key) + " AND tgt.__is_current = true"

    (
        delta_table.alias("tgt")
        .merge(df_new.alias("src"), merge_cond)
        .whenMatchedUpdate(set={
            "__effective_to": F.lit(now).cast("timestamp"),
            "__is_current": F.lit(False),
        })
        .whenNotMatchedInsertAll()
        .execute()
    )


def write_delta(
    spark,
    df,
    path,
    load_type="append",
    primary_key=None,
    enable_expiration=False,
):
    """
    Write DataFrame to Delta with append or SCD strategies.
    Args:
        df: Input DataFrame.
        path: Delta path or UC table.
    Returns:
        None
    """
    primary_key = primary_key or []

    # Ensure target schema exists for Unity Catalog tables before writing
    if _is_uc_table(path):
        _ensure_schema_exists(spark, path)

    df = _add_row_hash(df)

    load_type = load_type.lower().strip()
    if load_type == "scd1":
        load_type = "scd_type1"
    elif load_type == "scd2":
        load_type = "scd_type2"

    if load_type not in ["append", "overwrite", "scd_type1", "scd_type2"]:
        raise ValueError("Unsupported load_type")

    if load_type in ["scd_type1", "scd_type2"] and not primary_key:
        raise ValueError("primary_key required for SCD loads")

    if load_type == "append":
        _load_append(spark, df, path)
    elif load_type == "overwrite":
        _load_overwrite(spark, df, path)
    elif load_type == "scd_type1":
        _load_scd_type1(spark, df, path, primary_key)
    elif load_type == "scd_type2":
        _load_scd_type2(spark, df, path, primary_key, enable_expiration)