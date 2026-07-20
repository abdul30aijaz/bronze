# Databricks notebook source
# DBTITLE 1, Notebook Description
"""Metadata-driven control table sync for MDIF ingestion framework."""

# COMMAND ----------

# DBTITLE 1, Magic Command
# MAGIC %run /Workspace/Shared/bronze/env

# COMMAND ----------

# DBTITLE 1, Imports
import json
from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
    TimestampType,
    ArrayType,
)
from utils.metadata_utils import read_json_files

# COMMAND ----------

# DBTITLE 1, Ensure Control Table Exists
def ensure_table(spark: SparkSession) -> None:
    """
    Create schema and control table if they do not exist.
    Args:
        spark: Spark session.
    Returns:
        None
    """
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")

    ddl = f"""
    CREATE TABLE IF NOT EXISTS {DEFAULT_CONTROL_TABLE} (
        job_name STRING NOT NULL,
        table_name STRING NOT NULL,
        region STRING,
        market STRING,
        domain STRING,
        source_name STRING,

        ingestion_steps STRUCT<
            landing: STRUCT<
                source_path: STRING,
                file_format: STRING,
                format_options: STRING,
                format_options_key: STRING,
                notebook_path: STRING,
                parameters: STRING,
                pii_columns: ARRAY<STRING>
            >,
            raw: STRUCT<
                schema_config: STRING,
                deduplication: BOOLEAN
            >,
            raw_trusted: STRUCT<
                load_type: STRING,
                primary_key: ARRAY<STRING>,
                enable_expiration: BOOLEAN,
                validations: ARRAY<STRING>,
                schema_evolution: BOOLEAN
            >
        >,

        notify STRUCT<
            emails: ARRAY<STRING>
        >,

        trigger STRUCT<
            type: STRING,
            cron: STRING,
            timezone: STRING
        >,

        schema ARRAY<STRUCT<
            column_name: STRING,
            data_type: STRING
        >>,

        is_active BOOLEAN,
        sync_timestamp TIMESTAMP
    )
    USING DELTA
    COMMENT 'MDIF pipeline control table'
    """

    spark.sql(ddl)
    print(f"Table ready: {DEFAULT_CONTROL_TABLE}")



# COMMAND ----------

# DBTITLE 1, Utility — Serialize Values
def _serialize(value) -> str | None:
    """
    Serialize non-string values to JSON.
    Args:
        value: Input value.
    Returns:
        str | None: Serialized value.
    """
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value)


# COMMAND ----------

# DBTITLE 1, Flatten Metadata Row
def flatten_to_row(job_name: str, table_name: str, data: dict, sync_timestamp) -> dict:
    """
    Convert metadata JSON into flat row structure.
    Args:
        job_name: Job identifier.
        table_name: Table name.
        data: Raw metadata dictionary.
        sync_timestamp: Sync timestamp.
    Returns:
        dict: Flattened row.
    """
    ingestion_steps = data.get("ingestion_steps", {})
    landing_cfg = ingestion_steps.get("landing", {})
    raw_cfg = ingestion_steps.get("raw", {})
    rt_cfg = ingestion_steps.get("raw_trusted", {})

    landing = None
    if landing_cfg:
        is_api = "parameters" in landing_cfg

        format_opts_key = next(
            (k for k in landing_cfg if k.endswith("_options")),
            None,
        )
        format_opts_val = landing_cfg.get(format_opts_key) if format_opts_key else None

        landing = {
            "source_path": landing_cfg.get("source_path"),
            "file_format": landing_cfg.get("file_format"),
            "format_options": _serialize(format_opts_val),
            "format_options_key": format_opts_key,
            "notebook_path": (
                landing_cfg["parameters"].get("notebook_path") if is_api else None
            ),
            "parameters": _serialize(landing_cfg.get("parameters")),
            "pii_columns": landing_cfg.get("pii_columns", []),
        }

    raw = (
        {
            "schema_config": raw_cfg.get("schema_config"),
            "deduplication": raw_cfg.get("deduplication"),
        }
        if raw_cfg
        else None
    )

    raw_trusted = (
        {
            "load_type": rt_cfg.get("load_type"),
            "primary_key": rt_cfg.get("primary_key", []),
            "enable_expiration": rt_cfg.get("enable_expiration"),
            "validations": rt_cfg.get("validations", []),
            "schema_evolution": rt_cfg.get("schema_evolution"),
        }
        if rt_cfg
        else None
    )

    ingestion_steps_struct = (
        {"landing": landing, "raw": raw, "raw_trusted": raw_trusted}
        if landing or raw or raw_trusted
        else None
    )

    notify_cfg = data.get("notify") or {}
    notify = {"emails": notify_cfg.get("emails", [])} if notify_cfg else None

    trigger_cfg = data.get("trigger")
    trigger = (
        {
            "type": trigger_cfg.get("type", ""),
            "cron": trigger_cfg.get("cron"),
            "timezone": trigger_cfg.get("timezone"),
        }
        if trigger_cfg
        else None
    )

    return {
        "job_name": job_name,
        "table_name": table_name,
        "region": data.get("region"),
        "market": data.get("market"),
        "domain": data.get("domain"),
        "source_name": data.get("source_name"),
        "ingestion_steps": ingestion_steps_struct,
        "notify": notify,
        "trigger": trigger,
        "schema": data.get("schema", []),
        "is_active": data.get("is_active", True),
        "sync_timestamp": sync_timestamp,
    }


# COMMAND ----------

# DBTITLE 1, Schema Definition
INCOMING_SCHEMA = StructType([
    StructField("job_name",    StringType(), False),
    StructField("table_name",  StringType(), False),
    StructField("region",      StringType(), True),
    StructField("market",      StringType(), True),
    StructField("domain",      StringType(), True),
    StructField("source_name", StringType(), True),
    StructField("ingestion_steps", StructType([
        StructField("landing", StructType([
            StructField("source_path",        StringType(),           True),
            StructField("file_format",        StringType(),           True),
            StructField("format_options",     StringType(),           True),
            StructField("format_options_key", StringType(),           True),
            StructField("notebook_path",      StringType(),           True),
            StructField("parameters",         StringType(),           True),
            StructField("pii_columns",        ArrayType(StringType()), True),
        ]), True),
        StructField("raw", StructType([
            StructField("schema_config", StringType(),  True),
            StructField("deduplication", BooleanType(), True),
        ]), True),
        StructField("raw_trusted", StructType([
            StructField("load_type",         StringType(),             True),
            StructField("primary_key",       ArrayType(StringType()),  True),
            StructField("enable_expiration", BooleanType(),            True),
            StructField("validations",       ArrayType(StringType()),  True),
            StructField("schema_evolution",  BooleanType(),            True),
        ]), True),
    ]), True),
    StructField("notify", StructType([
        StructField("emails", ArrayType(StringType()), True),
    ]), True),
    StructField("trigger", StructType([
        StructField("type",     StringType(), True),
        StructField("cron",     StringType(), True),
        StructField("timezone", StringType(), True),
    ]), True),
    StructField("schema", ArrayType(StructType([
        StructField("column_name", StringType(), True),
        StructField("data_type",   StringType(), True),
    ])), True),
    StructField("is_active",      BooleanType(),   True),
    StructField("sync_timestamp", TimestampType(), True),
])


# COMMAND ----------

# DBTITLE 1, Align Schema Helper
def _cast_field(field_path: str, field):
    """
    Cast nested schema fields to match target schema.
    Args:
        field_path: Column path.
        field: Schema field.
    Returns:
        Column expression.
    """
    dtype = field.dataType

    if isinstance(dtype, StructType):
        return F.struct(*[
            _cast_field(f"{field_path}.{sf.name}", sf).alias(sf.name)
            for sf in dtype.fields
        ])

    if isinstance(dtype, ArrayType) and isinstance(dtype.elementType, StructType):
        return F.transform(
            F.col(field_path).cast(ArrayType(dtype.elementType)),
            lambda x: F.struct(*[
                x[sf.name].cast(sf.dataType).alias(sf.name)
                for sf in dtype.elementType.fields
            ]),
        )

    return F.col(field_path).cast(dtype)


def align_incoming_to_target(incoming, spark: SparkSession):
    """
    Align incoming dataframe to target control table schema.
    Args:
        incoming: Input dataframe.
        spark: Spark session.
    Returns:
        DataFrame: Aligned dataframe.
    """
    try:
        target_schema = spark.table(DEFAULT_CONTROL_TABLE).schema
    except Exception as e:
        print(f"Skipping alignment: {e}")
        return incoming

    return incoming.select([
        _cast_field(f.name, f).alias(f.name)
        for f in target_schema
    ])


# COMMAND ----------

# DBTITLE 1, Main Execution
def run() -> None:
    """
    Sync metadata into Delta control table.
    Returns:
        None
    """
    spark = SparkSession.builder.appName(
        "MDIF-SyncControlTable"
    ).getOrCreate()

    current_user = spark.sql("SELECT current_user()").collect()[0][0]
    sync_ts = datetime.now()

    print(
        f"Sync Control Table | user={current_user} | "
        f"jobs={JOB_NAMES} | target={DEFAULT_CONTROL_TABLE}"
    )

    all_rows = []

    for job_name in JOB_NAMES:
        metadata_dir = f"{METADATA_BASE_DIR}/{job_name}"
        entries = read_json_files(dbutils, metadata_dir)

        rows = [
            flatten_to_row(jn, tn, data, sync_ts)
            for jn, tn, data in entries
        ]

        all_rows.extend(rows)

    ensure_table(spark)

    incoming = spark.createDataFrame(all_rows, schema=INCOMING_SCHEMA)
    incoming = align_incoming_to_target(incoming, spark)

    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    DeltaTable.forName(spark, DEFAULT_CONTROL_TABLE) \
        .alias("tgt") \
        .merge(
            incoming.alias("src"),
            "tgt.job_name = src.job_name AND tgt.table_name = src.table_name",
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

    print(
        f"Sync complete | {len(all_rows)} rows | jobs={len(JOB_NAMES)}"
    )


run()