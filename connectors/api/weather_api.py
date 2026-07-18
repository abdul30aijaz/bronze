# Databricks notebook source
import sys
BRONZE_PATH = "/Workspace/Shared/Mars_AZURE_Pet_Care_MDIF/bronze"
if BRONZE_PATH not in sys.path:
    sys.path.insert(0, BRONZE_PATH)

from utils.spark_utils import get_or_create_spark
spark = get_or_create_spark(spark=spark)

# COMMAND ----------

import requests
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, FloatType, DateType
)
from utils.logging_utils import log_event, get_run_id

# COMMAND ----------

# ── Widgets passed from _run_notebook() ───────────────────────
dbutils.widgets.text("table_name",  "", "Table Name")
dbutils.widgets.text("run_id",      "", "Run ID")
dbutils.widgets.text("output_path", "", "Output Path")
dbutils.widgets.text("latitude",    "", "Latitude")
dbutils.widgets.text("longitude",   "", "Longitude")
dbutils.widgets.text("past_days",   "", "Past Days")

table_name  = dbutils.widgets.get("table_name")
run_id      = dbutils.widgets.get("run_id")
output_path = dbutils.widgets.get("output_path")
latitude    = float(dbutils.widgets.get("latitude"))
longitude   = float(dbutils.widgets.get("longitude"))
past_days   = int(dbutils.widgets.get("past_days"))
print(dbutils.widgets.getAll())

# COMMAND ----------

# ── Validate required params ───────────────────────────────────
if not output_path:
    raise ValueError("output_path is empty — not passed from _run_notebook")
if not table_name:
    raise ValueError("table_name is empty — not passed from _run_notebook")

# COMMAND ----------

# ── API Config ─────────────────────────────────────────────────
API_URL = "https://api.open-meteo.com/v1/forecast"
PARAMS  = {
    "latitude":  latitude,
    "longitude": longitude,
    "daily": ",".join([
        "temperature_2m_max",
        "temperature_2m_min",
        "precipitation_sum",
        "windspeed_10m_max"
    ]),
    "timezone":  "Asia/Kolkata",
    "past_days": past_days
}

# COMMAND ----------

# ── Fetch from API ─────────────────────────────────────────────
try:
    log_event(spark, "INFO",
              f"Weather API call started: {table_name}",
              run_id, function_name="weather_api",
              status="running", table_name=table_name,
              additional_data={"url": API_URL, "latitude": str(latitude),
                               "longitude": str(longitude), "past_days": str(past_days)})

    response = requests.get(API_URL, params=PARAMS, timeout=30)
    response.raise_for_status()
    raw      = response.json()

    log_event(spark, "INFO",
              f"Weather API call complete: {table_name}",
              run_id, function_name="weather_api",
              status="success", table_name=table_name)

except Exception as exc:
    log_event(spark, "ERROR",
              f"Weather API call FAILED: {table_name} -- {exc}",
              run_id, function_name="weather_api",
              status="failed", table_name=table_name,
              additional_data={"error": str(exc)})
    raise

# COMMAND ----------

# ── Flatten response into rows ─────────────────────────────────
try:
    daily   = raw["daily"]
    records = [
        {
            "date":                str(date),
            "temperature_max":     float(tmax)   if tmax   is not None else None,
            "temperature_min":     float(tmin)   if tmin   is not None else None,
            "precipitation_sum":   float(precip) if precip is not None else None,
            "windspeed_max":       float(wind)   if wind   is not None else None,
            "latitude":            float(raw["latitude"]),
            "longitude":           float(raw["longitude"]),
        }
        for date, tmax, tmin, precip, wind in zip(
            daily["time"],
            daily["temperature_2m_max"],
            daily["temperature_2m_min"],
            daily["precipitation_sum"],
            daily["windspeed_10m_max"],
        )
    ]

    log_event(spark, "INFO",
              f"Response flattened: {table_name} -- {len(records)} records",
              run_id, function_name="weather_api",
              status="success", table_name=table_name,
              additional_data={"record_count": str(len(records))})

except Exception as exc:
    log_event(spark, "ERROR",
              f"Response flatten FAILED: {table_name} -- {exc}",
              run_id, function_name="weather_api",
              status="failed", table_name=table_name,
              additional_data={"error": str(exc)})
    raise

# COMMAND ----------

# ── Create Spark DataFrame ─────────────────────────────────────
try:
    df        = spark.createDataFrame(records)
    row_count = df.count()

    log_event(spark, "INFO",
              f"DataFrame created: {table_name} -- {row_count} rows",
              run_id, function_name="weather_api",
              status="success", table_name=table_name,
              additional_data={"row_count": str(row_count)})

except Exception as exc:
    log_event(spark, "ERROR",
              f"DataFrame creation FAILED: {table_name} -- {exc}",
              run_id, function_name="weather_api",
              status="failed", table_name=table_name,
              additional_data={"error": str(exc)})
    raise

# COMMAND ----------

print(f"output_path = '{output_path}'")
print(f"type = {type(output_path)}")
print(f"length = {len(output_path)}")

# ── Write to output_path as CSV ────────────────────────────────
from connectors.csv_connector import write_csv

try:
    write_csv(df, output_path)
    row_count = df.count()

    log_event(spark, "INFO",
              f"API output written: {table_name} -- {row_count} rows to {output_path}",
              run_id, function_name="weather_api",
              status="success", table_name=table_name,
              additional_data={"output_path": output_path,
                               "row_count":   str(row_count),
                               "format":      "csv"})

except Exception as exc:
    log_event(spark, "ERROR",
              f"API output write FAILED: {table_name} -- {exc}",
              run_id, function_name="weather_api",
              status="failed", table_name=table_name,
              additional_data={"output_path": output_path, "error": str(exc)})
    raise