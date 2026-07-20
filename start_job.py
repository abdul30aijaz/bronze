# Databricks notebook source
# MAGIC %md
# MAGIC MDIF Start Job ENTRY POINT FOR METADATA DATA DRIVEN INGESTION FRAMEWORK
# MAGIC
# MAGIC First task in the pipeline (start_job -> read_metadata -> main).

# COMMAND ----------

# DBTITLE 1,Load Environment
# MAGIC %run /Workspace/Shared/bronze/env

# COMMAND ----------

# DBTITLE 1,Environment Summary
print(f"DEV_PATH: {DEV_PATH}")
print(f"DEFAULT_CONTROL_TABLE: {DEFAULT_CONTROL_TABLE}")