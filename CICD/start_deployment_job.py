# Databricks notebook source
# DBTITLE 1, Magic Command
# MAGIC %run /Workspace/Shared/bronze/env

# COMMAND ----------

# DBTITLE 1, Check Metadata Directory
def check_metadata_dir(job_name: str) -> list:
    """
    Confirm the metadata directory exists and contains JSON files for one job.
    Returns the table names (JSON filenames without extension).
    Does not parse file contents — that's validate_metadata's job.
    """
    metadata_dir = f"{METADATA_BASE_DIR}/{job_name}"

    try:
        files = dbutils.fs.ls(metadata_dir)
    except Exception as exc:
        raise ValueError(f"Metadata dir not found for job '{job_name}': {metadata_dir}") from exc

    json_files = [f.name for f in files if f.name.endswith(".json")]

    if not json_files:
        raise ValueError(f"No JSON files found for job '{job_name}' in {metadata_dir}")

    table_names = [f[:-len(".json")] for f in json_files]

    return table_names

# COMMAND ----------

# DBTITLE 1, Run Check
def run() -> None:
    require_job_names()

    print(f"Metadata base dir = {METADATA_BASE_DIR}")
    print(f"Jobs To Deploy ={JOB_NAMES}")

    failures = {}
    for job_name in JOB_NAMES:
        try:
            table_names = check_metadata_dir(job_name)
            print(f"{job_name} | {len(table_names)} table(s): {', '.join(table_names)}")
        except ValueError as exc:
            failures[job_name] = str(exc)
            print(f"FAIL | {job_name} | {exc}")

    if failures:
        raise ValueError(f"Check failed for {len(failures)} job(s): {list(failures.keys())}")

    print(f"{len(JOB_NAMES)} job(s) ")

# COMMAND ----------

# DBTITLE 1, Execute
run()