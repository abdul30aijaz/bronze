# Databricks notebook source
# DBTITLE 1, Notebook Description
"""Utilities for creating and managing Databricks ingestion jobs."""

# COMMAND ----------

# DBTITLE 1, Magic Command
# MAGIC %run ../env

# COMMAND ----------

# DBTITLE 1, Imports
import json
from typing import Any

from utils.spark_utils import get_or_create_spark
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import compute, iam, jobs
from pyspark.sql import SparkSession

# COMMAND ----------

# DBTITLE 1, Session & Secrets
# Initialize Spark session
dbutils: Any
spark = SparkSession.builder.getOrCreate()
spark = get_or_create_spark(spark=spark, app_name="MDIF-DeployJobs")

# DATABRICKS_HOST and DATABRICKS_TOKEN are already loaded from env.py
# No Service Principal / team-members needed on Free Edition

# COMMAND ----------

# DBTITLE 1, Helper — Format Job Name
def databricks_job_name(job_name: str) -> str:
    """
    Generate standardized Databricks job name.
    Args:
        job_name: Input job name.
    Returns:
        str: Formatted job name.
    """
    return f"MDIF_{job_name.upper().replace(' ', '_')}"

# COMMAND ----------

# DBTITLE 1, Helper — Fetch Existing Jobs
def get_existing_jobs(w: WorkspaceClient) -> dict:
    """
    Retrieve all existing Databricks jobs.
    Args:
        w: Workspace client instance.
    Returns:
        dict: Mapping of job names to job IDs.
    """
    return {
        job.settings.name: job.job_id
        for job in w.jobs.list(expand_tasks=False)
        if job.settings and job.settings.name
    }

# COMMAND ----------

# DBTITLE 1, Build Job Settings
def _build_job_settings(
    job_name: str,
    table_names: list,
    trigger_row=None,
    source_path: str = None,
) -> dict:
    """
    Build Databricks job configuration.
    Args:
        job_name: Name of the job.
        table_names: List of tables to process.
        trigger_row: Trigger configuration row.
        source_path: Landing source path for file arrival triggers.
    Returns:
        dict: Complete job settings dictionary.
    """
    # Resolve trigger configuration
    trigger_type = trigger_row["type"] if trigger_row else None
    schedule = None
    file_arrival = None

    if trigger_type == "schedule" and trigger_row["cron"]:
        schedule = jobs.CronSchedule(
            quartz_cron_expression=trigger_row["cron"],
            timezone_id=trigger_row["timezone"] or "UTC",
            pause_status=jobs.PauseStatus.UNPAUSED,
        )

    elif trigger_type == "file_arrival":
        if source_path:
            file_arrival = jobs.FileArrivalTriggerConfiguration(
                url=source_path + "/",
                min_time_between_triggers_seconds=300,
            )
        else:
            print(
                "warning: file_arrival trigger defined but source_path is missing"
            )

    # Build base job settings (serverless compute — no job_clusters)
    settings = dict(
        name=databricks_job_name(job_name),
        tags={"mdif_job_name": job_name, "managed_by": "cd_create_jobs"},
        tasks=[
            # Load environment
            jobs.Task(
                task_key="start_job",
                notebook_task=jobs.NotebookTask(
                    notebook_path=f"{WORKSPACE_PATH}/start_job",
                    source=jobs.Source.WORKSPACE,
                    base_parameters={
                        "JOB_NAME": job_name,
                    },
                ),
            ),

            # Read metadata
            jobs.Task(
                task_key="read_metadata",
                depends_on=[jobs.TaskDependency(task_key="start_job")],
                notebook_task=jobs.NotebookTask(
                    notebook_path=f"{WORKSPACE_PATH}/read_metadata",
                    source=jobs.Source.WORKSPACE,
                    base_parameters={
                        "JOB_NAME": job_name,
                        "TABLE_FILTER": "",
                    },
                ),
            ),

            # Ingestor task (parallel execution per table)
            jobs.Task(
                task_key="ingestor",
                depends_on=[
                    jobs.TaskDependency(task_key="read_metadata")
                ],
                for_each_task=jobs.ForEachTask(
                    inputs="{{tasks.read_metadata.values.table_names}}",
                    concurrency=max(1, min(len(table_names), 100)),
                    task=jobs.Task(
                        task_key="ingestor_iteration",
                        libraries=[
                            compute.Library(requirements=REQUIREMENTS)
                        ],
                        notebook_task=jobs.NotebookTask(
                            notebook_path=f"{WORKSPACE_PATH}/main",
                            source=jobs.Source.WORKSPACE,
                            base_parameters={
                                "table_name": "{{input}}"
                            },
                        ),
                    ),
                ),
            ),

            # Dashboard updates
            jobs.Task(
                task_key="dashboard_updates",
                depends_on=[
                    jobs.TaskDependency(task_key="ingestor")
                ],
                run_if=jobs.RunIf.ALL_DONE,
                notebook_task=jobs.NotebookTask(
                    notebook_path=f"{WORKSPACE_PATH}/dashboard_updates",
                    source=jobs.Source.WORKSPACE,
                ),
            ),

            # Notifications
            jobs.Task(
                task_key="notify_job",
                depends_on=[
                    jobs.TaskDependency(task_key="ingestor")
                ],
                run_if=jobs.RunIf.ALL_DONE,
                notebook_task=jobs.NotebookTask(
                    notebook_path=f"{WORKSPACE_PATH}/notify_job",
                    source=jobs.Source.WORKSPACE,
                ),
            ),
        ],
    )
    # TODO: run_as and access_control_list removed (not supported on Free Edition serverless)

    # Attach trigger if configured
    if schedule:
        settings["schedule"] = schedule
    elif file_arrival:
        settings["trigger"] = jobs.TriggerSettings(
            file_arrival=file_arrival,
            pause_status=jobs.PauseStatus.UNPAUSED,
        )

    return settings

# COMMAND ----------

# DBTITLE 1, Create Job
def create_job(
    w: WorkspaceClient,
    job_name: str,
    table_names: list,
    trigger_row=None,
    source_path: str = None,
) -> None:
    """
    Create a new Databricks job.
    Args:
        w: Workspace client.
        job_name: Name of the job.
        table_names: List of tables to process.
        trigger_row: Trigger configuration row.
        source_path: Landing source path.
    Returns:
        None
    """
    new_job = w.jobs.create(
        **_build_job_settings(
            job_name,
            table_names,
            trigger_row,
            source_path,
        )
    )
    print(
        f"created: {databricks_job_name(job_name)} | job_id={new_job.job_id}"
    )


# COMMAND ----------

# DBTITLE 1, Update Job
def update_job(
    w: WorkspaceClient,
    job_id: int,
    job_name: str,
    table_names: list,
    trigger_row=None,
    source_path: str = None,
) -> None:
    """
    Update an existing Databricks job configuration.
    Args:
        w: Workspace client.
        job_id: Databricks job ID.
        job_name: Name of the job.
        table_names: List of tables to process.
        trigger_row: Trigger configuration row.
        source_path: Landing source path.
    Returns:
        None
    """
    settings = _build_job_settings(
        job_name,
        table_names,
        trigger_row,
        source_path,
    )

    # Remove ACLs before reset (not supported in reset API)
    settings.pop("access_control_list", None)

    w.jobs.reset(
        job_id=job_id,
        new_settings=jobs.JobSettings(**settings),
    )

    print(
        f"updated: {databricks_job_name(job_name)} | job_id={job_id}"
    )


# COMMAND ----------

# DBTITLE 1, Main — Deploy Job
def main() -> None:
    """
    Deploy or update Databricks ingestion job.
    Reads metadata and triggers job creation or update.
    Returns:
        None
    """
    print(
        f"Deploy Jobs | job={JOB_NAME} | workspace={WORKSPACE_PATH}"
    )

    w = WorkspaceClient(
        host=DATABRICKS_HOST,
        token=DATABRICKS_TOKEN,
    )

    rows = (
        spark.table(DEFAULT_CONTROL_TABLE)
        .select("table_name", "trigger", "ingestion_steps")
        .where(f"job_name = '{JOB_NAME}'")
        .collect()
    )

    table_names = [row["table_name"] for row in rows]

    if not table_names:
        raise ValueError(
            f"No tables found for job: {JOB_NAME}"
        )

    # Extract job-level configuration
    trigger_row = rows[0]["trigger"] if rows else None
    source_path = (
        rows[0]["ingestion_steps"]["landing"]["source_path"]
        if rows[0]["ingestion_steps"]
        and rows[0]["ingestion_steps"]["landing"]
        else None
    )

    trigger_log = (
        trigger_row["type"]
        if trigger_row and trigger_row["type"]
        else "none"
    )

    print(f"tables to deploy: {table_names}")
    print(f"trigger: {trigger_log}")

    existing_jobs = get_existing_jobs(w)
    db_job_name = databricks_job_name(JOB_NAME)

    try:
        if db_job_name in existing_jobs:
            update_job(
                w,
                existing_jobs[db_job_name],
                JOB_NAME,
                table_names,
                trigger_row,
                source_path,
            )
        else:
            create_job(
                w,
                JOB_NAME,
                table_names,
                trigger_row,
                source_path,
            )
    except Exception as exc:
        raise RuntimeError(
            f"Failed to deploy '{JOB_NAME}': {exc}"
        ) from exc

    print(f"deploy complete | job={db_job_name}")


# COMMAND ----------

# DBTITLE 1, Execute
main()