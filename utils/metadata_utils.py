"""
Metadata Utilities

Shared helper functions for JSON parsing, serialization, and Spark Row -> dict
conversion, used across MDIF pipeline notebooks.
"""

import datetime
import json
from pyspark.sql import Row


def json_safe(obj):
    """Converts datetime objects to ISO format """
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def row_to_dict(obj):
    """Recursively converts Spark Row objects to plain dictionaries."""
    if isinstance(obj, Row):
        return {k: row_to_dict(v) for k, v in obj.asDict().items()}
    elif isinstance(obj, list):
        return [row_to_dict(i) for i in obj]
    return obj


def read_json_files(dbutils, metadata_dir: str, *, require_sorted: bool = True) -> list:
    """
    Load all JSON metadata configs from a DBFS directory.

    Args:
        dbutils: Databricks dbutils instance, passed in from the calling
                  notebook (not available inside a plain .py module).
        metadata_dir: Directory containing JSON files.
        require_sorted: Whether to sort files by name for stable processing order.

    Returns:
        list: (job_name, table_name, json_data) tuples.
    """
    try:
        files = dbutils.fs.ls(metadata_dir)
    except Exception as exc:
        raise ValueError(f"Metadata dir not found: {metadata_dir}") from exc

    if require_sorted:
        files = sorted(files, key=lambda x: x.name)

    job_name = metadata_dir.rstrip("/").split("/")[-1]

    entries = []
    for file in files:
        if not file.name.endswith(".json"):
            continue

        table_name = file.name.replace(".json", "")

        try:
            content = dbutils.fs.head(file.path)
            data = json.loads(content)
        except Exception as exc:
            print(f"failed reading {file.path}: {exc}")
            continue

        entries.append((job_name, table_name, data))
        print(f"read: {job_name}/{table_name}")

    if not entries:
        raise ValueError(f"No JSON files found under: {metadata_dir}")

    return entries