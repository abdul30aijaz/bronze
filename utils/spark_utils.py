"""Spark session utility"""
from pyspark.sql import SparkSession


def get_or_create_spark(
    spark: SparkSession | None = None,
    app_name: str = "MDIF",
) -> SparkSession:
    """Return existing SparkSession, or create one."""
    if spark is not None:
        return spark

    return SparkSession.builder.appName(app_name).getOrCreate()
