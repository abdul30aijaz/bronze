"""Parquet connector for reading and writing Parquet files."""

from pyspark.sql import DataFrame, SparkSession


def read_parquet(
    spark: SparkSession,
    path: str = None,
    mergeSchema: bool = True,
    recursiveFileLookup: bool = True,
    datetimeRebaseMode: str = "EXCEPTION",
    int96RebaseMode: str = "EXCEPTION",
    **kwargs,
):
    """
    Read Parquet data into a Spark DataFrame.
    Args:
        spark: Spark session.
        path: Parquet file path.

    Returns:
        DataFrame
    """
    # Configure Parquet read options.
    options = {
        "mergeSchema": mergeSchema,
        "recursiveFileLookup": recursiveFileLookup,
        "datetimeRebaseMode": datetimeRebaseMode,
        "int96RebaseMode": int96RebaseMode,
        **kwargs,
    }

    return spark.read.format("parquet").options(**options).load(path)


def write_parquet(df: DataFrame, path: str, mode: str = "overwrite"):
    """
    Write a DataFrame to Parquet.
    Args:
        df: Spark DataFrame.
        path: Output path.
        mode: Write mode.

    Returns:
        None
    """
    # Save DataFrame in Parquet format.
    df.write.mode(mode).parquet(path)