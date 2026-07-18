"""JSON connector for reading JSON files into Spark DataFrames."""

from pyspark.sql import DataFrame, SparkSession


def read_json(
    spark: SparkSession,
    path: str = None,
    multiLine: bool = True,
    mode: str = "PERMISSIVE",
    columnNameOfCorruptRecord: str = "_corrupt_record",
    dateFormat: str = "yyyy-MM-dd",
    timestampFormat: str = "yyyy-MM-dd HH:mm:ss",
    encoding: str = "UTF-8",
    **kwargs,
):
    """
    Read JSON data into a Spark DataFrame.
    Args:
        spark: Spark session.
        path: JSON file path.
    Returns:
        DataFrame
    """
    # Configure JSON read options.
    options = {
        "multiLine": multiLine,
        "mode": mode,
        "columnNameOfCorruptRecord": columnNameOfCorruptRecord,
        "dateFormat": dateFormat,
        "timestampFormat": timestampFormat,
        "encoding": encoding,
        **kwargs,
    }

    return spark.read.format("json").options(**options).load(path)