"""Read JDBC database sources into Spark DataFrames."""

from pyspark.sql import SparkSession


def read_database(
    spark: SparkSession,
    url: str = None,
    dbtable: str = None,
    fetchsize: str = "10000",
    driver: str = "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    **kwargs,
):
    """
    Read data from a JDBC source.
    Args:
        spark: Spark session.
        url: JDBC connection URL.
        dbtable: Source table name.
    Returns:
        DataFrame
    """
    # Validate required JDBC options.
    if not url or not dbtable:
        raise ValueError(
            "DatabaseConnector requires 'url' and 'dbtable' in connector options."
        )

    # Build JDBC connection options.
    options = {
        "url": url,
        "dbtable": dbtable,
        "fetchsize": fetchsize,
        "driver": driver,
        **kwargs,
    }

    return spark.read.format("jdbc").options(**options).load()