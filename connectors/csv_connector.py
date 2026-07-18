"""CSV connector for reading and writing CSV files."""

from pyspark.sql import DataFrame, SparkSession


def read_csv(
    spark: SparkSession,
    path: str = None,
    header: bool = True,
    sep: str = ",",
    quote: str = '"',
    escape: str = "\\",
    comment: str = "#",
    encoding: str = "UTF-8",
    nullValue: str = "NA",
    nanValue: str = "NaN",
    positiveInf: str = "Inf",
    negativeInf: str = "-Inf",
    emptyValue: str = "",
    ignoreLeadingWhiteSpace: bool = True,
    ignoreTrailingWhiteSpace: bool = True,
    multiLine: bool = False,
    dateFormat: str = "yyyy-MM-dd",
    timestampFormat: str = "yyyy-MM-dd HH:mm:ss",
    locale: str = "en-US",
    maxColumns: int = 20480,
    maxCharsPerColumn: int = -1,
    maxMalformedLogPerPartition: int = 10,
    mode: str = "PERMISSIVE",
    columnNameOfCorruptRecord: str = "_corrupt_record",
    lineSep: str = "\n",
    charToEscapeQuoteEscaping: str = "\\",
    unescapedQuoteHandling: str = "STOP_AT_DELIMITER",
    enforceSchema: bool = True,
    recursiveFileLookup: bool = False,
    pathGlobFilter: str = "*.csv",
):
    """
    Read CSV data into a Spark DataFrame.

    Args:
        spark: Spark session.
        path: CSV file path.

    Returns:
        DataFrame
    """
    return (
        spark.read.format("csv")
        .option("header", header)
        .option("sep", sep)
        .option("quote", quote)
        .option("escape", escape)
        .option("comment", comment)
        .option("encoding", encoding)
        .option("nullValue", nullValue)
        .option("nanValue", nanValue)
        .option("positiveInf", positiveInf)
        .option("negativeInf", negativeInf)
        .option("emptyValue", emptyValue)
        .option("ignoreLeadingWhiteSpace", ignoreLeadingWhiteSpace)
        .option("ignoreTrailingWhiteSpace", ignoreTrailingWhiteSpace)
        .option("multiLine", multiLine)
        .option("dateFormat", dateFormat)
        .option("timestampFormat", timestampFormat)
        .option("locale", locale)
        .option("maxColumns", maxColumns)
        .option("maxCharsPerColumn", maxCharsPerColumn)
        .option("maxMalformedLogPerPartition", maxMalformedLogPerPartition)
        .option("mode", mode)
        .option("columnNameOfCorruptRecord", columnNameOfCorruptRecord)
        .option("lineSep", lineSep)
        .option("charToEscapeQuoteEscaping", charToEscapeQuoteEscaping)
        .option("unescapedQuoteHandling", unescapedQuoteHandling)
        .option("enforceSchema", enforceSchema)
        .option("recursiveFileLookup", recursiveFileLookup)
        .option("pathGlobFilter", pathGlobFilter)
        .load(path)
    )


def write_csv(df: DataFrame, path: str, mode: str = "overwrite"):
    """
    Write a DataFrame to CSV.

    Args:
        df: Spark DataFrame.
        path: Output path.
        mode: Write mode.

    Returns:
        None
    """
    # Write CSV with header row.
    df.write.mode(mode).option("header", "true").csv(path)