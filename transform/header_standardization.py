import re
from pyspark.sql import DataFrame

"""Utilities for standardizing DataFrame column headers."""


def _to_snake_case(name: str) -> str:
    """
    Convert a string into snake_case format.
    Args:
        name (str): Original column name.
    Returns:
        str: Normalized snake_case column name.
    """
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", s)
    s = re.sub(r"([a-zA-Z])(\d)", r"\1_\2", s)
    s = re.sub(r"[^a-zA-Z0-9]", "_", s)
    s = re.sub(r"_+", "_", s)
    return s.strip("_").lower()


def standardize_headers(df: DataFrame) -> DataFrame:
    """
    Convert all DataFrame column names to snake_case.
    Args:
        df (DataFrame): Input Spark DataFrame.
    Returns:
        DataFrame: DataFrame with standardized column names.
    """
    original = df.columns
    snake = [_to_snake_case(c) for c in original]

    for old, new in zip(original, snake):
        if old != new:
            df = df.withColumnRenamed(old, new)

    return df