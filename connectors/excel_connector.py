"""Excel connector for reading Excel files into Spark DataFrames."""
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning, module="openpyxl")
import os
import re
import tempfile

import pandas as pd
from pyspark.dbutils import DBUtils
from pyspark.sql import DataFrame, SparkSession



def _strip_empty(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove empty rows and columns from the DataFrame.
    Args:
        df: Input pandas DataFrame.
    Returns:
        pd.DataFrame: Cleaned DataFrame with normalized column names.
    """
    # Remove rows and columns containing only null values.
    df = df.dropna(how="all").dropna(axis=1, how="all")

    # Identify rows and columns containing only blank strings.
    all_blank_row = df.apply(
        lambda row: row.map(lambda x: str(x).strip() == "" if pd.notna(x) else False).all(),
        axis=1,
    )
    all_blank_col = df.apply(
        lambda col: col.map(lambda x: str(x).strip() == "" if pd.notna(x) else False).all(),
        axis=0,
    )

    df = df[~all_blank_row].loc[:, ~all_blank_col]

    # Normalize column names for Spark compatibility.
    new_cols = []
    for col in df.columns:
        if isinstance(col, int):
            new_cols.append(f"col_{col}")
        elif isinstance(col, str) and re.match(r"^Unnamed:\s*\d+", col):
            new_cols.append(f"col_{col.split(':')[1].strip()}")
        else:
            new_cols.append(str(col))

    df.columns = new_cols
    return df.reset_index(drop=True)


def _resolve_sheet(excel_options: dict):
    """
    Resolve the sheet to read from the Excel file.
    Args:
        excel_options: Excel reader configuration.
    Returns:
        str | int: Sheet name or sheet index.
    """
    # Use sheet_name when provided.
    sheet_name = excel_options.get("sheet_name")
    if sheet_name is not None and str(sheet_name).strip() != "":
        if isinstance(sheet_name, int):
            return sheet_name
        try:
            return int(sheet_name)
        except (ValueError, TypeError):
            return str(sheet_name)

    # Fall back to sheet_index if available.
    if "sheet_index" in excel_options:
        return int(excel_options["sheet_index"])

    # Default to the first sheet.
    return 0


def read_excel(
    spark: SparkSession,
    path: str,
    excel_options: dict = None,
) -> DataFrame:
    """
    Read an Excel file into a Spark DataFrame.
    Args:
        spark: Spark session.
        path: Excel file path.
        excel_options: Excel reader configuration.
    Returns:
        DataFrame: Spark DataFrame containing the Excel data.
    """
    if excel_options is None:
        excel_options = {}

    # Resolve the sheet to read.
    sheet = _resolve_sheet(excel_options)

    # Configure row selection.
    header_row = int(excel_options["header_row"])
    data_start_row = int(
        excel_options.get("data_start_row", header_row + 1)
    )
    data_end_row = excel_options.get("data_end_row")

    # Configure column selection.
    usecols = excel_options.get("usecols")

    # Build pandas read options.
    pandas_kwargs = {
        "engine": "openpyxl",
        "sheet_name": sheet,
        "header": 0,
        "skiprows": header_row - 1,
        "dtype": str,
    }

    if usecols:
        pandas_kwargs["usecols"] = usecols

    EXCEL_MAX_ROW = 1048576

    # Apply row limits when specified.
    if data_end_row is not None:
        data_end_row = int(data_end_row)

        if data_end_row >= EXCEL_MAX_ROW:
            data_end_row = None

    if data_end_row is not None:
        nrows = data_end_row - data_start_row + 1
        gap = data_start_row - header_row - 1

        if gap > 0:
            pandas_kwargs["skiprows"] = (
                list(range(header_row - 1))
                + list(range(header_row, header_row + gap))
            )

        pandas_kwargs["nrows"] = nrows

    # Read the Excel file from local or remote storage.
    if path.startswith(("abfss://", "dbfs:")):
        dbutils = DBUtils(spark)

        tmp = tempfile.NamedTemporaryFile(
            delete=False,
            suffix=".xlsx"
        )
        local_path = tmp.name
        tmp.close()

        try:
            dbutils.fs.cp(path, f"file://{local_path}")
            df = pd.read_excel(local_path, **pandas_kwargs)
        finally:
            if os.path.exists(local_path):
                os.remove(local_path)
    else:
        df = pd.read_excel(path, **pandas_kwargs)

    print(
        f"[excel_connector] Sheet={sheet!r} | "
        f"Raw: {len(df)} rows × {len(df.columns)} cols"
    )

    # Remove empty rows and columns.
    df = _strip_empty(df)

    print(
        f"[excel_connector] Final: "
        f"{len(df)} rows × {len(df.columns)} cols"
    )

    return spark.createDataFrame(df)