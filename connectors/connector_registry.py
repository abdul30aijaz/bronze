"""Connector factory for resolving reader functions by file format."""

from connectors.csv_connector import read_csv
from connectors.database_connector import read_database
from connectors.delta_connector import read_delta
from connectors.excel_connector import read_excel
from connectors.json_connector import read_json
from connectors.parquet_connector import read_parquet

# Map supported file formats to their corresponding reader functions.
_CONNECTOR_MAP = {
    "excel":    read_excel,
    "csv":      read_csv,
    "parquet":  read_parquet,
    "json":     read_json,
    "delta":    read_delta,
    "database": read_database,
}


def get_connector(file_format):
    """
    Retrieve the reader function for the specified file format.
    Args:
        file_format: Input file format.
    Returns:
        function: Reader function for the specified file format.
    """
    # Normalize the input format for consistent lookup.
    key = file_format.lower().strip()

    connector = _CONNECTOR_MAP.get(key)

    # Raise an error if the file format is unsupported.
    if connector is None:
        raise ValueError(
            f"No connector for '{file_format}'. "
            f"Supported: {sorted(_CONNECTOR_MAP.keys())}"
        )

    return connector