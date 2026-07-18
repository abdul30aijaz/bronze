"""Archive utilities for MDIF pipeline."""

def build_archive_path(source_path: str, table_name: str, filename: str) -> str:
    """Build destination archive path for a given file.
    Args:
        source_path: Base source directory path.
        table_name: Name of the table being processed.
        filename: Name of the file to be archived.

    Returns:
        Full archive path where the file should be moved.
    """
    return f"{source_path.rstrip('/')}/ARCHIVE/{table_name.upper()}/{filename}"


def move_to_archive(dbutils, source_path: str, table_name: str, log_fn=None) -> list:
    """Move all files from source directory to archive location.
    Args:
        dbutils: Databricks utilities object for filesystem operations.
        source_path: Base source directory containing table files.
        table_name: Name of the table being processed.
        log_fn: Optional logging function for tracking file moves.

    Returns:
        List of archive paths where files were moved.
    """
    full_path = f"{source_path.rstrip('/')}/{table_name.upper()}"
    files = [f for f in dbutils.fs.ls(full_path) if not f.name.endswith("/")]

    archived = []
    for f in files:
        archive_path = build_archive_path(source_path, table_name, f.name)
        if log_fn:
            log_fn(f"Archiving: {f.path} → {archive_path}")
        dbutils.fs.mv(f.path, archive_path)
        archived.append(archive_path)

    return archived