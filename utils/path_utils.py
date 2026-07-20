
"""Path resolution utilities for standardized data lake structure."""


def _get_schema_name(source_name: str, market: str) -> str:
    """
    Build a normalized schema name from source and market.
    Args:
        source_name: Source system name.
        market: Market/region identifier.
    Returns:
        str: Normalized schema name.
    """
    source = source_name.lower().strip().replace(" ", "_")
    mkt = market.lower().strip().replace(" ", "_")
    return f"{source}_{mkt}"


def resolve_paths(
    region,
    market,
    domain,
    source_name,
    table_name,
    ingestion_ts,
    job_name,
    landing_base,
    raw_base,
):
    """
    Generate standardized Unity Catalog Volume and table paths.
    Args:
        region: Data region.
        market: Market identifier.
        domain: Business domain.
        source_name: Source system name.
        table_name: Table name.
        ingestion_ts: Ingestion timestamp.
        job_name: Job identifier.
        landing_base: Base landing path (Unity Catalog Volume).
        raw_base: Base raw path (Unity Catalog Volume).
    Returns:
        dict: Resolved landing, raw, and curated paths.
    """
    import sys
    sys.path.insert(0, "/Workspace/Shared/bronze")
    from env import CATALOG
    
    base = f"{region}/{market}/{domain}/{source_name}/{table_name}"
    schema_name = _get_schema_name(source_name, market)

    return {
        "landing": f"{landing_base}/{base}/{ingestion_ts}",
        "raw": f"{raw_base}/{base}/{ingestion_ts}",
        "raw_trusted": f"{CATALOG}.{schema_name}.{table_name}",
    }