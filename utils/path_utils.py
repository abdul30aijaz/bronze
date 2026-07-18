
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
    adls_base,
):
    """
    Generate standardized ADLS and Unity Catalog paths.
    Args:
        region: Data region.
        market: Market identifier.
        domain: Business domain.
        source_name: Source system name.
        table_name: Table name.
        ingestion_ts: Ingestion timestamp.
        job_name: Job identifier.
        adls_base: Base ADLS path.
    Returns:
        dict: Resolved landing, raw, and curated paths.
    """
    base = f"{region}/{market}/{domain}/{source_name}/{table_name}"
    schema_name = _get_schema_name(source_name, market)

    return {
        "landing": f"{adls_base}/LANDING/{base}/{ingestion_ts}",
        "raw": f"{adls_base}/RAW/{base}/{ingestion_ts}",
        "raw_trusted": f"cdap_mars_pc_mdif.{schema_name}.{table_name}",
    }