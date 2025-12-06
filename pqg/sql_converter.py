"""SQL-based iSamples parquet to PQG converter.

This converter uses pure DuckDB SQL to transform iSamples export parquet files
to PQG format (both narrow and wide). It's 50-100x faster than the Python
row-by-row approach because all processing happens in DuckDB's vectorized engine.

Usage:
    from pqg.sql_converter import convert_isamples_sql

    # Narrow format (with edge rows)
    convert_isamples_sql(
        'isamples_export.parquet',
        'output_narrow.parquet',
        wide=False
    )

    # Wide format (edges as p__* columns)
    convert_isamples_sql(
        'isamples_export.parquet',
        'output_wide.parquet',
        wide=True
    )
"""

import duckdb
import time
from pathlib import Path
from typing import Dict, Any, Optional


def convert_isamples_sql(
    input_parquet: str,
    output_parquet: str,
    wide: bool = False,
    verbose: bool = True,
    dedupe_sites: bool = True,
    site_precision: int = 5,
) -> Dict[str, Any]:
    """Convert iSamples export parquet to PQG format using pure SQL.

    Uses a staged approach with temporary tables to avoid DuckDB's internal
    complexity limits with large CTE chains.

    Args:
        input_parquet: Path to source iSamples parquet file
        output_parquet: Path to write PQG parquet
        wide: If True, output wide format (edges as columns); if False, narrow format
        verbose: Print progress messages
        dedupe_sites: If True, deduplicate SamplingSites by rounded lat/lon + place_name
        site_precision: Decimal places for lat/lon rounding when deduping (default 5 = ~1m)

    Returns:
        Dictionary with conversion statistics
    """
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")

    stats = {"wide": wide, "dedupe_sites": dedupe_sites}
    start_total = time.time()

    if verbose:
        print(f"Converting {input_parquet} to {'wide' if wide else 'narrow'} PQG format...")
        print(f"  Site deduplication: {'enabled (precision={})'.format(site_precision) if dedupe_sites else 'disabled'}")

    # Count source rows
    source_count = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{input_parquet}')
    """).fetchone()[0]
    stats["source_rows"] = source_count
    if verbose:
        print(f"  Source rows: {source_count:,}")

    # Use staged conversion to avoid DuckDB complexity limits
    if verbose:
        print("  Executing staged transformation...")

    start = time.time()
    _convert_staged(con, input_parquet, output_parquet, wide, dedupe_sites, site_precision, verbose)
    stats["transform_time"] = time.time() - start

    if verbose:
        print(f"  Transform time: {stats['transform_time']:.2f}s")

    # Verify output
    output_count = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{output_parquet}')
    """).fetchone()[0]
    stats["output_rows"] = output_count

    # Get output file size
    output_path = Path(output_parquet)
    if output_path.exists():
        stats["output_size_mb"] = output_path.stat().st_size / (1024 * 1024)

    stats["total_time"] = time.time() - start_total

    if verbose:
        print(f"  Output rows: {output_count:,}")
        if "output_size_mb" in stats:
            print(f"  Output size: {stats['output_size_mb']:.1f} MB")
        print(f"  Total time: {stats['total_time']:.2f}s")

    # Validate output against schema
    if verbose:
        print("  Validating output schema...")

    from pqg.schemas import NARROW_SCHEMA, WIDE_SCHEMA, validate_parquet
    expected_schema = WIDE_SCHEMA if wide else NARROW_SCHEMA
    validation_errors = validate_parquet(output_parquet, expected_schema)

    if validation_errors:
        stats["validation_errors"] = validation_errors
        if verbose:
            print(f"  ⚠️  Schema validation warnings ({len(validation_errors)}):")
            for err in validation_errors[:5]:
                print(f"      - {err}")
            if len(validation_errors) > 5:
                print(f"      ... and {len(validation_errors) - 5} more")
    else:
        stats["validation_errors"] = []
        if verbose:
            print("  ✅ Schema validation passed!")

    return stats


def _convert_staged(
    con,
    input_parquet: str,
    output_parquet: str,
    wide: bool,
    dedupe_sites: bool,
    site_precision: int,
    verbose: bool
) -> None:
    """Execute staged conversion using temporary tables.

    This breaks the work into smaller steps to avoid DuckDB's CTE complexity limits.
    """

    # Stage 1: Load source data with row IDs
    if verbose:
        print("    Stage 1: Loading source data...")
    con.execute(f"""
        CREATE TEMP TABLE source AS
        SELECT
            row_number() OVER () as src_row_id,
            *
        FROM read_parquet('{input_parquet}')
    """)

    # Stage 2: Create entity tables with full 40-column schema
    # All entity tables have identical column structure to support UNION ALL
    if verbose:
        print("    Stage 2: Creating entity tables...")

    # 2a. MaterialSampleRecord nodes (full 40-column schema)
    con.execute("""
        CREATE TEMP TABLE samples AS
        SELECT
            -- Core identification
            src_row_id as row_id,
            sample_identifier as pid,
            NULL::INTEGER as tcreated,
            NULL::INTEGER as tmodified,
            'MaterialSampleRecord' as otype,
            -- Edge columns (NULL for entities)
            NULL::INTEGER as s,
            NULL::VARCHAR as p,
            NULL::INTEGER[] as o,
            -- Graph metadata
            source_collection as n,
            NULL::VARCHAR[] as altids,
            ST_POINT(sample_location_longitude, sample_location_latitude) as geometry,
            -- Entity columns (populate relevant ones)
            NULL::VARCHAR[] as authorized_by,
            NULL::VARCHAR as has_feature_of_interest,
            NULL::VARCHAR as affiliation,
            CAST(sampling_purpose AS VARCHAR) as sampling_purpose,
            NULL::VARCHAR[] as complies_with,
            NULL::VARCHAR as project,
            NULL::VARCHAR[] as alternate_identifiers,
            NULL::VARCHAR as relationship,
            NULL::VARCHAR as elevation,
            sample_identifier as sample_identifier,
            NULL::VARCHAR as dc_rights,
            NULL::VARCHAR as result_time,
            NULL::VARCHAR as contact_information,
            sample_location_latitude as latitude,
            NULL::VARCHAR as target,
            NULL::VARCHAR as role,
            NULL::VARCHAR as scheme_uri,
            NULL::VARCHAR[] as is_part_of,
            NULL::VARCHAR as scheme_name,
            NULL::VARCHAR as name,
            sample_location_longitude as longitude,
            NULL::BOOLEAN as obfuscated,
            NULL::VARCHAR as curation_location,
            NULL::VARCHAR as last_modified_time,  -- Not always present in source
            NULL::VARCHAR[] as access_constraints,
            NULL::VARCHAR[] as place_name,
            description,
            label,
            NULL::VARCHAR as thumbnail_url
        FROM source
    """)

    sample_max = con.execute("SELECT COALESCE(MAX(row_id), 0) FROM samples").fetchone()[0]

    # 2b. SamplingEvent nodes (full 40-column schema)
    # Note: Only use fields that exist in the actual export data
    con.execute(f"""
        CREATE TEMP TABLE events AS
        SELECT
            -- Core identification
            {sample_max} + row_number() OVER () as row_id,
            sample_identifier || '_event' as pid,
            NULL::INTEGER as tcreated,
            NULL::INTEGER as tmodified,
            'SamplingEvent' as otype,
            -- Edge columns (NULL for entities)
            NULL::INTEGER as s,
            NULL::VARCHAR as p,
            NULL::INTEGER[] as o,
            -- Graph metadata
            source_collection as n,
            NULL::VARCHAR[] as altids,
            NULL::GEOMETRY as geometry,
            -- Entity columns (populate from available fields)
            NULL::VARCHAR[] as authorized_by,
            produced_by.has_feature_of_interest as has_feature_of_interest,
            NULL::VARCHAR as affiliation,
            NULL::VARCHAR as sampling_purpose,
            NULL::VARCHAR[] as complies_with,
            NULL::VARCHAR as project,
            NULL::VARCHAR[] as alternate_identifiers,
            NULL::VARCHAR as relationship,
            NULL::VARCHAR as elevation,
            NULL::VARCHAR as sample_identifier,
            NULL::VARCHAR as dc_rights,
            produced_by.result_time as result_time,
            NULL::VARCHAR as contact_information,
            NULL::DOUBLE as latitude,
            NULL::VARCHAR as target,
            NULL::VARCHAR as role,
            NULL::VARCHAR as scheme_uri,
            NULL::VARCHAR[] as is_part_of,
            NULL::VARCHAR as scheme_name,
            NULL::VARCHAR as name,
            NULL::DOUBLE as longitude,
            NULL::BOOLEAN as obfuscated,
            NULL::VARCHAR as curation_location,
            NULL::VARCHAR as last_modified_time,
            NULL::VARCHAR[] as access_constraints,
            NULL::VARCHAR[] as place_name,
            produced_by.description as description,
            produced_by.label as label,
            NULL::VARCHAR as thumbnail_url
        FROM source
        WHERE produced_by IS NOT NULL
    """)

    event_max = con.execute(f"SELECT COALESCE(MAX(row_id), {sample_max}) FROM events").fetchone()[0]

    # 2c. SamplingSite nodes (with optional deduplication)
    if dedupe_sites:
        site_key_expr = f"""
            'site:' ||
            COALESCE(CAST(ROUND(produced_by.sampling_site.sample_location.latitude, {site_precision}) AS VARCHAR), 'NULL') || '_' ||
            COALESCE(CAST(ROUND(produced_by.sampling_site.sample_location.longitude, {site_precision}) AS VARCHAR), 'NULL') || '_' ||
            COALESCE(LOWER(TRIM(produced_by.sampling_site.label)), '') || '_' ||
            COALESCE(LOWER(TRIM(CAST(produced_by.sampling_site.place_name AS VARCHAR))), '')
        """
    else:
        site_key_expr = "sample_identifier || '_site'"

    # Create mapping from sample to site key
    con.execute(f"""
        CREATE TEMP TABLE sample_to_site AS
        SELECT
            sample_identifier,
            {site_key_expr} as site_pid
        FROM source
        WHERE produced_by IS NOT NULL
          AND produced_by.sampling_site IS NOT NULL
    """)

    # Create deduplicated sites (full 40-column schema)
    # Note: Only use fields that exist in the actual export data
    con.execute(f"""
        CREATE TEMP TABLE sites AS
        SELECT
            -- Core identification
            {event_max} + row_number() OVER () as row_id,
            site_pid as pid,
            NULL::INTEGER as tcreated,
            NULL::INTEGER as tmodified,
            'SamplingSite' as otype,
            -- Edge columns (NULL for entities)
            NULL::INTEGER as s,
            NULL::VARCHAR as p,
            NULL::INTEGER[] as o,
            -- Graph metadata
            first(n) as n,
            NULL::VARCHAR[] as altids,
            NULL::GEOMETRY as geometry,
            -- Entity columns
            NULL::VARCHAR[] as authorized_by,
            NULL::VARCHAR as has_feature_of_interest,
            NULL::VARCHAR as affiliation,
            NULL::VARCHAR as sampling_purpose,
            NULL::VARCHAR[] as complies_with,
            NULL::VARCHAR as project,
            NULL::VARCHAR[] as alternate_identifiers,
            NULL::VARCHAR as relationship,
            NULL::VARCHAR as elevation,
            NULL::VARCHAR as sample_identifier,
            NULL::VARCHAR as dc_rights,
            NULL::VARCHAR as result_time,
            NULL::VARCHAR as contact_information,
            NULL::DOUBLE as latitude,
            NULL::VARCHAR as target,
            NULL::VARCHAR as role,
            NULL::VARCHAR as scheme_uri,
            NULL::VARCHAR[] as is_part_of,
            NULL::VARCHAR as scheme_name,
            NULL::VARCHAR as name,
            NULL::DOUBLE as longitude,
            NULL::BOOLEAN as obfuscated,
            NULL::VARCHAR as curation_location,
            NULL::VARCHAR as last_modified_time,
            NULL::VARCHAR[] as access_constraints,
            first(place_name) as place_name,
            first(description) as description,
            first(label) as label,
            NULL::VARCHAR as thumbnail_url
        FROM (
            SELECT
                sts.site_pid,
                s.produced_by.sampling_site.label as label,
                s.produced_by.sampling_site.description as description,
                s.source_collection as n,
                s.produced_by.sampling_site.place_name as place_name
            FROM sample_to_site sts
            JOIN source s ON s.sample_identifier = sts.sample_identifier
        ) sub
        GROUP BY site_pid
    """)

    site_max = con.execute(f"SELECT COALESCE(MAX(row_id), {event_max}) FROM sites").fetchone()[0]

    # 2d. GeospatialCoordLocation nodes (full 40-column schema)
    con.execute(f"""
        CREATE TEMP TABLE locations AS
        SELECT
            -- Core identification
            {site_max} + row_number() OVER () as row_id,
            sample_identifier || '_location' as pid,
            NULL::INTEGER as tcreated,
            NULL::INTEGER as tmodified,
            'GeospatialCoordLocation' as otype,
            -- Edge columns (NULL for entities)
            NULL::INTEGER as s,
            NULL::VARCHAR as p,
            NULL::INTEGER[] as o,
            -- Graph metadata
            source_collection as n,
            NULL::VARCHAR[] as altids,
            ST_POINT(
                produced_by.sampling_site.sample_location.longitude,
                produced_by.sampling_site.sample_location.latitude
            ) as geometry,
            -- Entity columns
            NULL::VARCHAR[] as authorized_by,
            NULL::VARCHAR as has_feature_of_interest,
            NULL::VARCHAR as affiliation,
            NULL::VARCHAR as sampling_purpose,
            NULL::VARCHAR[] as complies_with,
            NULL::VARCHAR as project,
            NULL::VARCHAR[] as alternate_identifiers,
            NULL::VARCHAR as relationship,
            CAST(produced_by.sampling_site.sample_location.elevation AS VARCHAR) as elevation,
            NULL::VARCHAR as sample_identifier,
            NULL::VARCHAR as dc_rights,
            NULL::VARCHAR as result_time,
            NULL::VARCHAR as contact_information,
            produced_by.sampling_site.sample_location.latitude as latitude,
            NULL::VARCHAR as target,
            NULL::VARCHAR as role,
            NULL::VARCHAR as scheme_uri,
            NULL::VARCHAR[] as is_part_of,
            NULL::VARCHAR as scheme_name,
            NULL::VARCHAR as name,
            produced_by.sampling_site.sample_location.longitude as longitude,
            NULL::BOOLEAN as obfuscated,
            NULL::VARCHAR as curation_location,
            NULL::VARCHAR as last_modified_time,
            NULL::VARCHAR[] as access_constraints,
            NULL::VARCHAR[] as place_name,
            NULL::VARCHAR as description,
            NULL::VARCHAR as label,
            NULL::VARCHAR as thumbnail_url
        FROM source
        WHERE produced_by IS NOT NULL
          AND produced_by.sampling_site IS NOT NULL
          AND produced_by.sampling_site.sample_location IS NOT NULL
          AND produced_by.sampling_site.sample_location.latitude IS NOT NULL
    """)

    location_max = con.execute(f"SELECT COALESCE(MAX(row_id), {site_max}) FROM locations").fetchone()[0]

    # Stage 3: Create IdentifiedConcept entities
    if verbose:
        print("    Stage 3: Creating concept entities...")

    # 3a. has_sample_object_type concepts (full 40-column schema)
    con.execute(f"""
        CREATE TEMP TABLE object_types AS
        WITH expanded AS (
            SELECT unnest.identifier AS pid
            FROM source
            CROSS JOIN UNNEST(has_sample_object_type) AS unnest
            WHERE has_sample_object_type IS NOT NULL
        ),
        dedup AS (SELECT DISTINCT pid FROM expanded)
        SELECT
            -- Core identification
            {location_max} + row_number() OVER () as row_id,
            pid,
            NULL::INTEGER as tcreated,
            NULL::INTEGER as tmodified,
            'IdentifiedConcept' as otype,
            -- Edge columns (NULL for entities)
            NULL::INTEGER as s,
            NULL::VARCHAR as p,
            NULL::INTEGER[] as o,
            -- Graph metadata
            NULL::VARCHAR as n,
            NULL::VARCHAR[] as altids,
            NULL::GEOMETRY as geometry,
            -- Entity columns
            NULL::VARCHAR[] as authorized_by,
            NULL::VARCHAR as has_feature_of_interest,
            NULL::VARCHAR as affiliation,
            NULL::VARCHAR as sampling_purpose,
            NULL::VARCHAR[] as complies_with,
            NULL::VARCHAR as project,
            NULL::VARCHAR[] as alternate_identifiers,
            NULL::VARCHAR as relationship,
            NULL::VARCHAR as elevation,
            NULL::VARCHAR as sample_identifier,
            NULL::VARCHAR as dc_rights,
            NULL::VARCHAR as result_time,
            NULL::VARCHAR as contact_information,
            NULL::DOUBLE as latitude,
            NULL::VARCHAR as target,
            NULL::VARCHAR as role,
            NULL::VARCHAR as scheme_uri,
            NULL::VARCHAR[] as is_part_of,
            NULL::VARCHAR as scheme_name,
            NULL::VARCHAR as name,
            NULL::DOUBLE as longitude,
            NULL::BOOLEAN as obfuscated,
            NULL::VARCHAR as curation_location,
            NULL::VARCHAR as last_modified_time,
            NULL::VARCHAR[] as access_constraints,
            NULL::VARCHAR[] as place_name,
            NULL::VARCHAR as description,
            pid as label,
            NULL::VARCHAR as thumbnail_url
        FROM dedup
    """)

    object_type_max = con.execute(f"SELECT COALESCE(MAX(row_id), {location_max}) FROM object_types").fetchone()[0]

    # 3b. has_material_category concepts (full 40-column schema)
    con.execute(f"""
        CREATE TEMP TABLE materials AS
        WITH expanded AS (
            SELECT unnest.identifier AS pid
            FROM source
            CROSS JOIN UNNEST(has_material_category) AS unnest
            WHERE has_material_category IS NOT NULL
        ),
        dedup AS (SELECT DISTINCT pid FROM expanded)
        SELECT
            -- Core identification
            {object_type_max} + row_number() OVER () as row_id,
            d.pid,
            NULL::INTEGER as tcreated,
            NULL::INTEGER as tmodified,
            'IdentifiedConcept' as otype,
            -- Edge columns (NULL for entities)
            NULL::INTEGER as s,
            NULL::VARCHAR as p,
            NULL::INTEGER[] as o,
            -- Graph metadata
            NULL::VARCHAR as n,
            NULL::VARCHAR[] as altids,
            NULL::GEOMETRY as geometry,
            -- Entity columns
            NULL::VARCHAR[] as authorized_by,
            NULL::VARCHAR as has_feature_of_interest,
            NULL::VARCHAR as affiliation,
            NULL::VARCHAR as sampling_purpose,
            NULL::VARCHAR[] as complies_with,
            NULL::VARCHAR as project,
            NULL::VARCHAR[] as alternate_identifiers,
            NULL::VARCHAR as relationship,
            NULL::VARCHAR as elevation,
            NULL::VARCHAR as sample_identifier,
            NULL::VARCHAR as dc_rights,
            NULL::VARCHAR as result_time,
            NULL::VARCHAR as contact_information,
            NULL::DOUBLE as latitude,
            NULL::VARCHAR as target,
            NULL::VARCHAR as role,
            NULL::VARCHAR as scheme_uri,
            NULL::VARCHAR[] as is_part_of,
            NULL::VARCHAR as scheme_name,
            NULL::VARCHAR as name,
            NULL::DOUBLE as longitude,
            NULL::BOOLEAN as obfuscated,
            NULL::VARCHAR as curation_location,
            NULL::VARCHAR as last_modified_time,
            NULL::VARCHAR[] as access_constraints,
            NULL::VARCHAR[] as place_name,
            NULL::VARCHAR as description,
            d.pid as label,
            NULL::VARCHAR as thumbnail_url
        FROM dedup d
        LEFT JOIN object_types ot ON ot.pid = d.pid
        WHERE ot.pid IS NULL
    """)

    material_max = con.execute(f"SELECT COALESCE(MAX(row_id), {object_type_max}) FROM materials").fetchone()[0]

    # 3c. has_context_category concepts (full 40-column schema)
    con.execute(f"""
        CREATE TEMP TABLE contexts AS
        WITH expanded AS (
            SELECT unnest.identifier AS pid
            FROM source
            CROSS JOIN UNNEST(has_context_category) AS unnest
            WHERE has_context_category IS NOT NULL
        ),
        dedup AS (SELECT DISTINCT pid FROM expanded)
        SELECT
            -- Core identification
            {material_max} + row_number() OVER () as row_id,
            d.pid,
            NULL::INTEGER as tcreated,
            NULL::INTEGER as tmodified,
            'IdentifiedConcept' as otype,
            -- Edge columns (NULL for entities)
            NULL::INTEGER as s,
            NULL::VARCHAR as p,
            NULL::INTEGER[] as o,
            -- Graph metadata
            NULL::VARCHAR as n,
            NULL::VARCHAR[] as altids,
            NULL::GEOMETRY as geometry,
            -- Entity columns
            NULL::VARCHAR[] as authorized_by,
            NULL::VARCHAR as has_feature_of_interest,
            NULL::VARCHAR as affiliation,
            NULL::VARCHAR as sampling_purpose,
            NULL::VARCHAR[] as complies_with,
            NULL::VARCHAR as project,
            NULL::VARCHAR[] as alternate_identifiers,
            NULL::VARCHAR as relationship,
            NULL::VARCHAR as elevation,
            NULL::VARCHAR as sample_identifier,
            NULL::VARCHAR as dc_rights,
            NULL::VARCHAR as result_time,
            NULL::VARCHAR as contact_information,
            NULL::DOUBLE as latitude,
            NULL::VARCHAR as target,
            NULL::VARCHAR as role,
            NULL::VARCHAR as scheme_uri,
            NULL::VARCHAR[] as is_part_of,
            NULL::VARCHAR as scheme_name,
            NULL::VARCHAR as name,
            NULL::DOUBLE as longitude,
            NULL::BOOLEAN as obfuscated,
            NULL::VARCHAR as curation_location,
            NULL::VARCHAR as last_modified_time,
            NULL::VARCHAR[] as access_constraints,
            NULL::VARCHAR[] as place_name,
            NULL::VARCHAR as description,
            d.pid as label,
            NULL::VARCHAR as thumbnail_url
        FROM dedup d
        LEFT JOIN object_types ot ON ot.pid = d.pid
        LEFT JOIN materials mat ON mat.pid = d.pid
        WHERE ot.pid IS NULL AND mat.pid IS NULL
    """)

    context_max = con.execute(f"SELECT COALESCE(MAX(row_id), {material_max}) FROM contexts").fetchone()[0]

    # 3d. Keywords (full 40-column schema)
    con.execute(f"""
        CREATE TEMP TABLE keywords AS
        WITH expanded AS (
            SELECT 'keyword:' || unnest.keyword AS pid, unnest.keyword AS kw_label
            FROM source
            CROSS JOIN UNNEST(keywords) AS unnest
            WHERE keywords IS NOT NULL
        ),
        dedup AS (SELECT DISTINCT pid, kw_label FROM expanded)
        SELECT
            -- Core identification
            {context_max} + row_number() OVER () as row_id,
            pid,
            NULL::INTEGER as tcreated,
            NULL::INTEGER as tmodified,
            'IdentifiedConcept' as otype,
            -- Edge columns (NULL for entities)
            NULL::INTEGER as s,
            NULL::VARCHAR as p,
            NULL::INTEGER[] as o,
            -- Graph metadata
            NULL::VARCHAR as n,
            NULL::VARCHAR[] as altids,
            NULL::GEOMETRY as geometry,
            -- Entity columns
            NULL::VARCHAR[] as authorized_by,
            NULL::VARCHAR as has_feature_of_interest,
            NULL::VARCHAR as affiliation,
            NULL::VARCHAR as sampling_purpose,
            NULL::VARCHAR[] as complies_with,
            NULL::VARCHAR as project,
            NULL::VARCHAR[] as alternate_identifiers,
            NULL::VARCHAR as relationship,
            NULL::VARCHAR as elevation,
            NULL::VARCHAR as sample_identifier,
            NULL::VARCHAR as dc_rights,
            NULL::VARCHAR as result_time,
            NULL::VARCHAR as contact_information,
            NULL::DOUBLE as latitude,
            NULL::VARCHAR as target,
            NULL::VARCHAR as role,
            NULL::VARCHAR as scheme_uri,
            NULL::VARCHAR[] as is_part_of,
            NULL::VARCHAR as scheme_name,
            NULL::VARCHAR as name,
            NULL::DOUBLE as longitude,
            NULL::BOOLEAN as obfuscated,
            NULL::VARCHAR as curation_location,
            NULL::VARCHAR as last_modified_time,
            NULL::VARCHAR[] as access_constraints,
            NULL::VARCHAR[] as place_name,
            NULL::VARCHAR as description,
            kw_label as label,
            NULL::VARCHAR as thumbnail_url
        FROM dedup
    """)

    keyword_max = con.execute(f"SELECT COALESCE(MAX(row_id), {context_max}) FROM keywords").fetchone()[0]

    # Stage 4: Create Agent entities
    if verbose:
        print("    Stage 4: Creating agent entities...")

    # 4a. Registrant agents (full 40-column schema)
    con.execute(f"""
        CREATE TEMP TABLE registrants AS
        WITH expanded AS (
            SELECT
                'agent:' || LOWER(TRIM(registrant.name)) AS pid,
                registrant.name AS agent_name
            FROM source
            WHERE registrant IS NOT NULL
              AND registrant.name IS NOT NULL
              AND TRIM(registrant.name) != ''
        ),
        dedup AS (SELECT DISTINCT pid, agent_name FROM expanded)
        SELECT
            -- Core identification
            {keyword_max} + row_number() OVER () as row_id,
            pid,
            NULL::INTEGER as tcreated,
            NULL::INTEGER as tmodified,
            'Agent' as otype,
            -- Edge columns (NULL for entities)
            NULL::INTEGER as s,
            NULL::VARCHAR as p,
            NULL::INTEGER[] as o,
            -- Graph metadata
            NULL::VARCHAR as n,
            NULL::VARCHAR[] as altids,
            NULL::GEOMETRY as geometry,
            -- Entity columns
            NULL::VARCHAR[] as authorized_by,
            NULL::VARCHAR as has_feature_of_interest,
            NULL::VARCHAR as affiliation,
            NULL::VARCHAR as sampling_purpose,
            NULL::VARCHAR[] as complies_with,
            NULL::VARCHAR as project,
            NULL::VARCHAR[] as alternate_identifiers,
            NULL::VARCHAR as relationship,
            NULL::VARCHAR as elevation,
            NULL::VARCHAR as sample_identifier,
            NULL::VARCHAR as dc_rights,
            NULL::VARCHAR as result_time,
            NULL::VARCHAR as contact_information,
            NULL::DOUBLE as latitude,
            NULL::VARCHAR as target,
            'registrant' as role,
            NULL::VARCHAR as scheme_uri,
            NULL::VARCHAR[] as is_part_of,
            NULL::VARCHAR as scheme_name,
            agent_name as name,
            NULL::DOUBLE as longitude,
            NULL::BOOLEAN as obfuscated,
            NULL::VARCHAR as curation_location,
            NULL::VARCHAR as last_modified_time,
            NULL::VARCHAR[] as access_constraints,
            NULL::VARCHAR[] as place_name,
            NULL::VARCHAR as description,
            agent_name as label,
            NULL::VARCHAR as thumbnail_url
        FROM dedup
    """)

    registrant_max = con.execute(f"SELECT COALESCE(MAX(row_id), {keyword_max}) FROM registrants").fetchone()[0]

    # 4b. Responsibility agents (full 40-column schema)
    con.execute(f"""
        CREATE TEMP TABLE responsibility_agents AS
        WITH expanded AS (
            SELECT
                'agent:' || LOWER(TRIM(unnest.name)) || ':' || LOWER(TRIM(COALESCE(unnest.role, 'unknown'))) AS pid,
                unnest.name AS agent_name,
                unnest.role AS agent_role
            FROM source
            CROSS JOIN UNNEST(produced_by.responsibility) AS unnest
            WHERE produced_by IS NOT NULL
              AND produced_by.responsibility IS NOT NULL
              AND unnest.name IS NOT NULL
              AND TRIM(unnest.name) != ''
        ),
        dedup AS (SELECT DISTINCT pid, agent_name, agent_role FROM expanded)
        SELECT
            -- Core identification
            {registrant_max} + row_number() OVER () as row_id,
            d.pid,
            NULL::INTEGER as tcreated,
            NULL::INTEGER as tmodified,
            'Agent' as otype,
            -- Edge columns (NULL for entities)
            NULL::INTEGER as s,
            NULL::VARCHAR as p,
            NULL::INTEGER[] as o,
            -- Graph metadata
            NULL::VARCHAR as n,
            NULL::VARCHAR[] as altids,
            NULL::GEOMETRY as geometry,
            -- Entity columns
            NULL::VARCHAR[] as authorized_by,
            NULL::VARCHAR as has_feature_of_interest,
            NULL::VARCHAR as affiliation,
            NULL::VARCHAR as sampling_purpose,
            NULL::VARCHAR[] as complies_with,
            NULL::VARCHAR as project,
            NULL::VARCHAR[] as alternate_identifiers,
            NULL::VARCHAR as relationship,
            NULL::VARCHAR as elevation,
            NULL::VARCHAR as sample_identifier,
            NULL::VARCHAR as dc_rights,
            NULL::VARCHAR as result_time,
            NULL::VARCHAR as contact_information,
            NULL::DOUBLE as latitude,
            NULL::VARCHAR as target,
            d.agent_role as role,
            NULL::VARCHAR as scheme_uri,
            NULL::VARCHAR[] as is_part_of,
            NULL::VARCHAR as scheme_name,
            d.agent_name as name,
            NULL::DOUBLE as longitude,
            NULL::BOOLEAN as obfuscated,
            NULL::VARCHAR as curation_location,
            NULL::VARCHAR as last_modified_time,
            NULL::VARCHAR[] as access_constraints,
            NULL::VARCHAR[] as place_name,
            NULL::VARCHAR as description,
            d.agent_name as label,
            NULL::VARCHAR as thumbnail_url
        FROM dedup d
        LEFT JOIN registrants r ON r.pid = d.pid
        WHERE r.pid IS NULL
    """)

    agent_max = con.execute(f"SELECT COALESCE(MAX(row_id), {registrant_max}) FROM responsibility_agents").fetchone()[0]

    # Stage 5: Combine all entities and build PID lookup
    if verbose:
        print("    Stage 5: Building entity lookup...")

    con.execute("""
        CREATE TEMP TABLE all_entities AS
        SELECT * FROM samples
        UNION ALL SELECT * FROM events
        UNION ALL SELECT * FROM sites
        UNION ALL SELECT * FROM locations
        UNION ALL SELECT * FROM object_types
        UNION ALL SELECT * FROM materials
        UNION ALL SELECT * FROM contexts
        UNION ALL SELECT * FROM keywords
        UNION ALL SELECT * FROM registrants
        UNION ALL SELECT * FROM responsibility_agents
    """)

    con.execute("""
        CREATE TEMP TABLE pid_lookup AS
        SELECT pid, row_id FROM all_entities
    """)
    con.execute("CREATE INDEX idx_pid_lookup ON pid_lookup(pid)")

    if wide:
        # Wide format: entities with p__* columns (no edge rows)
        _build_wide_output_staged(con, output_parquet, verbose)
    else:
        # Narrow format: entities + edge rows
        _build_narrow_edges_staged(con, output_parquet, agent_max, verbose)


def _edge_null_columns_sql() -> str:
    """Generate NULL entity columns for edge rows (40-column schema).

    Derives column definitions from sql_columns.py to ensure consistency
    and prevent column order drift.
    """
    from pqg.schemas.sql_columns import NARROW_COLUMNS, get_null_columns_sql

    # Entity columns are everything after the first 11 (core + edge + graph metadata)
    # Core: row_id, pid, tcreated, tmodified, otype (5)
    # Edge: s, p, o (3)
    # Graph: n, altids, geometry (3)
    ENTITY_COLUMNS = NARROW_COLUMNS[11:]  # columns 12-40 are entity-specific

    return get_null_columns_sql(ENTITY_COLUMNS)


def _build_narrow_edges_staged(con, output_parquet: str, agent_max: int, verbose: bool) -> None:
    """Create edge rows and combine with entities for narrow format output."""

    if verbose:
        print("    Stage 6: Creating edge tables...")

    null_cols = _edge_null_columns_sql()

    # Edge: Sample -> produced_by -> Event (full 40-column schema)
    con.execute(f"""
        CREATE TEMP TABLE edge_produced_by AS
        SELECT
            -- Core identification
            {agent_max} + row_number() OVER () as row_id,
            s.sample_identifier || '_edge_produced_by' as pid,
            NULL::INTEGER as tcreated,
            NULL::INTEGER as tmodified,
            '_edge_' as otype,
            -- Edge columns
            samp.row_id as s,
            'produced_by' as p,
            [evt.row_id]::INTEGER[] as o,
            -- Graph metadata
            s.source_collection as n,
            NULL::VARCHAR[] as altids,
            NULL::GEOMETRY as geometry,
            -- Entity columns (all NULL for edges)
            {null_cols}
        FROM source s
        JOIN pid_lookup samp ON samp.pid = s.sample_identifier
        JOIN pid_lookup evt ON evt.pid = s.sample_identifier || '_event'
        WHERE s.produced_by IS NOT NULL
    """)

    edge_pb_max = con.execute(f"SELECT COALESCE(MAX(row_id), {agent_max}) FROM edge_produced_by").fetchone()[0]

    # Edge: Event -> sampling_site -> Site (full 40-column schema)
    con.execute(f"""
        CREATE TEMP TABLE edge_sampling_site AS
        SELECT
            -- Core identification
            {edge_pb_max} + row_number() OVER () as row_id,
            s.sample_identifier || '_edge_sampling_site' as pid,
            NULL::INTEGER as tcreated,
            NULL::INTEGER as tmodified,
            '_edge_' as otype,
            -- Edge columns
            evt.row_id as s,
            'sampling_site' as p,
            [site.row_id]::INTEGER[] as o,
            -- Graph metadata
            s.source_collection as n,
            NULL::VARCHAR[] as altids,
            NULL::GEOMETRY as geometry,
            -- Entity columns (all NULL for edges)
            {null_cols}
        FROM source s
        JOIN pid_lookup evt ON evt.pid = s.sample_identifier || '_event'
        JOIN sample_to_site sts ON sts.sample_identifier = s.sample_identifier
        JOIN pid_lookup site ON site.pid = sts.site_pid
        WHERE s.produced_by IS NOT NULL
          AND s.produced_by.sampling_site IS NOT NULL
    """)

    edge_ss_max = con.execute(f"SELECT COALESCE(MAX(row_id), {edge_pb_max}) FROM edge_sampling_site").fetchone()[0]

    # Edge: Site -> sample_location -> Location (full 40-column schema)
    con.execute(f"""
        CREATE TEMP TABLE edge_sample_location AS
        SELECT
            -- Core identification
            {edge_ss_max} + row_number() OVER () as row_id,
            site.pid || '_edge_sample_location' as pid,
            NULL::INTEGER as tcreated,
            NULL::INTEGER as tmodified,
            '_edge_' as otype,
            -- Edge columns
            site.row_id as s,
            'sample_location' as p,
            [loc.row_id]::INTEGER[] as o,
            -- Graph metadata
            site.n as n,
            NULL::VARCHAR[] as altids,
            NULL::GEOMETRY as geometry,
            -- Entity columns (all NULL for edges)
            {null_cols}
        FROM sites site
        JOIN sample_to_site sts ON sts.site_pid = site.pid
        JOIN source s ON s.sample_identifier = sts.sample_identifier
        JOIN pid_lookup loc ON loc.pid = s.sample_identifier || '_location'
        WHERE s.produced_by IS NOT NULL
          AND s.produced_by.sampling_site IS NOT NULL
          AND s.produced_by.sampling_site.sample_location IS NOT NULL
          AND s.produced_by.sampling_site.sample_location.latitude IS NOT NULL
        QUALIFY row_number() OVER (PARTITION BY site.pid ORDER BY s.sample_identifier) = 1
    """)

    edge_sl_max = con.execute(f"SELECT COALESCE(MAX(row_id), {edge_ss_max}) FROM edge_sample_location").fetchone()[0]

    # Edge: Sample -> has_sample_object_type -> Concept (full 40-column schema)
    con.execute(f"""
        CREATE TEMP TABLE edge_object_type AS
        WITH expanded AS (
            SELECT
                s.sample_identifier,
                s.source_collection,
                unnest.identifier as concept_id
            FROM source s
            CROSS JOIN UNNEST(s.has_sample_object_type) as unnest
            WHERE s.has_sample_object_type IS NOT NULL
        )
        SELECT
            -- Core identification
            {edge_sl_max} + row_number() OVER () as row_id,
            e.sample_identifier || '_edge_object_type_' || row_number() OVER (PARTITION BY e.sample_identifier) as pid,
            NULL::INTEGER as tcreated,
            NULL::INTEGER as tmodified,
            '_edge_' as otype,
            -- Edge columns
            samp.row_id as s,
            'has_sample_object_type' as p,
            [concept.row_id]::INTEGER[] as o,
            -- Graph metadata
            e.source_collection as n,
            NULL::VARCHAR[] as altids,
            NULL::GEOMETRY as geometry,
            -- Entity columns (all NULL for edges)
            {null_cols}
        FROM expanded e
        JOIN pid_lookup samp ON samp.pid = e.sample_identifier
        JOIN pid_lookup concept ON concept.pid = e.concept_id
    """)

    edge_ot_max = con.execute(f"SELECT COALESCE(MAX(row_id), {edge_sl_max}) FROM edge_object_type").fetchone()[0]

    # Edge: Sample -> has_material_category -> Concept (full 40-column schema)
    con.execute(f"""
        CREATE TEMP TABLE edge_material AS
        WITH expanded AS (
            SELECT
                s.sample_identifier,
                s.source_collection,
                unnest.identifier as concept_id
            FROM source s
            CROSS JOIN UNNEST(s.has_material_category) as unnest
            WHERE s.has_material_category IS NOT NULL
        )
        SELECT
            -- Core identification
            {edge_ot_max} + row_number() OVER () as row_id,
            e.sample_identifier || '_edge_material_' || row_number() OVER (PARTITION BY e.sample_identifier) as pid,
            NULL::INTEGER as tcreated,
            NULL::INTEGER as tmodified,
            '_edge_' as otype,
            -- Edge columns
            samp.row_id as s,
            'has_material_category' as p,
            [concept.row_id]::INTEGER[] as o,
            -- Graph metadata
            e.source_collection as n,
            NULL::VARCHAR[] as altids,
            NULL::GEOMETRY as geometry,
            -- Entity columns (all NULL for edges)
            {null_cols}
        FROM expanded e
        JOIN pid_lookup samp ON samp.pid = e.sample_identifier
        JOIN pid_lookup concept ON concept.pid = e.concept_id
    """)

    edge_mat_max = con.execute(f"SELECT COALESCE(MAX(row_id), {edge_ot_max}) FROM edge_material").fetchone()[0]

    # Edge: Sample -> has_context_category -> Concept (full 40-column schema)
    con.execute(f"""
        CREATE TEMP TABLE edge_context AS
        WITH expanded AS (
            SELECT
                s.sample_identifier,
                s.source_collection,
                unnest.identifier as concept_id
            FROM source s
            CROSS JOIN UNNEST(s.has_context_category) as unnest
            WHERE s.has_context_category IS NOT NULL
        )
        SELECT
            -- Core identification
            {edge_mat_max} + row_number() OVER () as row_id,
            e.sample_identifier || '_edge_context_' || row_number() OVER (PARTITION BY e.sample_identifier) as pid,
            NULL::INTEGER as tcreated,
            NULL::INTEGER as tmodified,
            '_edge_' as otype,
            -- Edge columns
            samp.row_id as s,
            'has_context_category' as p,
            [concept.row_id]::INTEGER[] as o,
            -- Graph metadata
            e.source_collection as n,
            NULL::VARCHAR[] as altids,
            NULL::GEOMETRY as geometry,
            -- Entity columns (all NULL for edges)
            {null_cols}
        FROM expanded e
        JOIN pid_lookup samp ON samp.pid = e.sample_identifier
        JOIN pid_lookup concept ON concept.pid = e.concept_id
    """)

    edge_ctx_max = con.execute(f"SELECT COALESCE(MAX(row_id), {edge_mat_max}) FROM edge_context").fetchone()[0]

    # Edge: Sample -> keywords -> Concept (full 40-column schema)
    con.execute(f"""
        CREATE TEMP TABLE edge_keywords AS
        WITH expanded AS (
            SELECT
                s.sample_identifier,
                s.source_collection,
                'keyword:' || unnest.keyword as concept_id
            FROM source s
            CROSS JOIN UNNEST(s.keywords) as unnest
            WHERE s.keywords IS NOT NULL
        )
        SELECT
            -- Core identification
            {edge_ctx_max} + row_number() OVER () as row_id,
            e.sample_identifier || '_edge_keyword_' || row_number() OVER (PARTITION BY e.sample_identifier) as pid,
            NULL::INTEGER as tcreated,
            NULL::INTEGER as tmodified,
            '_edge_' as otype,
            -- Edge columns
            samp.row_id as s,
            'keywords' as p,
            [concept.row_id]::INTEGER[] as o,
            -- Graph metadata
            e.source_collection as n,
            NULL::VARCHAR[] as altids,
            NULL::GEOMETRY as geometry,
            -- Entity columns (all NULL for edges)
            {null_cols}
        FROM expanded e
        JOIN pid_lookup samp ON samp.pid = e.sample_identifier
        JOIN pid_lookup concept ON concept.pid = e.concept_id
    """)

    edge_kw_max = con.execute(f"SELECT COALESCE(MAX(row_id), {edge_ctx_max}) FROM edge_keywords").fetchone()[0]

    # Edge: Sample -> registrant -> Agent (full 40-column schema)
    con.execute(f"""
        CREATE TEMP TABLE edge_registrant AS
        SELECT
            -- Core identification
            {edge_kw_max} + row_number() OVER () as row_id,
            s.sample_identifier || '_edge_registrant' as pid,
            NULL::INTEGER as tcreated,
            NULL::INTEGER as tmodified,
            '_edge_' as otype,
            -- Edge columns
            samp.row_id as s,
            'registrant' as p,
            [agent.row_id]::INTEGER[] as o,
            -- Graph metadata
            s.source_collection as n,
            NULL::VARCHAR[] as altids,
            NULL::GEOMETRY as geometry,
            -- Entity columns (all NULL for edges)
            {null_cols}
        FROM source s
        JOIN pid_lookup samp ON samp.pid = s.sample_identifier
        JOIN pid_lookup agent ON agent.pid = 'agent:' || LOWER(TRIM(s.registrant.name))
        WHERE s.registrant IS NOT NULL
          AND s.registrant.name IS NOT NULL
          AND TRIM(s.registrant.name) != ''
    """)

    edge_reg_max = con.execute(f"SELECT COALESCE(MAX(row_id), {edge_kw_max}) FROM edge_registrant").fetchone()[0]

    # Edge: Event -> responsibility -> Agent (full 40-column schema)
    con.execute(f"""
        CREATE TEMP TABLE edge_responsibility AS
        WITH expanded AS (
            SELECT
                s.sample_identifier,
                s.source_collection,
                unnest.name as agent_name,
                unnest.role as agent_role,
                'agent:' || LOWER(TRIM(unnest.name)) || ':' || LOWER(TRIM(COALESCE(unnest.role, 'unknown'))) as agent_pid
            FROM source s
            CROSS JOIN UNNEST(s.produced_by.responsibility) as unnest
            WHERE s.produced_by IS NOT NULL
              AND s.produced_by.responsibility IS NOT NULL
              AND unnest.name IS NOT NULL
              AND TRIM(unnest.name) != ''
        )
        SELECT
            -- Core identification
            {edge_reg_max} + row_number() OVER () as row_id,
            e.sample_identifier || '_edge_responsibility_' || row_number() OVER (PARTITION BY e.sample_identifier) as pid,
            NULL::INTEGER as tcreated,
            NULL::INTEGER as tmodified,
            '_edge_' as otype,
            -- Edge columns
            evt.row_id as s,
            'responsibility' as p,
            [agent.row_id]::INTEGER[] as o,
            -- Graph metadata
            e.source_collection as n,
            NULL::VARCHAR[] as altids,
            NULL::GEOMETRY as geometry,
            -- Entity columns (all NULL for edges)
            {null_cols}
        FROM expanded e
        JOIN pid_lookup evt ON evt.pid = e.sample_identifier || '_event'
        JOIN pid_lookup agent ON agent.pid = e.agent_pid
    """)

    # Stage 7: Combine and export
    if verbose:
        print("    Stage 7: Combining and exporting...")

    con.execute(f"""
        COPY (
            SELECT * FROM all_entities
            UNION ALL SELECT * FROM edge_produced_by
            UNION ALL SELECT * FROM edge_sampling_site
            UNION ALL SELECT * FROM edge_sample_location
            UNION ALL SELECT * FROM edge_object_type
            UNION ALL SELECT * FROM edge_material
            UNION ALL SELECT * FROM edge_context
            UNION ALL SELECT * FROM edge_keywords
            UNION ALL SELECT * FROM edge_registrant
            UNION ALL SELECT * FROM edge_responsibility
            ORDER BY row_id
        ) TO '{output_parquet}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)


def _build_wide_output_staged(con, output_parquet: str, verbose: bool) -> None:
    """Build wide format output with p__* columns instead of edge rows.

    Wide format matches Eric's OpenContext schema:
    - NO s, p, o columns (edge data stored in p__* columns)
    - NO properties JSON column (flattened to direct columns)
    - latitude, longitude as top-level DOUBLE columns
    - place_name as VARCHAR[] column
    - result_time, elevation as direct columns
    - All 10 p__* columns present
    """

    if verbose:
        print("    Stage 6: Building wide format columns...")
        print("      6a: Creating sample_edges table (this may take a while)...")

    # Create sample edge aggregations
    # Note: DuckDB's UNNEST in correlated subqueries needs special handling
    # We use LATERAL JOINs for the array fields
    con.execute("""
        CREATE TEMP TABLE sample_edges AS
        SELECT
            s.sample_identifier,
            samp.row_id as sample_row_id,
            evt.row_id as p__produced_by,
            ot_agg.row_ids as p__has_sample_object_type,
            mat_agg.row_ids as p__has_material_category,
            ctx_agg.row_ids as p__has_context_category,
            kw_agg.row_ids as p__keywords,
            (SELECT agent.row_id
             FROM pid_lookup agent
             WHERE agent.pid = 'agent:' || LOWER(TRIM(s.registrant.name))
             LIMIT 1
            ) as p__registrant
        FROM source s
        JOIN pid_lookup samp ON samp.pid = s.sample_identifier
        LEFT JOIN pid_lookup evt ON evt.pid = s.sample_identifier || '_event'
        LEFT JOIN LATERAL (
            SELECT list(ot_lookup.row_id) as row_ids
            FROM UNNEST(s.has_sample_object_type) as t(ot)
            JOIN pid_lookup ot_lookup ON ot_lookup.pid = ot.identifier
        ) ot_agg ON true
        LEFT JOIN LATERAL (
            SELECT list(mat_lookup.row_id) as row_ids
            FROM UNNEST(s.has_material_category) as t(mat)
            JOIN pid_lookup mat_lookup ON mat_lookup.pid = mat.identifier
        ) mat_agg ON true
        LEFT JOIN LATERAL (
            SELECT list(ctx_lookup.row_id) as row_ids
            FROM UNNEST(s.has_context_category) as t(ctx)
            JOIN pid_lookup ctx_lookup ON ctx_lookup.pid = ctx.identifier
        ) ctx_agg ON true
        LEFT JOIN LATERAL (
            SELECT list(kw_lookup.row_id) as row_ids
            FROM UNNEST(s.keywords) as t(kw)
            JOIN pid_lookup kw_lookup ON kw_lookup.pid = 'keyword:' || kw.keyword
        ) kw_agg ON true
    """)

    if verbose:
        count = con.execute("SELECT COUNT(*) FROM sample_edges").fetchone()[0]
        print(f"      6a: sample_edges created with {count:,} rows")
        print("      6b: Creating event_edges table...")

    # Create event edge aggregations
    # Use LATERAL JOIN for responsibility UNNEST
    con.execute("""
        CREATE TEMP TABLE event_edges AS
        SELECT
            s.sample_identifier,
            evt.row_id as event_row_id,
            site.row_id as p__sampling_site,
            resp_agg.row_ids as p__responsibility
        FROM source s
        JOIN pid_lookup evt ON evt.pid = s.sample_identifier || '_event'
        LEFT JOIN sample_to_site sts ON sts.sample_identifier = s.sample_identifier
        LEFT JOIN pid_lookup site ON site.pid = sts.site_pid
        LEFT JOIN LATERAL (
            SELECT list(agent.row_id) as row_ids
            FROM UNNEST(s.produced_by.responsibility) as t(resp)
            JOIN pid_lookup agent ON agent.pid = 'agent:' || LOWER(TRIM(resp.name)) || ':' || LOWER(TRIM(COALESCE(resp.role, 'unknown')))
            WHERE resp.name IS NOT NULL AND TRIM(resp.name) != ''
        ) resp_agg ON true
        WHERE s.produced_by IS NOT NULL
    """)

    if verbose:
        count = con.execute("SELECT COUNT(*) FROM event_edges").fetchone()[0]
        print(f"      6b: event_edges created with {count:,} rows")
        print("      6c: Creating site_edges table...")

    # Create site edge aggregations
    con.execute("""
        CREATE TEMP TABLE site_edges AS
        SELECT
            site.pid as site_pid,
            site.row_id as site_row_id,
            MIN(loc.row_id) as p__sample_location
        FROM sites site
        JOIN sample_to_site sts ON sts.site_pid = site.pid
        JOIN source s ON s.sample_identifier = sts.sample_identifier
        LEFT JOIN pid_lookup loc ON loc.pid = s.sample_identifier || '_location'
        GROUP BY site.pid, site.row_id
    """)

    if verbose:
        count = con.execute("SELECT COUNT(*) FROM site_edges").fetchone()[0]
        print(f"      6c: site_edges created with {count:,} rows")

    # Export wide format with full 47-column schema (37 entity + 10 p__)
    # Wide = Narrow columns minus s,p,o plus p__* relationship columns
    if verbose:
        print("    Stage 7: Exporting wide format with full 47 columns...")

    # Generate NULL p__ columns SQL for entities without edges
    null_p_cols = """
                NULL::INTEGER[] as p__has_context_category,
                NULL::INTEGER[] as p__has_material_category,
                NULL::INTEGER[] as p__has_sample_object_type,
                NULL::INTEGER[] as p__keywords,
                NULL::INTEGER[] as p__produced_by,
                NULL::INTEGER[] as p__registrant,
                NULL::INTEGER[] as p__responsibility,
                NULL::INTEGER[] as p__sample_location,
                NULL::INTEGER[] as p__sampling_site,
                NULL::INTEGER[] as p__site_location"""

    con.execute(f"""
        COPY (
            -- Samples with edges (MaterialSampleRecord)
            SELECT
                -- Core (no s,p,o for wide)
                samp.row_id, samp.pid, samp.tcreated, samp.tmodified, samp.otype,
                -- Graph metadata
                samp.n, samp.altids, samp.geometry,
                -- All entity columns (29 columns)
                samp.authorized_by, samp.has_feature_of_interest, samp.affiliation,
                samp.sampling_purpose, samp.complies_with, samp.project,
                samp.alternate_identifiers, samp.relationship, samp.elevation,
                samp.sample_identifier, samp.dc_rights, samp.result_time,
                samp.contact_information, samp.latitude, samp.target, samp.role,
                samp.scheme_uri, samp.is_part_of, samp.scheme_name, samp.name,
                samp.longitude, samp.obfuscated, samp.curation_location,
                samp.last_modified_time, samp.access_constraints, samp.place_name,
                samp.description, samp.label, samp.thumbnail_url,
                -- p__ columns (10 columns)
                se.p__has_context_category,
                se.p__has_material_category,
                se.p__has_sample_object_type,
                se.p__keywords,
                [se.p__produced_by]::INTEGER[] as p__produced_by,
                [se.p__registrant]::INTEGER[] as p__registrant,
                NULL::INTEGER[] as p__responsibility,
                NULL::INTEGER[] as p__sample_location,
                NULL::INTEGER[] as p__sampling_site,
                NULL::INTEGER[] as p__site_location
            FROM samples samp
            LEFT JOIN sample_edges se ON se.sample_row_id = samp.row_id

            UNION ALL

            -- Events with edges (SamplingEvent)
            SELECT
                evt.row_id, evt.pid, evt.tcreated, evt.tmodified, evt.otype,
                evt.n, evt.altids, evt.geometry,
                evt.authorized_by, evt.has_feature_of_interest, evt.affiliation,
                evt.sampling_purpose, evt.complies_with, evt.project,
                evt.alternate_identifiers, evt.relationship, evt.elevation,
                evt.sample_identifier, evt.dc_rights, evt.result_time,
                evt.contact_information, evt.latitude, evt.target, evt.role,
                evt.scheme_uri, evt.is_part_of, evt.scheme_name, evt.name,
                evt.longitude, evt.obfuscated, evt.curation_location,
                evt.last_modified_time, evt.access_constraints, evt.place_name,
                evt.description, evt.label, evt.thumbnail_url,
                NULL::INTEGER[] as p__has_context_category,
                NULL::INTEGER[] as p__has_material_category,
                NULL::INTEGER[] as p__has_sample_object_type,
                NULL::INTEGER[] as p__keywords,
                NULL::INTEGER[] as p__produced_by,
                NULL::INTEGER[] as p__registrant,
                ee.p__responsibility,
                NULL::INTEGER[] as p__sample_location,
                [ee.p__sampling_site]::INTEGER[] as p__sampling_site,
                NULL::INTEGER[] as p__site_location
            FROM events evt
            LEFT JOIN event_edges ee ON ee.event_row_id = evt.row_id

            UNION ALL

            -- Sites with edges (SamplingSite)
            SELECT
                site.row_id, site.pid, site.tcreated, site.tmodified, site.otype,
                site.n, site.altids, site.geometry,
                site.authorized_by, site.has_feature_of_interest, site.affiliation,
                site.sampling_purpose, site.complies_with, site.project,
                site.alternate_identifiers, site.relationship, site.elevation,
                site.sample_identifier, site.dc_rights, site.result_time,
                site.contact_information, site.latitude, site.target, site.role,
                site.scheme_uri, site.is_part_of, site.scheme_name, site.name,
                site.longitude, site.obfuscated, site.curation_location,
                site.last_modified_time, site.access_constraints, site.place_name,
                site.description, site.label, site.thumbnail_url,
                NULL::INTEGER[] as p__has_context_category,
                NULL::INTEGER[] as p__has_material_category,
                NULL::INTEGER[] as p__has_sample_object_type,
                NULL::INTEGER[] as p__keywords,
                NULL::INTEGER[] as p__produced_by,
                NULL::INTEGER[] as p__registrant,
                NULL::INTEGER[] as p__responsibility,
                [se.p__sample_location]::INTEGER[] as p__sample_location,
                NULL::INTEGER[] as p__sampling_site,
                NULL::INTEGER[] as p__site_location
            FROM sites site
            LEFT JOIN site_edges se ON se.site_row_id = site.row_id

            UNION ALL

            -- Locations (GeospatialCoordLocation - no outgoing edges)
            SELECT
                loc.row_id, loc.pid, loc.tcreated, loc.tmodified, loc.otype,
                loc.n, loc.altids, loc.geometry,
                loc.authorized_by, loc.has_feature_of_interest, loc.affiliation,
                loc.sampling_purpose, loc.complies_with, loc.project,
                loc.alternate_identifiers, loc.relationship, loc.elevation,
                loc.sample_identifier, loc.dc_rights, loc.result_time,
                loc.contact_information, loc.latitude, loc.target, loc.role,
                loc.scheme_uri, loc.is_part_of, loc.scheme_name, loc.name,
                loc.longitude, loc.obfuscated, loc.curation_location,
                loc.last_modified_time, loc.access_constraints, loc.place_name,
                loc.description, loc.label, loc.thumbnail_url,
                {null_p_cols}
            FROM locations loc

            UNION ALL

            -- IdentifiedConcept types (no outgoing edges)
            SELECT
                c.row_id, c.pid, c.tcreated, c.tmodified, c.otype,
                c.n, c.altids, c.geometry,
                c.authorized_by, c.has_feature_of_interest, c.affiliation,
                c.sampling_purpose, c.complies_with, c.project,
                c.alternate_identifiers, c.relationship, c.elevation,
                c.sample_identifier, c.dc_rights, c.result_time,
                c.contact_information, c.latitude, c.target, c.role,
                c.scheme_uri, c.is_part_of, c.scheme_name, c.name,
                c.longitude, c.obfuscated, c.curation_location,
                c.last_modified_time, c.access_constraints, c.place_name,
                c.description, c.label, c.thumbnail_url,
                {null_p_cols}
            FROM (
                SELECT * FROM object_types
                UNION ALL SELECT * FROM materials
                UNION ALL SELECT * FROM contexts
                UNION ALL SELECT * FROM keywords
            ) c

            UNION ALL

            -- Agent types (no outgoing edges)
            SELECT
                a.row_id, a.pid, a.tcreated, a.tmodified, a.otype,
                a.n, a.altids, a.geometry,
                a.authorized_by, a.has_feature_of_interest, a.affiliation,
                a.sampling_purpose, a.complies_with, a.project,
                a.alternate_identifiers, a.relationship, a.elevation,
                a.sample_identifier, a.dc_rights, a.result_time,
                a.contact_information, a.latitude, a.target, a.role,
                a.scheme_uri, a.is_part_of, a.scheme_name, a.name,
                a.longitude, a.obfuscated, a.curation_location,
                a.last_modified_time, a.access_constraints, a.place_name,
                a.description, a.label, a.thumbnail_url,
                {null_p_cols}
            FROM (
                SELECT * FROM registrants
                UNION ALL SELECT * FROM responsibility_agents
            ) a

            ORDER BY row_id
        ) TO '{output_parquet}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)



if __name__ == "__main__":
    import sys

    if len(sys.argv) < 3:
        print("Usage: python sql_converter.py <input.parquet> <output.parquet> [--wide]")
        print("\nExamples:")
        print("  python sql_converter.py input.parquet output_narrow.parquet")
        print("  python sql_converter.py input.parquet output_wide.parquet --wide")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]
    wide = "--wide" in sys.argv

    print(f"Converting {input_file} -> {output_file} ({'wide' if wide else 'narrow'} format)")
    stats = convert_isamples_sql(input_file, output_file, wide=wide)
    print(f"\nDone! Processed {stats['source_rows']:,} rows in {stats['total_time']:.2f}s")
