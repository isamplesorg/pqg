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

    # Stage 2: Create entity tables
    if verbose:
        print("    Stage 2: Creating entity tables...")

    # 2a. MaterialSampleRecord nodes
    con.execute("""
        CREATE TEMP TABLE samples AS
        SELECT
            src_row_id as row_id,
            sample_identifier as pid,
            'MaterialSampleRecord' as otype,
            label,
            description,
            NULL::VARCHAR[] as altids,
            NULL::INTEGER as s,
            NULL::VARCHAR as p,
            NULL::INTEGER[] as o,
            source_collection as n,
            NULL::JSON as properties,
            ST_POINT(sample_location_longitude, sample_location_latitude) as geometry
        FROM source
    """)

    sample_max = con.execute("SELECT COALESCE(MAX(row_id), 0) FROM samples").fetchone()[0]

    # 2b. SamplingEvent nodes
    con.execute(f"""
        CREATE TEMP TABLE events AS
        SELECT
            {sample_max} + row_number() OVER () as row_id,
            sample_identifier || '_event' as pid,
            'SamplingEvent' as otype,
            produced_by.label as label,
            produced_by.description as description,
            NULL::VARCHAR[] as altids,
            NULL::INTEGER as s,
            NULL::VARCHAR as p,
            NULL::INTEGER[] as o,
            source_collection as n,
            json_object(
                'result_time', produced_by.result_time,
                'has_feature_of_interest', produced_by.has_feature_of_interest
            ) as properties,
            NULL::GEOMETRY as geometry
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

    # Create deduplicated sites
    con.execute(f"""
        CREATE TEMP TABLE sites AS
        SELECT
            {event_max} + row_number() OVER () as row_id,
            site_pid as pid,
            'SamplingSite' as otype,
            first(label) as label,
            first(description) as description,
            NULL::VARCHAR[] as altids,
            NULL::INTEGER as s,
            NULL::VARCHAR as p,
            NULL::INTEGER[] as o,
            first(n) as n,
            json_object('place_name', first(place_name)) as properties,
            NULL::GEOMETRY as geometry
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

    # 2d. GeospatialCoordLocation nodes
    con.execute(f"""
        CREATE TEMP TABLE locations AS
        SELECT
            {site_max} + row_number() OVER () as row_id,
            sample_identifier || '_location' as pid,
            'GeospatialCoordLocation' as otype,
            NULL as label,
            NULL as description,
            NULL::VARCHAR[] as altids,
            NULL::INTEGER as s,
            NULL::VARCHAR as p,
            NULL::INTEGER[] as o,
            source_collection as n,
            json_object(
                'latitude', produced_by.sampling_site.sample_location.latitude,
                'longitude', produced_by.sampling_site.sample_location.longitude,
                'elevation', produced_by.sampling_site.sample_location.elevation
            ) as properties,
            ST_POINT(
                produced_by.sampling_site.sample_location.longitude,
                produced_by.sampling_site.sample_location.latitude
            ) as geometry
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

    # 3a. has_sample_object_type concepts
    con.execute(f"""
        CREATE TEMP TABLE object_types AS
        SELECT DISTINCT
            {location_max} + row_number() OVER () as row_id,
            unnest.identifier as pid,
            'IdentifiedConcept' as otype,
            unnest.identifier as label,
            NULL as description,
            NULL::VARCHAR[] as altids,
            NULL::INTEGER as s,
            NULL::VARCHAR as p,
            NULL::INTEGER[] as o,
            NULL as n,
            json_object('concept_type', 'sample_object_type') as properties,
            NULL::GEOMETRY as geometry
        FROM source, UNNEST(has_sample_object_type) as unnest
        WHERE has_sample_object_type IS NOT NULL
    """)

    object_type_max = con.execute(f"SELECT COALESCE(MAX(row_id), {location_max}) FROM object_types").fetchone()[0]

    # 3b. has_material_category concepts (excluding already added)
    con.execute(f"""
        CREATE TEMP TABLE materials AS
        SELECT DISTINCT
            {object_type_max} + row_number() OVER () as row_id,
            unnest.identifier as pid,
            'IdentifiedConcept' as otype,
            unnest.identifier as label,
            NULL as description,
            NULL::VARCHAR[] as altids,
            NULL::INTEGER as s,
            NULL::VARCHAR as p,
            NULL::INTEGER[] as o,
            NULL as n,
            json_object('concept_type', 'material_category') as properties,
            NULL::GEOMETRY as geometry
        FROM source, UNNEST(has_material_category) as unnest
        WHERE has_material_category IS NOT NULL
          AND unnest.identifier NOT IN (SELECT pid FROM object_types)
    """)

    material_max = con.execute(f"SELECT COALESCE(MAX(row_id), {object_type_max}) FROM materials").fetchone()[0]

    # 3c. has_context_category concepts (excluding already added)
    con.execute(f"""
        CREATE TEMP TABLE contexts AS
        SELECT DISTINCT
            {material_max} + row_number() OVER () as row_id,
            unnest.identifier as pid,
            'IdentifiedConcept' as otype,
            unnest.identifier as label,
            NULL as description,
            NULL::VARCHAR[] as altids,
            NULL::INTEGER as s,
            NULL::VARCHAR as p,
            NULL::INTEGER[] as o,
            NULL as n,
            json_object('concept_type', 'context_category') as properties,
            NULL::GEOMETRY as geometry
        FROM source, UNNEST(has_context_category) as unnest
        WHERE has_context_category IS NOT NULL
          AND unnest.identifier NOT IN (SELECT pid FROM object_types)
          AND unnest.identifier NOT IN (SELECT pid FROM materials)
    """)

    context_max = con.execute(f"SELECT COALESCE(MAX(row_id), {material_max}) FROM contexts").fetchone()[0]

    # 3d. Keywords
    con.execute(f"""
        CREATE TEMP TABLE keywords AS
        SELECT DISTINCT
            {context_max} + row_number() OVER () as row_id,
            'keyword:' || unnest.keyword as pid,
            'IdentifiedConcept' as otype,
            unnest.keyword as label,
            NULL as description,
            NULL::VARCHAR[] as altids,
            NULL::INTEGER as s,
            NULL::VARCHAR as p,
            NULL::INTEGER[] as o,
            NULL as n,
            json_object('concept_type', 'keyword') as properties,
            NULL::GEOMETRY as geometry
        FROM source, UNNEST(keywords) as unnest
        WHERE keywords IS NOT NULL
    """)

    keyword_max = con.execute(f"SELECT COALESCE(MAX(row_id), {context_max}) FROM keywords").fetchone()[0]

    # Stage 4: Create Agent entities
    if verbose:
        print("    Stage 4: Creating agent entities...")

    # 4a. Registrant agents
    con.execute(f"""
        CREATE TEMP TABLE registrants AS
        SELECT DISTINCT
            {keyword_max} + row_number() OVER () as row_id,
            'agent:' || LOWER(TRIM(registrant.name)) as pid,
            'Agent' as otype,
            registrant.name as label,
            NULL as description,
            NULL::VARCHAR[] as altids,
            NULL::INTEGER as s,
            NULL::VARCHAR as p,
            NULL::INTEGER[] as o,
            NULL as n,
            json_object('role', 'registrant') as properties,
            NULL::GEOMETRY as geometry
        FROM source
        WHERE registrant IS NOT NULL
          AND registrant.name IS NOT NULL
          AND TRIM(registrant.name) != ''
    """)

    registrant_max = con.execute(f"SELECT COALESCE(MAX(row_id), {keyword_max}) FROM registrants").fetchone()[0]

    # 4b. Responsibility agents (from produced_by.responsibility)
    con.execute(f"""
        CREATE TEMP TABLE responsibility_agents AS
        SELECT DISTINCT
            {registrant_max} + row_number() OVER () as row_id,
            'agent:' || LOWER(TRIM(unnest.name)) || ':' || LOWER(TRIM(COALESCE(unnest.role, 'unknown'))) as pid,
            'Agent' as otype,
            unnest.name as label,
            NULL as description,
            NULL::VARCHAR[] as altids,
            NULL::INTEGER as s,
            NULL::VARCHAR as p,
            NULL::INTEGER[] as o,
            NULL as n,
            json_object('role', unnest.role) as properties,
            NULL::GEOMETRY as geometry
        FROM source, UNNEST(produced_by.responsibility) as unnest
        WHERE produced_by IS NOT NULL
          AND produced_by.responsibility IS NOT NULL
          AND unnest.name IS NOT NULL
          AND TRIM(unnest.name) != ''
          AND ('agent:' || LOWER(TRIM(unnest.name)) || ':' || LOWER(TRIM(COALESCE(unnest.role, 'unknown'))))
              NOT IN (SELECT pid FROM registrants)
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


def _build_narrow_edges_staged(con, output_parquet: str, agent_max: int, verbose: bool) -> None:
    """Create edge rows and combine with entities for narrow format output."""

    if verbose:
        print("    Stage 6: Creating edge tables...")

    # Edge: Sample -> produced_by -> Event
    con.execute(f"""
        CREATE TEMP TABLE edge_produced_by AS
        SELECT
            {agent_max} + row_number() OVER () as row_id,
            s.sample_identifier || '_edge_produced_by' as pid,
            '_edge_' as otype,
            NULL as label,
            NULL as description,
            NULL::VARCHAR[] as altids,
            samp.row_id as s,
            'produced_by' as p,
            [evt.row_id]::INTEGER[] as o,
            s.source_collection as n,
            NULL::JSON as properties,
            NULL::GEOMETRY as geometry
        FROM source s
        JOIN pid_lookup samp ON samp.pid = s.sample_identifier
        JOIN pid_lookup evt ON evt.pid = s.sample_identifier || '_event'
        WHERE s.produced_by IS NOT NULL
    """)

    edge_pb_max = con.execute(f"SELECT COALESCE(MAX(row_id), {agent_max}) FROM edge_produced_by").fetchone()[0]

    # Edge: Event -> sampling_site -> Site
    con.execute(f"""
        CREATE TEMP TABLE edge_sampling_site AS
        SELECT
            {edge_pb_max} + row_number() OVER () as row_id,
            s.sample_identifier || '_edge_sampling_site' as pid,
            '_edge_' as otype,
            NULL as label,
            NULL as description,
            NULL::VARCHAR[] as altids,
            evt.row_id as s,
            'sampling_site' as p,
            [site.row_id]::INTEGER[] as o,
            s.source_collection as n,
            NULL::JSON as properties,
            NULL::GEOMETRY as geometry
        FROM source s
        JOIN pid_lookup evt ON evt.pid = s.sample_identifier || '_event'
        JOIN sample_to_site sts ON sts.sample_identifier = s.sample_identifier
        JOIN pid_lookup site ON site.pid = sts.site_pid
        WHERE s.produced_by IS NOT NULL
          AND s.produced_by.sampling_site IS NOT NULL
    """)

    edge_ss_max = con.execute(f"SELECT COALESCE(MAX(row_id), {edge_pb_max}) FROM edge_sampling_site").fetchone()[0]

    # Edge: Site -> sample_location -> Location (one per unique site)
    con.execute(f"""
        CREATE TEMP TABLE edge_sample_location AS
        SELECT
            {edge_ss_max} + row_number() OVER () as row_id,
            site.pid || '_edge_sample_location' as pid,
            '_edge_' as otype,
            NULL as label,
            NULL as description,
            NULL::VARCHAR[] as altids,
            site.row_id as s,
            'sample_location' as p,
            [loc.row_id]::INTEGER[] as o,
            site.n as n,
            NULL::JSON as properties,
            NULL::GEOMETRY as geometry
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

    # Edge: Sample -> has_sample_object_type -> Concept
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
            {edge_sl_max} + row_number() OVER () as row_id,
            e.sample_identifier || '_edge_object_type_' || row_number() OVER (PARTITION BY e.sample_identifier) as pid,
            '_edge_' as otype,
            NULL as label,
            NULL as description,
            NULL::VARCHAR[] as altids,
            samp.row_id as s,
            'has_sample_object_type' as p,
            [concept.row_id]::INTEGER[] as o,
            e.source_collection as n,
            NULL::JSON as properties,
            NULL::GEOMETRY as geometry
        FROM expanded e
        JOIN pid_lookup samp ON samp.pid = e.sample_identifier
        JOIN pid_lookup concept ON concept.pid = e.concept_id
    """)

    edge_ot_max = con.execute(f"SELECT COALESCE(MAX(row_id), {edge_sl_max}) FROM edge_object_type").fetchone()[0]

    # Edge: Sample -> has_material_category -> Concept
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
            {edge_ot_max} + row_number() OVER () as row_id,
            e.sample_identifier || '_edge_material_' || row_number() OVER (PARTITION BY e.sample_identifier) as pid,
            '_edge_' as otype,
            NULL as label,
            NULL as description,
            NULL::VARCHAR[] as altids,
            samp.row_id as s,
            'has_material_category' as p,
            [concept.row_id]::INTEGER[] as o,
            e.source_collection as n,
            NULL::JSON as properties,
            NULL::GEOMETRY as geometry
        FROM expanded e
        JOIN pid_lookup samp ON samp.pid = e.sample_identifier
        JOIN pid_lookup concept ON concept.pid = e.concept_id
    """)

    edge_mat_max = con.execute(f"SELECT COALESCE(MAX(row_id), {edge_ot_max}) FROM edge_material").fetchone()[0]

    # Edge: Sample -> has_context_category -> Concept
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
            {edge_mat_max} + row_number() OVER () as row_id,
            e.sample_identifier || '_edge_context_' || row_number() OVER (PARTITION BY e.sample_identifier) as pid,
            '_edge_' as otype,
            NULL as label,
            NULL as description,
            NULL::VARCHAR[] as altids,
            samp.row_id as s,
            'has_context_category' as p,
            [concept.row_id]::INTEGER[] as o,
            e.source_collection as n,
            NULL::JSON as properties,
            NULL::GEOMETRY as geometry
        FROM expanded e
        JOIN pid_lookup samp ON samp.pid = e.sample_identifier
        JOIN pid_lookup concept ON concept.pid = e.concept_id
    """)

    edge_ctx_max = con.execute(f"SELECT COALESCE(MAX(row_id), {edge_mat_max}) FROM edge_context").fetchone()[0]

    # Edge: Sample -> keywords -> Concept
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
            {edge_ctx_max} + row_number() OVER () as row_id,
            e.sample_identifier || '_edge_keyword_' || row_number() OVER (PARTITION BY e.sample_identifier) as pid,
            '_edge_' as otype,
            NULL as label,
            NULL as description,
            NULL::VARCHAR[] as altids,
            samp.row_id as s,
            'keywords' as p,
            [concept.row_id]::INTEGER[] as o,
            e.source_collection as n,
            NULL::JSON as properties,
            NULL::GEOMETRY as geometry
        FROM expanded e
        JOIN pid_lookup samp ON samp.pid = e.sample_identifier
        JOIN pid_lookup concept ON concept.pid = e.concept_id
    """)

    edge_kw_max = con.execute(f"SELECT COALESCE(MAX(row_id), {edge_ctx_max}) FROM edge_keywords").fetchone()[0]

    # Edge: Sample -> registrant -> Agent
    con.execute(f"""
        CREATE TEMP TABLE edge_registrant AS
        SELECT
            {edge_kw_max} + row_number() OVER () as row_id,
            s.sample_identifier || '_edge_registrant' as pid,
            '_edge_' as otype,
            NULL as label,
            NULL as description,
            NULL::VARCHAR[] as altids,
            samp.row_id as s,
            'registrant' as p,
            [agent.row_id]::INTEGER[] as o,
            s.source_collection as n,
            NULL::JSON as properties,
            NULL::GEOMETRY as geometry
        FROM source s
        JOIN pid_lookup samp ON samp.pid = s.sample_identifier
        JOIN pid_lookup agent ON agent.pid = 'agent:' || LOWER(TRIM(s.registrant.name))
        WHERE s.registrant IS NOT NULL
          AND s.registrant.name IS NOT NULL
          AND TRIM(s.registrant.name) != ''
    """)

    edge_reg_max = con.execute(f"SELECT COALESCE(MAX(row_id), {edge_kw_max}) FROM edge_registrant").fetchone()[0]

    # Edge: Event -> responsibility -> Agent
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
            {edge_reg_max} + row_number() OVER () as row_id,
            e.sample_identifier || '_edge_responsibility_' || row_number() OVER (PARTITION BY e.sample_identifier) as pid,
            '_edge_' as otype,
            NULL as label,
            NULL as description,
            NULL::VARCHAR[] as altids,
            evt.row_id as s,
            'responsibility' as p,
            [agent.row_id]::INTEGER[] as o,
            e.source_collection as n,
            json_object('role', e.agent_role) as properties,
            NULL::GEOMETRY as geometry
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
    """Build wide format output with p__* columns instead of edge rows."""

    if verbose:
        print("    Stage 6: Building wide format columns...")

    # Create sample edge aggregations
    con.execute("""
        CREATE TEMP TABLE sample_edges AS
        SELECT
            s.sample_identifier,
            samp.row_id as sample_row_id,
            evt.row_id as p__produced_by,
            (SELECT list(ot_lookup.row_id)
             FROM UNNEST(s.has_sample_object_type) as ot
             JOIN pid_lookup ot_lookup ON ot_lookup.pid = ot.identifier
            ) as p__has_sample_object_type,
            (SELECT list(mat_lookup.row_id)
             FROM UNNEST(s.has_material_category) as mat
             JOIN pid_lookup mat_lookup ON mat_lookup.pid = mat.identifier
            ) as p__has_material_category,
            (SELECT list(ctx_lookup.row_id)
             FROM UNNEST(s.has_context_category) as ctx
             JOIN pid_lookup ctx_lookup ON ctx_lookup.pid = ctx.identifier
            ) as p__has_context_category,
            (SELECT list(kw_lookup.row_id)
             FROM UNNEST(s.keywords) as kw
             JOIN pid_lookup kw_lookup ON kw_lookup.pid = 'keyword:' || kw.keyword
            ) as p__keywords,
            (SELECT agent.row_id
             FROM pid_lookup agent
             WHERE agent.pid = 'agent:' || LOWER(TRIM(s.registrant.name))
            ) as p__registrant
        FROM source s
        JOIN pid_lookup samp ON samp.pid = s.sample_identifier
        LEFT JOIN pid_lookup evt ON evt.pid = s.sample_identifier || '_event'
    """)

    # Create event edge aggregations
    con.execute("""
        CREATE TEMP TABLE event_edges AS
        SELECT
            s.sample_identifier,
            evt.row_id as event_row_id,
            site.row_id as p__sampling_site,
            (SELECT list(agent.row_id)
             FROM UNNEST(s.produced_by.responsibility) as resp
             JOIN pid_lookup agent ON agent.pid = 'agent:' || LOWER(TRIM(resp.name)) || ':' || LOWER(TRIM(COALESCE(resp.role, 'unknown')))
             WHERE resp.name IS NOT NULL AND TRIM(resp.name) != ''
            ) as p__responsibility
        FROM source s
        JOIN pid_lookup evt ON evt.pid = s.sample_identifier || '_event'
        LEFT JOIN sample_to_site sts ON sts.sample_identifier = s.sample_identifier
        LEFT JOIN pid_lookup site ON site.pid = sts.site_pid
        WHERE s.produced_by IS NOT NULL
    """)

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

    # Export wide format
    if verbose:
        print("    Stage 7: Exporting wide format...")

    con.execute(f"""
        COPY (
            -- Samples with edges
            SELECT
                e.row_id, e.pid, e.otype, e.label, e.description, e.altids, e.n, e.properties, e.geometry,
                se.p__produced_by,
                se.p__has_sample_object_type,
                se.p__has_material_category,
                se.p__has_context_category,
                se.p__keywords,
                se.p__registrant,
                NULL::INTEGER as p__sampling_site,
                NULL::INTEGER as p__sample_location,
                NULL::INTEGER[] as p__responsibility
            FROM samples e
            LEFT JOIN sample_edges se ON se.sample_row_id = e.row_id

            UNION ALL

            -- Events with edges
            SELECT
                e.row_id, e.pid, e.otype, e.label, e.description, e.altids, e.n, e.properties, e.geometry,
                NULL, NULL, NULL, NULL, NULL, NULL,
                ee.p__sampling_site,
                NULL,
                ee.p__responsibility
            FROM events e
            LEFT JOIN event_edges ee ON ee.event_row_id = e.row_id

            UNION ALL

            -- Sites with edges
            SELECT
                e.row_id, e.pid, e.otype, e.label, e.description, e.altids, e.n, e.properties, e.geometry,
                NULL, NULL, NULL, NULL, NULL, NULL,
                NULL,
                se.p__sample_location,
                NULL
            FROM sites e
            LEFT JOIN site_edges se ON se.site_row_id = e.row_id

            UNION ALL

            -- Locations (no outgoing edges)
            SELECT
                row_id, pid, otype, label, description, altids, n, properties, geometry,
                NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
            FROM locations

            UNION ALL

            -- All concept types (no outgoing edges)
            SELECT
                row_id, pid, otype, label, description, altids, n, properties, geometry,
                NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
            FROM object_types
            UNION ALL
            SELECT
                row_id, pid, otype, label, description, altids, n, properties, geometry,
                NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
            FROM materials
            UNION ALL
            SELECT
                row_id, pid, otype, label, description, altids, n, properties, geometry,
                NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
            FROM contexts
            UNION ALL
            SELECT
                row_id, pid, otype, label, description, altids, n, properties, geometry,
                NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
            FROM keywords

            UNION ALL

            -- All agent types (no outgoing edges)
            SELECT
                row_id, pid, otype, label, description, altids, n, properties, geometry,
                NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
            FROM registrants
            UNION ALL
            SELECT
                row_id, pid, otype, label, description, altids, n, properties, geometry,
                NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
            FROM responsibility_agents

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
