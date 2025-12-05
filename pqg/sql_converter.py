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
) -> Dict[str, Any]:
    """Convert iSamples export parquet to PQG format using pure SQL.

    Args:
        input_parquet: Path to source iSamples parquet file
        output_parquet: Path to write PQG parquet
        wide: If True, output wide format (edges as columns); if False, narrow format
        verbose: Print progress messages

    Returns:
        Dictionary with conversion statistics
    """
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")

    stats = {"wide": wide}
    start_total = time.time()

    if verbose:
        print(f"Converting {input_parquet} to {'wide' if wide else 'narrow'} PQG format...")

    # Count source rows
    source_count = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{input_parquet}')
    """).fetchone()[0]
    stats["source_rows"] = source_count
    if verbose:
        print(f"  Source rows: {source_count:,}")

    # The main transformation SQL
    # This creates all entity nodes and edges in one pass

    if wide:
        sql = _build_wide_sql(input_parquet, output_parquet)
    else:
        sql = _build_narrow_sql(input_parquet, output_parquet)

    if verbose:
        print("  Executing transformation...")

    start = time.time()
    con.execute(sql)
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


def _build_narrow_sql(input_parquet: str, output_parquet: str) -> str:
    """Build SQL for narrow PQG format (entities + edge rows)."""

    return f"""
    COPY (
        WITH source AS (
            SELECT
                row_number() OVER () as src_row_id,
                *
            FROM read_parquet('{input_parquet}')
        ),

        -- ============================================
        -- ENTITY NODES
        -- ============================================

        -- 1. MaterialSampleRecord nodes (one per source row)
        samples AS (
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
        ),

        -- Calculate offset for next entity type
        sample_max AS (SELECT COALESCE(MAX(row_id), 0) as max_id FROM samples),

        -- 2. SamplingEvent nodes (from produced_by)
        events AS (
            SELECT
                (SELECT max_id FROM sample_max) + row_number() OVER () as row_id,
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
        ),

        event_max AS (SELECT COALESCE(MAX(row_id), (SELECT max_id FROM sample_max)) as max_id FROM events),

        -- 3. SamplingSite nodes (from produced_by.sampling_site)
        sites AS (
            SELECT
                (SELECT max_id FROM event_max) + row_number() OVER () as row_id,
                sample_identifier || '_site' as pid,
                'SamplingSite' as otype,
                produced_by.sampling_site.label as label,
                produced_by.sampling_site.description as description,
                NULL::VARCHAR[] as altids,
                NULL::INTEGER as s,
                NULL::VARCHAR as p,
                NULL::INTEGER[] as o,
                source_collection as n,
                json_object(
                    'place_name', produced_by.sampling_site.place_name
                ) as properties,
                NULL::GEOMETRY as geometry
            FROM source
            WHERE produced_by IS NOT NULL
              AND produced_by.sampling_site IS NOT NULL
        ),

        site_max AS (SELECT COALESCE(MAX(row_id), (SELECT max_id FROM event_max)) as max_id FROM sites),

        -- 4. GeospatialCoordLocation nodes (from produced_by.sampling_site.sample_location)
        locations AS (
            SELECT
                (SELECT max_id FROM site_max) + row_number() OVER () as row_id,
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
        ),

        location_max AS (SELECT COALESCE(MAX(row_id), (SELECT max_id FROM site_max)) as max_id FROM locations),

        -- 5. IdentifiedConcept nodes for has_sample_object_type
        object_types AS (
            SELECT DISTINCT
                (SELECT max_id FROM location_max) + row_number() OVER () as row_id,
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
        ),

        object_type_max AS (SELECT COALESCE(MAX(row_id), (SELECT max_id FROM location_max)) as max_id FROM object_types),

        -- 6. IdentifiedConcept nodes for has_material_category
        materials AS (
            SELECT DISTINCT
                (SELECT max_id FROM object_type_max) + row_number() OVER () as row_id,
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
        ),

        material_max AS (SELECT COALESCE(MAX(row_id), (SELECT max_id FROM object_type_max)) as max_id FROM materials),

        -- 7. IdentifiedConcept nodes for has_context_category
        contexts AS (
            SELECT DISTINCT
                (SELECT max_id FROM material_max) + row_number() OVER () as row_id,
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
        ),

        context_max AS (SELECT COALESCE(MAX(row_id), (SELECT max_id FROM material_max)) as max_id FROM contexts),

        -- 8. IdentifiedConcept nodes for keywords
        keywords AS (
            SELECT DISTINCT
                (SELECT max_id FROM context_max) + row_number() OVER () as row_id,
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
        ),

        keyword_max AS (SELECT COALESCE(MAX(row_id), (SELECT max_id FROM context_max)) as max_id FROM keywords),

        -- Combine all entities
        all_entities AS (
            SELECT * FROM samples
            UNION ALL SELECT * FROM events
            UNION ALL SELECT * FROM sites
            UNION ALL SELECT * FROM locations
            UNION ALL SELECT * FROM object_types
            UNION ALL SELECT * FROM materials
            UNION ALL SELECT * FROM contexts
            UNION ALL SELECT * FROM keywords
        ),

        -- Build PID to row_id lookup
        pid_lookup AS (
            SELECT pid, row_id FROM all_entities
        ),

        -- ============================================
        -- EDGE ROWS
        -- ============================================

        -- Edge: Sample -> produced_by -> Event
        edge_produced_by AS (
            SELECT
                (SELECT max_id FROM keyword_max) + row_number() OVER () as row_id,
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
        ),

        edge_produced_max AS (SELECT COALESCE(MAX(row_id), (SELECT max_id FROM keyword_max)) as max_id FROM edge_produced_by),

        -- Edge: Event -> sampling_site -> Site
        edge_sampling_site AS (
            SELECT
                (SELECT max_id FROM edge_produced_max) + row_number() OVER () as row_id,
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
            JOIN pid_lookup site ON site.pid = s.sample_identifier || '_site'
            WHERE s.produced_by IS NOT NULL
              AND s.produced_by.sampling_site IS NOT NULL
        ),

        edge_site_max AS (SELECT COALESCE(MAX(row_id), (SELECT max_id FROM edge_produced_max)) as max_id FROM edge_sampling_site),

        -- Edge: Site -> sample_location -> Location
        edge_sample_location AS (
            SELECT
                (SELECT max_id FROM edge_site_max) + row_number() OVER () as row_id,
                s.sample_identifier || '_edge_sample_location' as pid,
                '_edge_' as otype,
                NULL as label,
                NULL as description,
                NULL::VARCHAR[] as altids,
                site.row_id as s,
                'sample_location' as p,
                [loc.row_id]::INTEGER[] as o,
                s.source_collection as n,
                NULL::JSON as properties,
                NULL::GEOMETRY as geometry
            FROM source s
            JOIN pid_lookup site ON site.pid = s.sample_identifier || '_site'
            JOIN pid_lookup loc ON loc.pid = s.sample_identifier || '_location'
            WHERE s.produced_by IS NOT NULL
              AND s.produced_by.sampling_site IS NOT NULL
              AND s.produced_by.sampling_site.sample_location IS NOT NULL
              AND s.produced_by.sampling_site.sample_location.latitude IS NOT NULL
        ),

        edge_location_max AS (SELECT COALESCE(MAX(row_id), (SELECT max_id FROM edge_site_max)) as max_id FROM edge_sample_location),

        -- Edge: Sample -> has_sample_object_type -> Concept (multiple)
        edge_object_type AS (
            SELECT
                (SELECT max_id FROM edge_location_max) + row_number() OVER () as row_id,
                s.sample_identifier || '_edge_object_type_' || row_number() OVER (PARTITION BY s.sample_identifier) as pid,
                '_edge_' as otype,
                NULL as label,
                NULL as description,
                NULL::VARCHAR[] as altids,
                samp.row_id as s,
                'has_sample_object_type' as p,
                [concept.row_id]::INTEGER[] as o,
                s.source_collection as n,
                NULL::JSON as properties,
                NULL::GEOMETRY as geometry
            FROM source s, UNNEST(s.has_sample_object_type) as unnest
            JOIN pid_lookup samp ON samp.pid = s.sample_identifier
            JOIN pid_lookup concept ON concept.pid = unnest.identifier
            WHERE s.has_sample_object_type IS NOT NULL
        ),

        edge_ot_max AS (SELECT COALESCE(MAX(row_id), (SELECT max_id FROM edge_location_max)) as max_id FROM edge_object_type),

        -- Edge: Sample -> has_material_category -> Concept (multiple)
        edge_material AS (
            SELECT
                (SELECT max_id FROM edge_ot_max) + row_number() OVER () as row_id,
                s.sample_identifier || '_edge_material_' || row_number() OVER (PARTITION BY s.sample_identifier) as pid,
                '_edge_' as otype,
                NULL as label,
                NULL as description,
                NULL::VARCHAR[] as altids,
                samp.row_id as s,
                'has_material_category' as p,
                [concept.row_id]::INTEGER[] as o,
                s.source_collection as n,
                NULL::JSON as properties,
                NULL::GEOMETRY as geometry
            FROM source s, UNNEST(s.has_material_category) as unnest
            JOIN pid_lookup samp ON samp.pid = s.sample_identifier
            JOIN pid_lookup concept ON concept.pid = unnest.identifier
            WHERE s.has_material_category IS NOT NULL
        ),

        edge_mat_max AS (SELECT COALESCE(MAX(row_id), (SELECT max_id FROM edge_ot_max)) as max_id FROM edge_material),

        -- Edge: Sample -> has_context_category -> Concept (multiple)
        edge_context AS (
            SELECT
                (SELECT max_id FROM edge_mat_max) + row_number() OVER () as row_id,
                s.sample_identifier || '_edge_context_' || row_number() OVER (PARTITION BY s.sample_identifier) as pid,
                '_edge_' as otype,
                NULL as label,
                NULL as description,
                NULL::VARCHAR[] as altids,
                samp.row_id as s,
                'has_context_category' as p,
                [concept.row_id]::INTEGER[] as o,
                s.source_collection as n,
                NULL::JSON as properties,
                NULL::GEOMETRY as geometry
            FROM source s, UNNEST(s.has_context_category) as unnest
            JOIN pid_lookup samp ON samp.pid = s.sample_identifier
            JOIN pid_lookup concept ON concept.pid = unnest.identifier
            WHERE s.has_context_category IS NOT NULL
        ),

        edge_ctx_max AS (SELECT COALESCE(MAX(row_id), (SELECT max_id FROM edge_mat_max)) as max_id FROM edge_context),

        -- Edge: Sample -> keywords -> Concept (multiple)
        edge_keywords AS (
            SELECT
                (SELECT max_id FROM edge_ctx_max) + row_number() OVER () as row_id,
                s.sample_identifier || '_edge_keyword_' || row_number() OVER (PARTITION BY s.sample_identifier) as pid,
                '_edge_' as otype,
                NULL as label,
                NULL as description,
                NULL::VARCHAR[] as altids,
                samp.row_id as s,
                'keywords' as p,
                [concept.row_id]::INTEGER[] as o,
                s.source_collection as n,
                NULL::JSON as properties,
                NULL::GEOMETRY as geometry
            FROM source s, UNNEST(s.keywords) as unnest
            JOIN pid_lookup samp ON samp.pid = s.sample_identifier
            JOIN pid_lookup concept ON concept.pid = 'keyword:' || unnest.keyword
            WHERE s.keywords IS NOT NULL
        ),

        -- Combine all edges
        all_edges AS (
            SELECT * FROM edge_produced_by
            UNION ALL SELECT * FROM edge_sampling_site
            UNION ALL SELECT * FROM edge_sample_location
            UNION ALL SELECT * FROM edge_object_type
            UNION ALL SELECT * FROM edge_material
            UNION ALL SELECT * FROM edge_context
            UNION ALL SELECT * FROM edge_keywords
        ),

        -- Final output: entities + edges
        final AS (
            SELECT * FROM all_entities
            UNION ALL SELECT * FROM all_edges
        )

        SELECT * FROM final
        ORDER BY row_id
    ) TO '{output_parquet}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """


def _build_wide_sql(input_parquet: str, output_parquet: str) -> str:
    """Build SQL for wide PQG format (edges as p__* columns)."""

    return f"""
    COPY (
        WITH source AS (
            SELECT
                row_number() OVER () as src_row_id,
                *
            FROM read_parquet('{input_parquet}')
        ),

        -- ============================================
        -- ENTITY NODES (same as narrow)
        -- ============================================

        samples AS (
            SELECT
                src_row_id as row_id,
                sample_identifier as pid,
                'MaterialSampleRecord' as otype,
                label,
                description,
                NULL::VARCHAR[] as altids,
                source_collection as n,
                NULL::JSON as properties,
                ST_POINT(sample_location_longitude, sample_location_latitude) as geometry
            FROM source
        ),

        sample_max AS (SELECT COALESCE(MAX(row_id), 0) as max_id FROM samples),

        events AS (
            SELECT
                (SELECT max_id FROM sample_max) + row_number() OVER () as row_id,
                sample_identifier || '_event' as pid,
                'SamplingEvent' as otype,
                produced_by.label as label,
                produced_by.description as description,
                NULL::VARCHAR[] as altids,
                source_collection as n,
                json_object(
                    'result_time', produced_by.result_time,
                    'has_feature_of_interest', produced_by.has_feature_of_interest
                ) as properties,
                NULL::GEOMETRY as geometry
            FROM source
            WHERE produced_by IS NOT NULL
        ),

        event_max AS (SELECT COALESCE(MAX(row_id), (SELECT max_id FROM sample_max)) as max_id FROM events),

        sites AS (
            SELECT
                (SELECT max_id FROM event_max) + row_number() OVER () as row_id,
                sample_identifier || '_site' as pid,
                'SamplingSite' as otype,
                produced_by.sampling_site.label as label,
                produced_by.sampling_site.description as description,
                NULL::VARCHAR[] as altids,
                source_collection as n,
                json_object('place_name', produced_by.sampling_site.place_name) as properties,
                NULL::GEOMETRY as geometry
            FROM source
            WHERE produced_by IS NOT NULL AND produced_by.sampling_site IS NOT NULL
        ),

        site_max AS (SELECT COALESCE(MAX(row_id), (SELECT max_id FROM event_max)) as max_id FROM sites),

        locations AS (
            SELECT
                (SELECT max_id FROM site_max) + row_number() OVER () as row_id,
                sample_identifier || '_location' as pid,
                'GeospatialCoordLocation' as otype,
                NULL as label,
                NULL as description,
                NULL::VARCHAR[] as altids,
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
        ),

        location_max AS (SELECT COALESCE(MAX(row_id), (SELECT max_id FROM site_max)) as max_id FROM locations),

        object_types AS (
            SELECT DISTINCT
                (SELECT max_id FROM location_max) + row_number() OVER () as row_id,
                unnest.identifier as pid,
                'IdentifiedConcept' as otype,
                unnest.identifier as label,
                NULL as description,
                NULL::VARCHAR[] as altids,
                NULL as n,
                json_object('concept_type', 'sample_object_type') as properties,
                NULL::GEOMETRY as geometry
            FROM source, UNNEST(has_sample_object_type) as unnest
            WHERE has_sample_object_type IS NOT NULL
        ),

        object_type_max AS (SELECT COALESCE(MAX(row_id), (SELECT max_id FROM location_max)) as max_id FROM object_types),

        materials AS (
            SELECT DISTINCT
                (SELECT max_id FROM object_type_max) + row_number() OVER () as row_id,
                unnest.identifier as pid,
                'IdentifiedConcept' as otype,
                unnest.identifier as label,
                NULL as description,
                NULL::VARCHAR[] as altids,
                NULL as n,
                json_object('concept_type', 'material_category') as properties,
                NULL::GEOMETRY as geometry
            FROM source, UNNEST(has_material_category) as unnest
            WHERE has_material_category IS NOT NULL
              AND unnest.identifier NOT IN (SELECT pid FROM object_types)
        ),

        material_max AS (SELECT COALESCE(MAX(row_id), (SELECT max_id FROM object_type_max)) as max_id FROM materials),

        contexts AS (
            SELECT DISTINCT
                (SELECT max_id FROM material_max) + row_number() OVER () as row_id,
                unnest.identifier as pid,
                'IdentifiedConcept' as otype,
                unnest.identifier as label,
                NULL as description,
                NULL::VARCHAR[] as altids,
                NULL as n,
                json_object('concept_type', 'context_category') as properties,
                NULL::GEOMETRY as geometry
            FROM source, UNNEST(has_context_category) as unnest
            WHERE has_context_category IS NOT NULL
              AND unnest.identifier NOT IN (SELECT pid FROM object_types)
              AND unnest.identifier NOT IN (SELECT pid FROM materials)
        ),

        context_max AS (SELECT COALESCE(MAX(row_id), (SELECT max_id FROM material_max)) as max_id FROM contexts),

        keywords AS (
            SELECT DISTINCT
                (SELECT max_id FROM context_max) + row_number() OVER () as row_id,
                'keyword:' || unnest.keyword as pid,
                'IdentifiedConcept' as otype,
                unnest.keyword as label,
                NULL as description,
                NULL::VARCHAR[] as altids,
                NULL as n,
                json_object('concept_type', 'keyword') as properties,
                NULL::GEOMETRY as geometry
            FROM source, UNNEST(keywords) as unnest
            WHERE keywords IS NOT NULL
        ),

        -- All entities combined
        all_entities AS (
            SELECT * FROM samples
            UNION ALL SELECT * FROM events
            UNION ALL SELECT * FROM sites
            UNION ALL SELECT * FROM locations
            UNION ALL SELECT * FROM object_types
            UNION ALL SELECT * FROM materials
            UNION ALL SELECT * FROM contexts
            UNION ALL SELECT * FROM keywords
        ),

        -- PID to row_id lookup
        pid_lookup AS (
            SELECT pid, row_id FROM all_entities
        ),

        -- ============================================
        -- BUILD WIDE COLUMNS (p__* instead of edge rows)
        -- ============================================

        -- For samples: compute edge arrays
        sample_edges AS (
            SELECT
                s.sample_identifier,
                samp.row_id as sample_row_id,
                evt.row_id as p__produced_by,
                -- Aggregate multiple object types into array
                (SELECT list(ot_lookup.row_id)
                 FROM UNNEST(s.has_sample_object_type) as ot
                 JOIN pid_lookup ot_lookup ON ot_lookup.pid = ot.identifier
                ) as p__has_sample_object_type,
                -- Aggregate multiple materials into array
                (SELECT list(mat_lookup.row_id)
                 FROM UNNEST(s.has_material_category) as mat
                 JOIN pid_lookup mat_lookup ON mat_lookup.pid = mat.identifier
                ) as p__has_material_category,
                -- Aggregate multiple contexts into array
                (SELECT list(ctx_lookup.row_id)
                 FROM UNNEST(s.has_context_category) as ctx
                 JOIN pid_lookup ctx_lookup ON ctx_lookup.pid = ctx.identifier
                ) as p__has_context_category,
                -- Aggregate keywords into array
                (SELECT list(kw_lookup.row_id)
                 FROM UNNEST(s.keywords) as kw
                 JOIN pid_lookup kw_lookup ON kw_lookup.pid = 'keyword:' || kw.keyword
                ) as p__keywords
            FROM source s
            JOIN pid_lookup samp ON samp.pid = s.sample_identifier
            LEFT JOIN pid_lookup evt ON evt.pid = s.sample_identifier || '_event'
        ),

        -- For events: compute edge arrays
        event_edges AS (
            SELECT
                s.sample_identifier,
                evt.row_id as event_row_id,
                site.row_id as p__sampling_site
            FROM source s
            JOIN pid_lookup evt ON evt.pid = s.sample_identifier || '_event'
            LEFT JOIN pid_lookup site ON site.pid = s.sample_identifier || '_site'
            WHERE s.produced_by IS NOT NULL
        ),

        -- For sites: compute edge arrays
        site_edges AS (
            SELECT
                s.sample_identifier,
                site.row_id as site_row_id,
                loc.row_id as p__sample_location
            FROM source s
            JOIN pid_lookup site ON site.pid = s.sample_identifier || '_site'
            LEFT JOIN pid_lookup loc ON loc.pid = s.sample_identifier || '_location'
            WHERE s.produced_by IS NOT NULL AND s.produced_by.sampling_site IS NOT NULL
        ),

        -- Final wide output: entities with p__* columns
        final AS (
            -- Samples with edges
            SELECT
                e.row_id,
                e.pid,
                e.otype,
                e.label,
                e.description,
                e.altids,
                e.n,
                e.properties,
                e.geometry,
                se.p__produced_by,
                se.p__has_sample_object_type,
                se.p__has_material_category,
                se.p__has_context_category,
                se.p__keywords,
                NULL::INTEGER as p__sampling_site,
                NULL::INTEGER as p__sample_location
            FROM samples e
            LEFT JOIN sample_edges se ON se.sample_row_id = e.row_id

            UNION ALL

            -- Events with edges
            SELECT
                e.row_id,
                e.pid,
                e.otype,
                e.label,
                e.description,
                e.altids,
                e.n,
                e.properties,
                e.geometry,
                NULL as p__produced_by,
                NULL as p__has_sample_object_type,
                NULL as p__has_material_category,
                NULL as p__has_context_category,
                NULL as p__keywords,
                ee.p__sampling_site,
                NULL as p__sample_location
            FROM events e
            LEFT JOIN event_edges ee ON ee.event_row_id = e.row_id

            UNION ALL

            -- Sites with edges
            SELECT
                e.row_id,
                e.pid,
                e.otype,
                e.label,
                e.description,
                e.altids,
                e.n,
                e.properties,
                e.geometry,
                NULL as p__produced_by,
                NULL as p__has_sample_object_type,
                NULL as p__has_material_category,
                NULL as p__has_context_category,
                NULL as p__keywords,
                NULL as p__sampling_site,
                se.p__sample_location
            FROM sites e
            LEFT JOIN site_edges se ON se.site_row_id = e.row_id

            UNION ALL

            -- Locations (no outgoing edges)
            SELECT
                row_id, pid, otype, label, description, altids, n, properties, geometry,
                NULL, NULL, NULL, NULL, NULL, NULL, NULL
            FROM locations

            UNION ALL

            -- Concepts (no outgoing edges)
            SELECT
                row_id, pid, otype, label, description, altids, n, properties, geometry,
                NULL, NULL, NULL, NULL, NULL, NULL, NULL
            FROM object_types

            UNION ALL
            SELECT
                row_id, pid, otype, label, description, altids, n, properties, geometry,
                NULL, NULL, NULL, NULL, NULL, NULL, NULL
            FROM materials

            UNION ALL
            SELECT
                row_id, pid, otype, label, description, altids, n, properties, geometry,
                NULL, NULL, NULL, NULL, NULL, NULL, NULL
            FROM contexts

            UNION ALL
            SELECT
                row_id, pid, otype, label, description, altids, n, properties, geometry,
                NULL, NULL, NULL, NULL, NULL, NULL, NULL
            FROM keywords
        )

        SELECT * FROM final
        ORDER BY row_id
    ) TO '{output_parquet}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """


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
