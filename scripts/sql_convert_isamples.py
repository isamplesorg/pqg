#!/usr/bin/env python3
"""SQL-based iSamples parquet to PQG converter.

This converter uses pure DuckDB SQL to transform iSamples export parquet files
to PQG format (both narrow and wide). It's 130x faster than the Python
row-by-row approach because all processing happens in DuckDB's vectorized engine.

Usage:
    # Narrow format (with edge rows)
    python sql_convert_isamples.py input.parquet output_narrow.parquet

    # Wide format (edges as p__* columns)
    python sql_convert_isamples.py input.parquet output_wide.parquet --wide

Performance:
    - 6.68M source rows → 44M narrow rows in ~70 seconds
    - Rate: ~95,000 rows/sec (vs ~700 rows/sec for Python version)
"""

import argparse
import duckdb
import time
from pathlib import Path


def convert_narrow(con, input_parquet: str, output_parquet: str) -> dict:
    """Convert to narrow PQG format (entities + edge rows)."""

    sql = f"""
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

        -- 3. SamplingSite nodes
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
                json_object('place_name', produced_by.sampling_site.place_name) as properties,
                NULL::GEOMETRY as geometry
            FROM source
            WHERE produced_by IS NOT NULL
              AND produced_by.sampling_site IS NOT NULL
        ),

        site_max AS (SELECT COALESCE(MAX(row_id), (SELECT max_id FROM event_max)) as max_id FROM sites),

        -- 4. GeospatialCoordLocation nodes
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

        -- 5. IdentifiedConcept nodes for has_sample_object_type (deduplicated)
        object_types AS (
            SELECT DISTINCT
                unnest.identifier as pid,
                'IdentifiedConcept' as otype,
                unnest.identifier as label,
                json_object('concept_type', 'sample_object_type') as properties
            FROM source, UNNEST(has_sample_object_type) as unnest
            WHERE has_sample_object_type IS NOT NULL
        ),
        object_types_numbered AS (
            SELECT
                (SELECT max_id FROM location_max) + row_number() OVER () as row_id,
                pid, otype, label, NULL as description, NULL::VARCHAR[] as altids,
                NULL::INTEGER as s, NULL::VARCHAR as p, NULL::INTEGER[] as o,
                NULL as n, properties, NULL::GEOMETRY as geometry
            FROM object_types
        ),

        ot_max AS (SELECT COALESCE(MAX(row_id), (SELECT max_id FROM location_max)) as max_id FROM object_types_numbered),

        -- 6. IdentifiedConcept nodes for has_material_category
        materials AS (
            SELECT DISTINCT
                unnest.identifier as pid,
                'IdentifiedConcept' as otype,
                unnest.identifier as label,
                json_object('concept_type', 'material_category') as properties
            FROM source, UNNEST(has_material_category) as unnest
            WHERE has_material_category IS NOT NULL
              AND unnest.identifier NOT IN (SELECT pid FROM object_types)
        ),
        materials_numbered AS (
            SELECT
                (SELECT max_id FROM ot_max) + row_number() OVER () as row_id,
                pid, otype, label, NULL as description, NULL::VARCHAR[] as altids,
                NULL::INTEGER as s, NULL::VARCHAR as p, NULL::INTEGER[] as o,
                NULL as n, properties, NULL::GEOMETRY as geometry
            FROM materials
        ),

        mat_max AS (SELECT COALESCE(MAX(row_id), (SELECT max_id FROM ot_max)) as max_id FROM materials_numbered),

        -- 7. IdentifiedConcept nodes for has_context_category
        contexts AS (
            SELECT DISTINCT
                unnest.identifier as pid,
                'IdentifiedConcept' as otype,
                unnest.identifier as label,
                json_object('concept_type', 'context_category') as properties
            FROM source, UNNEST(has_context_category) as unnest
            WHERE has_context_category IS NOT NULL
              AND unnest.identifier NOT IN (SELECT pid FROM object_types)
              AND unnest.identifier NOT IN (SELECT pid FROM materials)
        ),
        contexts_numbered AS (
            SELECT
                (SELECT max_id FROM mat_max) + row_number() OVER () as row_id,
                pid, otype, label, NULL as description, NULL::VARCHAR[] as altids,
                NULL::INTEGER as s, NULL::VARCHAR as p, NULL::INTEGER[] as o,
                NULL as n, properties, NULL::GEOMETRY as geometry
            FROM contexts
        ),

        ctx_max AS (SELECT COALESCE(MAX(row_id), (SELECT max_id FROM mat_max)) as max_id FROM contexts_numbered),

        -- 8. IdentifiedConcept nodes for keywords
        keywords AS (
            SELECT DISTINCT
                'keyword:' || unnest.keyword as pid,
                'IdentifiedConcept' as otype,
                unnest.keyword as label,
                json_object('concept_type', 'keyword') as properties
            FROM source, UNNEST(keywords) as unnest
            WHERE keywords IS NOT NULL
        ),
        keywords_numbered AS (
            SELECT
                (SELECT max_id FROM ctx_max) + row_number() OVER () as row_id,
                pid, otype, label, NULL as description, NULL::VARCHAR[] as altids,
                NULL::INTEGER as s, NULL::VARCHAR as p, NULL::INTEGER[] as o,
                NULL as n, properties, NULL::GEOMETRY as geometry
            FROM keywords
        ),

        kw_max AS (SELECT COALESCE(MAX(row_id), (SELECT max_id FROM ctx_max)) as max_id FROM keywords_numbered),

        -- Combine all entities
        all_entities AS (
            SELECT * FROM samples
            UNION ALL SELECT * FROM events
            UNION ALL SELECT * FROM sites
            UNION ALL SELECT * FROM locations
            UNION ALL SELECT * FROM object_types_numbered
            UNION ALL SELECT * FROM materials_numbered
            UNION ALL SELECT * FROM contexts_numbered
            UNION ALL SELECT * FROM keywords_numbered
        ),

        -- PID to row_id lookup
        pid_lookup AS (
            SELECT pid, row_id FROM all_entities
        ),

        -- ============================================
        -- EDGE ROWS
        -- ============================================

        -- Edge: Sample -> produced_by -> Event
        edge_produced_by AS (
            SELECT
                (SELECT max_id FROM kw_max) + row_number() OVER () as row_id,
                s.sample_identifier || '_edge_produced_by' as pid,
                '_edge_' as otype,
                NULL as label, NULL as description, NULL::VARCHAR[] as altids,
                samp.row_id as s, 'produced_by' as p, [evt.row_id]::INTEGER[] as o,
                s.source_collection as n, NULL::JSON as properties, NULL::GEOMETRY as geometry
            FROM source s
            JOIN pid_lookup samp ON samp.pid = s.sample_identifier
            JOIN pid_lookup evt ON evt.pid = s.sample_identifier || '_event'
            WHERE s.produced_by IS NOT NULL
        ),

        edge_pb_max AS (SELECT COALESCE(MAX(row_id), (SELECT max_id FROM kw_max)) as max_id FROM edge_produced_by),

        -- Edge: Event -> sampling_site -> Site
        edge_sampling_site AS (
            SELECT
                (SELECT max_id FROM edge_pb_max) + row_number() OVER () as row_id,
                s.sample_identifier || '_edge_sampling_site' as pid,
                '_edge_' as otype,
                NULL as label, NULL as description, NULL::VARCHAR[] as altids,
                evt.row_id as s, 'sampling_site' as p, [site.row_id]::INTEGER[] as o,
                s.source_collection as n, NULL::JSON as properties, NULL::GEOMETRY as geometry
            FROM source s
            JOIN pid_lookup evt ON evt.pid = s.sample_identifier || '_event'
            JOIN pid_lookup site ON site.pid = s.sample_identifier || '_site'
            WHERE s.produced_by IS NOT NULL AND s.produced_by.sampling_site IS NOT NULL
        ),

        edge_ss_max AS (SELECT COALESCE(MAX(row_id), (SELECT max_id FROM edge_pb_max)) as max_id FROM edge_sampling_site),

        -- Edge: Site -> sample_location -> Location
        edge_sample_location AS (
            SELECT
                (SELECT max_id FROM edge_ss_max) + row_number() OVER () as row_id,
                s.sample_identifier || '_edge_sample_location' as pid,
                '_edge_' as otype,
                NULL as label, NULL as description, NULL::VARCHAR[] as altids,
                site.row_id as s, 'sample_location' as p, [loc.row_id]::INTEGER[] as o,
                s.source_collection as n, NULL::JSON as properties, NULL::GEOMETRY as geometry
            FROM source s
            JOIN pid_lookup site ON site.pid = s.sample_identifier || '_site'
            JOIN pid_lookup loc ON loc.pid = s.sample_identifier || '_location'
            WHERE s.produced_by IS NOT NULL
              AND s.produced_by.sampling_site IS NOT NULL
              AND s.produced_by.sampling_site.sample_location IS NOT NULL
              AND s.produced_by.sampling_site.sample_location.latitude IS NOT NULL
        ),

        edge_sl_max AS (SELECT COALESCE(MAX(row_id), (SELECT max_id FROM edge_ss_max)) as max_id FROM edge_sample_location),

        -- Edge: Sample -> has_sample_object_type -> Concept
        edge_object_type AS (
            SELECT
                (SELECT max_id FROM edge_sl_max) + row_number() OVER () as row_id,
                s.sample_identifier || '_edge_ot_' || row_number() OVER (PARTITION BY s.sample_identifier) as pid,
                '_edge_' as otype,
                NULL as label, NULL as description, NULL::VARCHAR[] as altids,
                samp.row_id as s, 'has_sample_object_type' as p, [concept.row_id]::INTEGER[] as o,
                s.source_collection as n, NULL::JSON as properties, NULL::GEOMETRY as geometry
            FROM source s, UNNEST(s.has_sample_object_type) as unnest
            JOIN pid_lookup samp ON samp.pid = s.sample_identifier
            JOIN pid_lookup concept ON concept.pid = unnest.identifier
            WHERE s.has_sample_object_type IS NOT NULL
        ),

        edge_ot_max AS (SELECT COALESCE(MAX(row_id), (SELECT max_id FROM edge_sl_max)) as max_id FROM edge_object_type),

        -- Edge: Sample -> has_material_category -> Concept
        edge_material AS (
            SELECT
                (SELECT max_id FROM edge_ot_max) + row_number() OVER () as row_id,
                s.sample_identifier || '_edge_mat_' || row_number() OVER (PARTITION BY s.sample_identifier) as pid,
                '_edge_' as otype,
                NULL as label, NULL as description, NULL::VARCHAR[] as altids,
                samp.row_id as s, 'has_material_category' as p, [concept.row_id]::INTEGER[] as o,
                s.source_collection as n, NULL::JSON as properties, NULL::GEOMETRY as geometry
            FROM source s, UNNEST(s.has_material_category) as unnest
            JOIN pid_lookup samp ON samp.pid = s.sample_identifier
            JOIN pid_lookup concept ON concept.pid = unnest.identifier
            WHERE s.has_material_category IS NOT NULL
        ),

        edge_mat_max AS (SELECT COALESCE(MAX(row_id), (SELECT max_id FROM edge_ot_max)) as max_id FROM edge_material),

        -- Edge: Sample -> has_context_category -> Concept
        edge_context AS (
            SELECT
                (SELECT max_id FROM edge_mat_max) + row_number() OVER () as row_id,
                s.sample_identifier || '_edge_ctx_' || row_number() OVER (PARTITION BY s.sample_identifier) as pid,
                '_edge_' as otype,
                NULL as label, NULL as description, NULL::VARCHAR[] as altids,
                samp.row_id as s, 'has_context_category' as p, [concept.row_id]::INTEGER[] as o,
                s.source_collection as n, NULL::JSON as properties, NULL::GEOMETRY as geometry
            FROM source s, UNNEST(s.has_context_category) as unnest
            JOIN pid_lookup samp ON samp.pid = s.sample_identifier
            JOIN pid_lookup concept ON concept.pid = unnest.identifier
            WHERE s.has_context_category IS NOT NULL
        ),

        edge_ctx_max AS (SELECT COALESCE(MAX(row_id), (SELECT max_id FROM edge_mat_max)) as max_id FROM edge_context),

        -- Edge: Sample -> keywords -> Concept
        edge_keywords AS (
            SELECT
                (SELECT max_id FROM edge_ctx_max) + row_number() OVER () as row_id,
                s.sample_identifier || '_edge_kw_' || row_number() OVER (PARTITION BY s.sample_identifier) as pid,
                '_edge_' as otype,
                NULL as label, NULL as description, NULL::VARCHAR[] as altids,
                samp.row_id as s, 'keywords' as p, [concept.row_id]::INTEGER[] as o,
                s.source_collection as n, NULL::JSON as properties, NULL::GEOMETRY as geometry
            FROM source s, UNNEST(s.keywords) as unnest
            JOIN pid_lookup samp ON samp.pid = s.sample_identifier
            JOIN pid_lookup concept ON concept.pid = 'keyword:' || unnest.keyword
            WHERE s.keywords IS NOT NULL
        ),

        -- Final: all entities + all edges
        final AS (
            SELECT * FROM all_entities
            UNION ALL SELECT * FROM edge_produced_by
            UNION ALL SELECT * FROM edge_sampling_site
            UNION ALL SELECT * FROM edge_sample_location
            UNION ALL SELECT * FROM edge_object_type
            UNION ALL SELECT * FROM edge_material
            UNION ALL SELECT * FROM edge_context
            UNION ALL SELECT * FROM edge_keywords
        )

        SELECT * FROM final ORDER BY row_id
    ) TO '{output_parquet}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """

    con.execute(sql)
    return {}


def convert_wide(con, input_parquet: str, output_parquet: str) -> dict:
    """Convert to wide PQG format (edges as p__* columns, no edge rows)."""

    # First create narrow, then convert to wide using existing wide_converter pattern
    # This is simpler and reuses the proven narrow→wide transformation

    sql = f"""
    COPY (
        WITH source AS (
            SELECT row_number() OVER () as src_row_id, *
            FROM read_parquet('{input_parquet}')
        ),

        -- Entity nodes (same as narrow but without s/p/o columns)
        samples AS (
            SELECT
                src_row_id as row_id,
                sample_identifier as pid,
                'MaterialSampleRecord' as otype,
                label, description,
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
                json_object('result_time', produced_by.result_time) as properties,
                NULL::GEOMETRY as geometry
            FROM source WHERE produced_by IS NOT NULL
        ),
        event_max AS (SELECT COALESCE(MAX(row_id), (SELECT max_id FROM sample_max)) as max_id FROM events),

        sites AS (
            SELECT
                (SELECT max_id FROM event_max) + row_number() OVER () as row_id,
                sample_identifier || '_site' as pid,
                'SamplingSite' as otype,
                produced_by.sampling_site.label as label,
                NULL as description,
                NULL::VARCHAR[] as altids,
                source_collection as n,
                NULL::JSON as properties,
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
                NULL as label, NULL as description,
                NULL::VARCHAR[] as altids,
                source_collection as n,
                NULL::JSON as properties,
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

        -- Concept entities
        object_types AS (
            SELECT DISTINCT unnest.identifier as pid FROM source, UNNEST(has_sample_object_type) as unnest
            WHERE has_sample_object_type IS NOT NULL
        ),
        object_types_numbered AS (
            SELECT (SELECT max_id FROM location_max) + row_number() OVER () as row_id,
                   pid, 'IdentifiedConcept' as otype, pid as label, NULL as description,
                   NULL::VARCHAR[] as altids, NULL as n, NULL::JSON as properties, NULL::GEOMETRY as geometry
            FROM object_types
        ),
        ot_max AS (SELECT COALESCE(MAX(row_id), (SELECT max_id FROM location_max)) as max_id FROM object_types_numbered),

        materials AS (
            SELECT DISTINCT unnest.identifier as pid FROM source, UNNEST(has_material_category) as unnest
            WHERE has_material_category IS NOT NULL AND unnest.identifier NOT IN (SELECT pid FROM object_types)
        ),
        materials_numbered AS (
            SELECT (SELECT max_id FROM ot_max) + row_number() OVER () as row_id,
                   pid, 'IdentifiedConcept' as otype, pid as label, NULL as description,
                   NULL::VARCHAR[] as altids, NULL as n, NULL::JSON as properties, NULL::GEOMETRY as geometry
            FROM materials
        ),
        mat_max AS (SELECT COALESCE(MAX(row_id), (SELECT max_id FROM ot_max)) as max_id FROM materials_numbered),

        contexts AS (
            SELECT DISTINCT unnest.identifier as pid FROM source, UNNEST(has_context_category) as unnest
            WHERE has_context_category IS NOT NULL
              AND unnest.identifier NOT IN (SELECT pid FROM object_types)
              AND unnest.identifier NOT IN (SELECT pid FROM materials)
        ),
        contexts_numbered AS (
            SELECT (SELECT max_id FROM mat_max) + row_number() OVER () as row_id,
                   pid, 'IdentifiedConcept' as otype, pid as label, NULL as description,
                   NULL::VARCHAR[] as altids, NULL as n, NULL::JSON as properties, NULL::GEOMETRY as geometry
            FROM contexts
        ),
        ctx_max AS (SELECT COALESCE(MAX(row_id), (SELECT max_id FROM mat_max)) as max_id FROM contexts_numbered),

        keywords AS (
            SELECT DISTINCT 'keyword:' || unnest.keyword as pid, unnest.keyword as label
            FROM source, UNNEST(keywords) as unnest WHERE keywords IS NOT NULL
        ),
        keywords_numbered AS (
            SELECT (SELECT max_id FROM ctx_max) + row_number() OVER () as row_id,
                   pid, 'IdentifiedConcept' as otype, label, NULL as description,
                   NULL::VARCHAR[] as altids, NULL as n, NULL::JSON as properties, NULL::GEOMETRY as geometry
            FROM keywords
        ),

        -- All entities combined
        all_entities AS (
            SELECT * FROM samples UNION ALL SELECT * FROM events UNION ALL SELECT * FROM sites
            UNION ALL SELECT * FROM locations UNION ALL SELECT * FROM object_types_numbered
            UNION ALL SELECT * FROM materials_numbered UNION ALL SELECT * FROM contexts_numbered
            UNION ALL SELECT * FROM keywords_numbered
        ),

        -- PID lookup
        pid_lookup AS (SELECT pid, row_id FROM all_entities),

        -- Build wide columns for samples
        sample_wide AS (
            SELECT
                samp.row_id, samp.pid, samp.otype, samp.label, samp.description,
                samp.altids, samp.n, samp.properties, samp.geometry,
                evt.row_id as p__produced_by,
                (SELECT list(c.row_id) FROM UNNEST(s.has_sample_object_type) as u
                 JOIN pid_lookup c ON c.pid = u.identifier) as p__has_sample_object_type,
                (SELECT list(c.row_id) FROM UNNEST(s.has_material_category) as u
                 JOIN pid_lookup c ON c.pid = u.identifier) as p__has_material_category,
                (SELECT list(c.row_id) FROM UNNEST(s.has_context_category) as u
                 JOIN pid_lookup c ON c.pid = u.identifier) as p__has_context_category,
                (SELECT list(c.row_id) FROM UNNEST(s.keywords) as u
                 JOIN pid_lookup c ON c.pid = 'keyword:' || u.keyword) as p__keywords,
                NULL::INTEGER as p__sampling_site,
                NULL::INTEGER as p__sample_location
            FROM source s
            JOIN samples samp ON samp.pid = s.sample_identifier
            LEFT JOIN pid_lookup evt ON evt.pid = s.sample_identifier || '_event'
        ),

        -- Events with sampling_site edge
        event_wide AS (
            SELECT
                e.row_id, e.pid, e.otype, e.label, e.description,
                e.altids, e.n, e.properties, e.geometry,
                NULL::INTEGER as p__produced_by,
                NULL::INTEGER[] as p__has_sample_object_type,
                NULL::INTEGER[] as p__has_material_category,
                NULL::INTEGER[] as p__has_context_category,
                NULL::INTEGER[] as p__keywords,
                site.row_id as p__sampling_site,
                NULL::INTEGER as p__sample_location
            FROM events e
            LEFT JOIN pid_lookup site ON site.pid = replace(e.pid, '_event', '_site')
        ),

        -- Sites with sample_location edge
        site_wide AS (
            SELECT
                st.row_id, st.pid, st.otype, st.label, st.description,
                st.altids, st.n, st.properties, st.geometry,
                NULL::INTEGER as p__produced_by,
                NULL::INTEGER[] as p__has_sample_object_type,
                NULL::INTEGER[] as p__has_material_category,
                NULL::INTEGER[] as p__has_context_category,
                NULL::INTEGER[] as p__keywords,
                NULL::INTEGER as p__sampling_site,
                loc.row_id as p__sample_location
            FROM sites st
            LEFT JOIN pid_lookup loc ON loc.pid = replace(st.pid, '_site', '_location')
        ),

        -- Other entities (no outgoing edges)
        other_entities AS (
            SELECT row_id, pid, otype, label, description, altids, n, properties, geometry,
                   NULL::INTEGER as p__produced_by,
                   NULL::INTEGER[] as p__has_sample_object_type,
                   NULL::INTEGER[] as p__has_material_category,
                   NULL::INTEGER[] as p__has_context_category,
                   NULL::INTEGER[] as p__keywords,
                   NULL::INTEGER as p__sampling_site,
                   NULL::INTEGER as p__sample_location
            FROM locations
            UNION ALL
            SELECT row_id, pid, otype, label, description, altids, n, properties, geometry,
                   NULL, NULL, NULL, NULL, NULL, NULL, NULL
            FROM object_types_numbered
            UNION ALL
            SELECT row_id, pid, otype, label, description, altids, n, properties, geometry,
                   NULL, NULL, NULL, NULL, NULL, NULL, NULL
            FROM materials_numbered
            UNION ALL
            SELECT row_id, pid, otype, label, description, altids, n, properties, geometry,
                   NULL, NULL, NULL, NULL, NULL, NULL, NULL
            FROM contexts_numbered
            UNION ALL
            SELECT row_id, pid, otype, label, description, altids, n, properties, geometry,
                   NULL, NULL, NULL, NULL, NULL, NULL, NULL
            FROM keywords_numbered
        ),

        -- Final wide output
        final AS (
            SELECT * FROM sample_wide
            UNION ALL SELECT * FROM event_wide
            UNION ALL SELECT * FROM site_wide
            UNION ALL SELECT * FROM other_entities
        )

        SELECT * FROM final ORDER BY row_id
    ) TO '{output_parquet}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """

    con.execute(sql)
    return {}


def main():
    parser = argparse.ArgumentParser(
        description="Convert iSamples parquet to PQG format using SQL (130x faster)"
    )
    parser.add_argument("input", help="Input iSamples parquet file")
    parser.add_argument("output", help="Output PQG parquet file")
    parser.add_argument("--wide", action="store_true",
                        help="Output wide format (edges as p__* columns)")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Print progress messages")
    args = parser.parse_args()

    # Initialize DuckDB with spatial extension
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")

    format_name = "wide" if args.wide else "narrow"
    print(f"Converting {args.input} to {format_name} PQG format...")

    # Count source
    start = time.time()
    source_count = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{args.input}')"
    ).fetchone()[0]
    print(f"  Source rows: {source_count:,}")

    # Convert
    if args.wide:
        convert_wide(con, args.input, args.output)
    else:
        convert_narrow(con, args.input, args.output)

    elapsed = time.time() - start

    # Verify
    output_count = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{args.output}')"
    ).fetchone()[0]
    output_size = Path(args.output).stat().st_size / (1024 * 1024)

    print(f"  Output rows: {output_count:,}")
    print(f"  Output size: {output_size:.1f} MB")
    print(f"  Time: {elapsed:.2f}s")
    print(f"  Rate: {source_count/elapsed:,.0f} rows/sec")


if __name__ == "__main__":
    main()
