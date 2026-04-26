"""
cli for pqg.
"""

import json
import logging
import os
import time
import typing
import click
import duckdb

import rich
import rich.tree
import rich.console

import pqg
import pqg.common


def get_logger():
    return logging.getLogger("pqg")


@click.group()
@click.pass_context
@click.option(
    "-V",
    "--verbosity",
    default="INFO",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]),
)
def cli(ctx, verbosity):
    logging.basicConfig(level=verbosity)
    ctx.ensure_object(dict)
    ctx.obj["dbinstance"] = duckdb.connect()


@cli.command()
@click.pass_context
@click.argument("store")
@click.option("-o", "--otype", default=None)
@click.option("-m", "--maxrows", default=10)
def entries(ctx, store, otype: typing.Optional[str], maxrows: int):
    """List identifiers and otype, optionally restricting by otype."""
    graph = pqg.PQG(ctx.obj["dbinstance"], store)
    graph.loadMetadata()
    for entry in graph.getIds(otype=otype, maxrows=maxrows):
        print(json.dumps(entry))


@cli.command()
@click.pass_context
@click.argument("store")
@click.argument("pid")
@click.option(
    "-e",
    "--expand",
    default=False,
    is_flag=True,
    help="Expand to include all objects referencing and referced by pid.",
)
def node(ctx, store, pid: str, expand: bool):
    """Retrieve a node, optionally including referenced nodes."""
    graph = pqg.PQG(ctx.obj["dbinstance"], store)
    graph.loadMetadata()
    if expand:
        node = graph.getNode(pid)
    else:
        node = graph.getNodeEntry(pid)
    print(json.dumps(node, indent=2, cls=pqg.common.JSONDateTimeEncoder))


@cli.command()
@click.pass_context
@click.argument("store")
@click.argument("pid")
def refs(ctx, store, pid: str):
    """Retrieve references to the specified node, recursively."""
    graph = pqg.PQG(ctx.obj["dbinstance"], store)
    graph.loadMetadata()
    for row in graph.getRootsForPid(pid):
        print(json.dumps(row, cls=pqg.common.JSONDateTimeEncoder))


@cli.command()
@click.pass_context
@click.argument("store")
@click.argument("pid")
def tree(ctx, store, pid: str):
    """Retrieve the nodes referenced by the specified node, recursively."""
    graph = pqg.PQG(ctx.obj["dbinstance"], store)
    graph.loadMetadata()
    _tree = rich.tree.Tree(pid)
    t0 = {
        pid: _tree,
    }
    for entry in graph.breadthFirstTraversal(pid):
        s = entry[0]
        p = entry[1]
        o = entry[2]
        _ts = t0[s]
        _tp = t0.get(p)
        if _tp is None:
            _tp = _ts.add(p)
            t0[p] = _tp
        for _o in o:
            _to = t0.get(_o)
            if _to is None:
                _to = _tp.add(_o)
                t0[_o] = _to
            # print(json.dumps(entry))
    rich.print(_tree)


@cli.command("types")
@click.pass_context
@click.argument("store")
def list_otypes(ctx, store):
    """List the types of objects and their counts."""
    graph = pqg.PQG(ctx.obj["dbinstance"], store)
    graph.loadMetadata()
    for row in graph.objectCounts():
        print(json.dumps(row))


@cli.command("predicates")
@click.pass_context
@click.argument("store")
def list_predicates(ctx, store):
    """List the predicates and their counts."""
    graph = pqg.PQG(ctx.obj["dbinstance"], store)
    graph.loadMetadata()
    for row in graph.predicateCounts():
        print(json.dumps(row))


@cli.command("metadata")
@click.pass_context
@click.argument("store")
def get_kv_metadata(ctx, store):
    """Retrieve the KV metadata."""
    graph = pqg.PQG(ctx.obj["dbinstance"], store)
    if graph._isparquet:
        kv_source = graph._source.replace("read_parquet(", "parquet_kv_metadata(", 1)
        with graph.getCursor() as cur:
            result = cur.sql(f"SELECT * FROM {kv_source}")
            meta = {}
            while row := result.fetchone():
                try:
                    data = json.loads(row[2].decode("utf-8"))
                except json.decoder.JSONDecodeError:
                    data = row[2].decode("utf-8")
                meta[row[1].decode("utf-8")] = data
            print(json.dumps(meta, indent=2, cls=pqg.common.JSONDateTimeEncoder))
    else:
        raise NotImplementedError("TODO: dump metadata table for DDB instance")


@cli.command("geo")
@click.pass_context
@click.argument("store")
def get_geo(ctx, store):
    """Retrieve geometries"""
    graph = pqg.PQG(ctx.obj["dbinstance"], store)
    graph.loadMetadata()
    with graph.getCursor() as csr:
        csr.sql("install spatial; load spatial;")
        print('{"type": "FeatureCollection", "features": [')
        results = csr.execute(
            f"SELECT pid, ST_AsGeoJSON(geometry) FROM {graph._table} where geometry is not NULL limit 500"
        )
        row = results.fetchone()
        if row is not None:
            entry = {
                "type": "Feature",
                "properties": {"pid": row[0]},
                "geometry": json.loads(row[1]),
            }
            print(json.dumps(entry))
        while row is not None:
            entry = {
                "type": "Feature",
                "properties": {"pid": row[0]},
                "geometry": json.loads(row[1]),
            }
            print(f", {json.dumps(entry)}")
            row = results.fetchone()
        print("]}")


@cli.command("add-h3")
@click.pass_context
@click.argument("input_parquet")
@click.option("-o", "--output", required=True, help="Output parquet file path")
@click.option(
    "-r",
    "--resolutions",
    default="4,6,8",
    help="Comma-separated H3 resolutions to add (default: 4,6,8)",
)
@click.option(
    "--lat-col", default="latitude", help="Latitude column name (default: latitude)"
)
@click.option(
    "--lon-col", default="longitude", help="Longitude column name (default: longitude)"
)
def add_h3(
    ctx,
    input_parquet: str,
    output: str,
    resolutions: str,
    lat_col: str,
    lon_col: str,
):
    """Add H3 index columns to a parquet file.

    Creates a new parquet file with h3_resN columns for each specified resolution.
    Only rows with valid lat/lon will have H3 values; others will be NULL.

    Example:
        pqg add-h3 input.parquet -o output_h3.parquet
        pqg add-h3 input.parquet -o output.parquet -r 4,6
    """
    console = rich.console.Console()
    logger = get_logger()

    # Parse resolutions
    res_list = [int(r.strip()) for r in resolutions.split(",")]
    logger.info(f"Adding H3 columns at resolutions: {res_list}")

    con = ctx.obj["dbinstance"]

    # Install and load H3 extension (community extension)
    console.print("[blue]Installing H3 extension from community...[/blue]")
    con.execute("INSTALL h3 FROM community; LOAD h3;")

    # Build H3 column expressions
    h3_cols = []
    for res in res_list:
        h3_cols.append(
            f"CASE WHEN {lat_col} IS NOT NULL AND {lon_col} IS NOT NULL "
            f"THEN h3_latlng_to_cell({lat_col}, {lon_col}, {res}) END as h3_res{res}"
        )
    h3_select = ", ".join(h3_cols)

    # Determine source (local file or URL)
    if input_parquet.startswith("http://") or input_parquet.startswith("https://"):
        source = f"read_parquet('{input_parquet}')"
    else:
        source = f"read_parquet('{os.path.abspath(input_parquet)}')"

    query = f"""
    COPY (
        SELECT *, {h3_select}
        FROM {source}
    ) TO '{os.path.abspath(output)}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """

    console.print(f"[blue]Processing {input_parquet}...[/blue]")
    start = time.time()
    con.execute(query)
    elapsed = time.time() - start

    # Get stats
    stats = con.sql(
        f"SELECT COUNT(*) as total, COUNT(h3_res{res_list[0]}) as with_h3 "
        f"FROM read_parquet('{os.path.abspath(output)}')"
    ).fetchone()

    output_size = os.path.getsize(output) / (1024 * 1024)

    console.print(f"[green]✓ Generated {output}[/green]")
    console.print(f"  Size: {output_size:.1f} MB")
    console.print(f"  Total rows: {stats[0]:,}")
    console.print(f"  Rows with H3: {stats[1]:,} ({100*stats[1]/stats[0]:.1f}%)")
    console.print(f"  Time: {elapsed:.1f}s")


@cli.command("facet-summaries")
@click.pass_context
@click.argument("input_parquet")
@click.option(
    "-o",
    "--output-dir",
    required=True,
    help="Output directory for summary files",
)
@click.option(
    "--otype-filter",
    default="MaterialSampleRecord",
    help="Filter to this otype (default: MaterialSampleRecord)",
)
@click.option(
    "--min-cross-count",
    default=100,
    type=int,
    help="Minimum count for cross-facet combinations (default: 100)",
)
def facet_summaries(
    ctx,
    input_parquet: str,
    output_dir: str,
    otype_filter: str,
    min_cross_count: int,
):
    """Generate pre-computed facet summary tables from a wide parquet file.

    Creates two output files:
    - facet_summaries_all.parquet: Combined counts for source, material, context, object_type
    - facet_source_material_cross.parquet: Source × material cross-tabulation

    Example:
        pqg facet-summaries wide.parquet -o summaries/
    """
    console = rich.console.Console()
    logger = get_logger()

    con = ctx.obj["dbinstance"]

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Determine source
    if input_parquet.startswith("http://") or input_parquet.startswith("https://"):
        source = f"read_parquet('{input_parquet}')"
    else:
        source = f"read_parquet('{os.path.abspath(input_parquet)}')"

    otype_clause = f"otype = '{otype_filter}'" if otype_filter else "1=1"

    # Generate combined facet summaries
    console.print("[blue]Generating combined facet summaries...[/blue]")
    start = time.time()

    combined_path = os.path.join(output_dir, "facet_summaries_all.parquet")
    combined_query = f"""
    COPY (
        -- Source facet
        SELECT 'source' as facet_type, n as facet_value, NULL as scheme, COUNT(*) as count
        FROM {source}
        WHERE {otype_clause}
        GROUP BY n

        UNION ALL

        -- Material facet
        SELECT 'material' as facet_type, c.label as facet_value, c.scheme_name as scheme, COUNT(*) as count
        FROM (
            SELECT UNNEST(p__has_material_category) as material_id
            FROM {source}
            WHERE {otype_clause} AND p__has_material_category IS NOT NULL
        ) s
        JOIN (SELECT row_id, label, scheme_name FROM {source} WHERE otype = 'IdentifiedConcept') c
        ON c.row_id = s.material_id
        GROUP BY c.label, c.scheme_name

        UNION ALL

        -- Context facet
        SELECT 'context' as facet_type, c.label as facet_value, c.scheme_name as scheme, COUNT(*) as count
        FROM (
            SELECT UNNEST(p__has_context_category) as context_id
            FROM {source}
            WHERE {otype_clause} AND p__has_context_category IS NOT NULL
        ) s
        JOIN (SELECT row_id, label, scheme_name FROM {source} WHERE otype = 'IdentifiedConcept') c
        ON c.row_id = s.context_id
        GROUP BY c.label, c.scheme_name

        UNION ALL

        -- Object type facet
        SELECT 'object_type' as facet_type, c.label as facet_value, c.scheme_name as scheme, COUNT(*) as count
        FROM (
            SELECT UNNEST(p__has_sample_object_type) as type_id
            FROM {source}
            WHERE {otype_clause} AND p__has_sample_object_type IS NOT NULL
        ) s
        JOIN (SELECT row_id, label, scheme_name FROM {source} WHERE otype = 'IdentifiedConcept') c
        ON c.row_id = s.type_id
        GROUP BY c.label, c.scheme_name
    ) TO '{combined_path}' (FORMAT PARQUET);
    """
    con.execute(combined_query)

    combined_stats = con.sql(
        f"SELECT COUNT(*) FROM read_parquet('{combined_path}')"
    ).fetchone()
    combined_size = os.path.getsize(combined_path)

    console.print(f"[green]✓ {combined_path}[/green]")
    console.print(f"  Rows: {combined_stats[0]}, Size: {combined_size:,} bytes")

    # Generate cross-facet summary
    console.print("[blue]Generating source × material cross-tabulation...[/blue]")

    cross_path = os.path.join(output_dir, "facet_source_material_cross.parquet")
    cross_query = f"""
    COPY (
        SELECT
            s.source,
            c.label as material,
            COUNT(*) as count
        FROM (
            SELECT n as source, UNNEST(p__has_material_category) as material_id
            FROM {source}
            WHERE {otype_clause} AND p__has_material_category IS NOT NULL
        ) s
        JOIN (SELECT row_id, label FROM {source} WHERE otype = 'IdentifiedConcept') c
        ON c.row_id = s.material_id
        GROUP BY s.source, c.label
        HAVING COUNT(*) > {min_cross_count}
        ORDER BY count DESC
    ) TO '{cross_path}' (FORMAT PARQUET);
    """
    con.execute(cross_query)

    cross_stats = con.sql(
        f"SELECT COUNT(*) FROM read_parquet('{cross_path}')"
    ).fetchone()
    cross_size = os.path.getsize(cross_path)

    elapsed = time.time() - start

    console.print(f"[green]✓ {cross_path}[/green]")
    console.print(f"  Rows: {cross_stats[0]}, Size: {cross_size:,} bytes")
    console.print(f"[green]Total time: {elapsed:.1f}s[/green]")

    # Print summary
    console.print("\n[bold]Summary:[/bold]")
    facet_counts = con.sql(
        f"SELECT facet_type, COUNT(*) as n, SUM(count) as total "
        f"FROM read_parquet('{combined_path}') GROUP BY facet_type"
    ).fetchall()
    for row in facet_counts:
        console.print(f"  {row[0]}: {row[1]} values, {row[2]:,} total records")


if __name__ == "__main__":
    cli()
