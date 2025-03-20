"""
cli for pqg.
"""

import json
import logging
import typing
import click
import duckdb

import rich
import rich.tree

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


if __name__ == "__main__":
    cli()
