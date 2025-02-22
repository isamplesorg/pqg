"""
cli for pqg.
"""

import json
import logging
import click
import pqg
import pqg.common

def get_logger():
    return logging.getLogger('pqg')

@click.group()
@click.pass_context
def cli(ctx):
    logging.basicConfig(level=logging.INFO)
    ctx.ensure_object(dict)

@cli.command()
@click.pass_context
@click.argument("store", type=click.Path(exists=True))
def records(ctx, store):
    graph = pqg.PQG(store)
    for entry in graph.getIds():
        print(json.dumps(entry))

@cli.command()
@click.pass_context
@click.argument("store", type=click.Path(exists=True))
@click.argument("pid")
@click.option("-e", "--expand", default=False, is_flag=True, help="Expand to include all objects referencing and referced by pid.")
def record(ctx, store, pid:str, expand:bool):
    graph = pqg.PQG(store)
    graph.initialize([])
    if expand:
        roots = graph.getRootsForPid([pid, ])
        results = []
        for edge in roots:
            apid = edge[1]
            results.append(graph.getNode(apid))
        print(json.dumps(results, indent=2, cls=pqg.common.JSONDateTimeEncoder))
    else:
        node = graph.getNode(pid)
        print(json.dumps(node, indent=2, cls=pqg.common.JSONDateTimeEncoder))

@cli.command()
@click.pass_context
@click.argument("store", type=click.Path(exists=True))
@click.argument("pid")
@click.option("-p", "--properties", default=False, is_flag=True, help="Include all object properties in output.")
def tree(ctx, store, pid:str, properties):
    graph = pqg.PQG(store)
    graph.initialize([])
    for entry in graph.breadthFirstTraversal(pid):
        print(f"{entry}")

@cli.command()
@click.pass_context
@click.argument("store", type=click.Path(exists=True))
def list_otypes(ctx, store):
    graph = pqg.PQG(store)
    graph.initialize([])


    #TODO: load PQG from duckdb. Need to persist and retrieve the graph metadata for class management

if __name__ == "__main__":
    cli()
