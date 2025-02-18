"""
cli for pqg.
"""

import json
import click
import pqg
import pqg.common

@click.group()
@click.pass_context
@click.argument("store", type=click.Path(exists=True))
def cli(ctx, store):
    ctx.ensure_object(dict)
    ctx.obj["store"] = store

@cli.command()
@click.pass_context
def records(ctx):
    graph = pqg.PQG(ctx.obj["store"])
    for entry in graph.getIds():
        print(json.dumps(entry))

@cli.command()
@click.pass_context
@click.argument("pid")
@click.option("-e", "--expand", default=False, is_flag=True)
def record(ctx, pid:str, expand:bool):
    graph = pqg.PQG(ctx.obj["store"])
    graph.initialize([])
    node = graph.getNode(pid)
    print(json.dumps(node, indent=2, cls=pqg.common.JSONDateTimeEncoder))

    #TODO: load PQG from duckdb. Need to persist and retrieve the graph metadata for class management

if __name__ == "__main__":
    cli()
