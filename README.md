# Property Graph in DuckDB

This is an implementation of a property graph using DuckDB.

The intent is providing functionality that lies between a full property graph such as Apache GraphAR and a fully decomposed graph such as an RDF store.

The graph is composed of nodes (vertices) and edges (relations between vertices). Nodes may be extended to represent different classes of information, though all nodes inherit from a common base node.

Only a single table ise used to capture node and edge information.

The model of a node is:

```
row_id      Auto-incrementing integer primary key for the node row.
pid         Globally unique identifier for the node (unique but not primary key).
tcreated    Time stamp for when the node instance is created.
tmodified   Time stamp for when the instance has been modified.
otype       The type of entity described by this node instance.
label       Optional textual label for human use.
description Optional human readable description of the node.
altids      A list of alternate identifiers for the node (aliases of pid)
...         Additional properties as needed for the class being stored at the node.
```

Node classes store literal values or lists of literal values. Composite nodes are stored as separate nodes and associations between the separate nodes are stored as edges.

The model of an edge is:

```
row_id      Auto-incrementing integer primary key for the edge row
pid         Globally unique identifier for the edge.
tcreated    Time stamp for when the edge instance is created.
tmodified   Time stamp for when the instance has been modified.
otype       "_edge_"
label       Optional textual label for human use.
description Optional human readable description of the edge.
altids      A list of alternate identifiers for the edge (aliases of pid)
s           Subject or source of the relation (row_id integer reference).
p           Predicate or type of the relation.
o           List of targets (objects) for the relation (list of row_id integer references).
n           Name of the graph containing the relation.
```

Serializing entities to a `pqg` should follow the basic rule that nested structures should be decomposed to a single level, with each level stored as a separate instance and the relations between those instance captured as edges. Hence, a given entity may result in several node instances along with corresponding edge instances.


## Installation

It is recommended that installation is made to a virtual environment. For example:

Using `venv`:

```
git clone https://github.com/isamplesorg/pqg.git
cd pqg
python -m venv .venv
source .venv/bin/activate
python -m pip install -e .

pqg --help
```

Using `uv`:

```
git clone https://github.com/isamplesorg/pqg.git
cd pqg
uv sync

uv run pqg --help
```
