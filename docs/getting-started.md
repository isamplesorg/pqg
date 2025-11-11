# Getting Started with PQG

Welcome to PQG (Property Graph in DuckDB)! This guide will help you get up and running quickly.

## What is PQG?

PQG is a property graph database implementation that uses DuckDB as its backend. It provides a middle ground between full property graph databases (like Neo4j) and fully decomposed RDF stores. Think of it as a powerful way to store and query connected data with the performance and simplicity of DuckDB.

### Key Features

- **Simple Data Model**: Store nodes (entities) and edges (relationships) in a single table
- **Python Native**: Work with Python dataclasses - no need to learn a new query language
- **Automatic Decomposition**: Complex nested objects are automatically broken down into nodes and edges
- **Spatial Support**: Built-in support for geographic data and GeoJSON export
- **Multiple Export Formats**: Export to Parquet, Graphviz, or GeoJSON
- **Powerful Querying**: Traverse graphs, query relationships, and aggregate data

## Installation

PQG requires Python 3.11 or higher.

### Option 1: Using venv (Recommended for beginners)

```bash
# Clone the repository
git clone https://github.com/isamplesorg/pqg.git
cd pqg

# Create a virtual environment
python -m venv .venv

# Activate the virtual environment
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install PQG in development mode
python -m pip install -e .

# Verify installation
pqg --help
```

### Option 2: Using uv (Faster alternative)

```bash
# Clone the repository
git clone https://github.com/isamplesorg/pqg.git
cd pqg

# Sync dependencies with uv
uv sync

# Run PQG commands
uv run pqg --help
```

## Quick Start: Your First Graph

Let's create a simple graph to understand the basics.

### Step 1: Create a Python script

Create a file called `my_first_graph.py`:

```python
import duckdb
from pqg import PQG, Node

# Create an in-memory DuckDB database
db = duckdb.connect()

# Initialize a PQG instance
graph = PQG(db, source="my_first_graph")

# Register the Node type
graph.registerType(Node)

# Initialize the graph schema
graph.initialize()
```

### Step 2: Add some nodes

```python
# Create a person node
person = Node(
    pid="person_001",
    label="Alice Smith",
    description="Software engineer and graph enthusiast"
)

# Create a project node
project = Node(
    pid="project_001",
    label="PQG Tutorial",
    description="Learning property graphs"
)

# Add nodes to the graph
graph.addNode(person)
graph.addNode(project)

# Commit the changes
db.commit()

print("Nodes added successfully!")
```

### Step 3: Add a relationship

```python
from pqg import Edge

# Create an edge connecting person to project
works_on = Edge(
    s="person_001",  # Subject (source)
    p="works_on",    # Predicate (relationship type)
    o=["project_001"]  # Object (target) - note: this is a list
)

# Add the edge to the graph
graph.addEdge(works_on)
db.commit()

print("Relationship added!")
```

### Step 4: Query your graph

```python
# Get a node
node_data = graph.getNode("person_001")
print(f"\nPerson data: {node_data}")

# Count objects in the graph
print("\nObjects in graph:")
for otype, count in graph.objectCounts():
    print(f"  {otype}: {count}")

# Query relationships
print("\nRelationships:")
for subject, predicate, obj in graph.getRelations(subject="person_001"):
    print(f"  {subject} --{predicate}--> {obj}")
```

### Complete Example

Here's the complete script:

```python
import duckdb
from pqg import PQG, Node, Edge

# Initialize
db = duckdb.connect()
graph = PQG(db, source="my_first_graph")
graph.registerType(Node)
graph.initialize()

# Create nodes
person = Node(pid="person_001", label="Alice Smith")
project = Node(pid="project_001", label="PQG Tutorial")

# Add to graph
graph.addNode(person)
graph.addNode(project)

# Create relationship
works_on = Edge(s="person_001", p="works_on", o=["project_001"])
graph.addEdge(works_on)

# Commit
db.commit()

# Query
print("Graph created successfully!")
print(f"\nTotal nodes: {len(list(graph.getIds()))}")
for subject, predicate, obj in graph.getRelations():
    print(f"{subject} --{predicate}--> {obj}")
```

Run it:

```bash
python my_first_graph.py
```

## Understanding the Data Model

### Nodes

Nodes represent entities in your graph. Every node has:

- `pid`: A unique identifier (auto-generated if not provided)
- `label`: A human-friendly name (optional)
- `description`: A longer description (optional)
- `altids`: Alternative identifiers (optional)

### Edges

Edges represent relationships between nodes. They use a triple pattern:

- `s`: **Subject** - The source node (who/what is doing something)
- `p`: **Predicate** - The relationship type (what they're doing)
- `o`: **Object** - The target node(s) (what they're doing it to)
- `n`: **Named graph** - Optional graph name for grouping relationships

Example: `Alice --works_on--> Project` becomes:
- s: "person_001" (Alice)
- p: "works_on" (the relationship)
- o: ["project_001"] (the Project)

## Working with Files

### Save to Parquet

```python
import pathlib

# Export your graph to Parquet format
graph.asParquet(pathlib.Path("my_graph.parquet"))
```

### Load from Parquet

```python
# Create a new graph from Parquet file
db = duckdb.connect()
graph = PQG(db, source="my_graph.parquet")
graph.loadMetadata()

# Now you can query it
print(graph.objectCounts())
```

## CLI Tools

PQG includes command-line tools for exploring graphs:

```bash
# List all nodes in a graph
pqg entries my_graph.parquet

# View a specific node
pqg node my_graph.parquet person_001

# See the node with all its connections
pqg node my_graph.parquet person_001 --expand

# List object types
pqg types my_graph.parquet

# List relationship types
pqg predicates my_graph.parquet
```

## Next Steps

Now that you've created your first graph, explore these tutorials:

1. **[Basic Usage Tutorial](tutorials/01-basic-usage.md)** - Learn more about creating and managing nodes
2. **[Complex Objects Tutorial](tutorials/02-complex-objects.md)** - Work with nested data structures
3. **[Querying Tutorial](tutorials/03-querying.md)** - Master graph queries and traversals
4. **[Visualization Tutorial](tutorials/04-visualization.md)** - Create visual representations of your graphs

## Common Patterns

### In-Memory vs Persistent Databases

```python
# In-memory (temporary, fast)
db = duckdb.connect()

# File-based (persistent)
db = duckdb.connect("my_database.duckdb")
```

### Auto-Generated IDs

```python
# PQG generates IDs automatically
node = Node(label="No ID specified")
graph.addNode(node)
# node.pid will be something like "anon_a1b2c3d4"
```

### Multiple Relationships

```python
# One person works on multiple projects
edge = Edge(
    s="person_001",
    p="works_on",
    o=["project_001", "project_002", "project_003"]
)
```

## Getting Help

- **Documentation**: Check the [User Guide](user-guide.md) for comprehensive information
- **CLI Help**: Run `pqg --help` or `pqg COMMAND --help`
- **Examples**: Look in the `examples/` directory for more code samples
- **Issues**: Report bugs or request features on [GitHub](https://github.com/isamplesorg/pqg/issues)

## Tips for Success

1. **Always commit**: Don't forget to call `db.commit()` after adding nodes/edges
2. **Use meaningful PIDs**: Clear identifiers make debugging easier
3. **Add labels**: Labels make your graph much easier to understand
4. **Start simple**: Begin with basic nodes and edges before complex structures
5. **Explore the CLI**: The command-line tools are great for understanding your data

Happy graphing!
